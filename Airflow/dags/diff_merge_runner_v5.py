from __future__ import annotations

import os
import json
import subprocess
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────

import sys

local_tz = pendulum.timezone("Asia/Kolkata")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts")))

from db_config import STATE_DIR, DIFF_MERGE_TABLES_CSV, PK_CSV, DIFF_MERGE_SCHEDULE  # noqa: E402

PYTHON_EXECUTABLE = os.environ.get("AIRFLOW_PYTHON", "python")

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="diff_merge_runner_v5",
    description="MSSQL → MySQL incremental sync + merge using state_diff_merge_v5",
    schedule=DIFF_MERGE_SCHEDULE,
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["sync", "mssql", "mysql", "merge"],
    params={
        "tables_csv": DIFF_MERGE_TABLES_CSV,
        "pk_csv":     PK_CSV,
        "state_dir":  STATE_DIR,
        "run_date":   "",   # YYYY-MM-DD — leave empty to use today's date
    },
) as dag:

    # ──────────────────────────────────────────────
    # 1️⃣ Run Sync  (extract MSSQL → MySQL incremental + add nd_auto_increment_id)
    # ──────────────────────────────────────────────

    def run_sync(**context):
        import logging
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_diff_merge_v5.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            "-u",
            script_path,
            "--mode",       "run",
            "--tables_csv", params["tables_csv"],
            "--pk_csv",     params["pk_csv"],
            "--state_dir",  params["state_dir"],
        ]

        if params.get("run_date"):
            cmd += ["--run_date", params["run_date"]]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

        for line in proc.stdout:
            logging.info(line.rstrip())

        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(f"Sync execution failed (exit {rc})")

    run_task = PythonOperator(
        task_id="run_sync",
        python_callable=run_sync,
    )

    # ──────────────────────────────────────────────
    # 2️⃣ Run Merge  (incremental → main + diffmerged; always runs after sync)
    # ──────────────────────────────────────────────

    def run_merge(**context):
        import logging
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_diff_merge_v5.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            "-u",
            script_path,
            "--mode",       "merge",
            "--tables_csv", params["tables_csv"],
            "--pk_csv",     params["pk_csv"],
            "--state_dir",  params["state_dir"],
        ]

        if params.get("run_date"):
            cmd += ["--run_date", params["run_date"]]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

        for line in proc.stdout:
            logging.info(line.rstrip())

        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(f"Merge execution failed (exit {rc})")

    merge_task = PythonOperator(
        task_id="run_merge",
        python_callable=run_merge,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ──────────────────────────────────────────────
    # 3️⃣ Generate Summary  (always runs; output parsed by validate step)
    # ──────────────────────────────────────────────

    def run_summary(**context):
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_diff_merge_v5.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            script_path,
            "--mode",       "summary",
            "--tables_csv", params["tables_csv"],
            "--state_dir",  params["state_dir"],
        ]

        if params.get("run_date"):
            cmd += ["--run_date", params["run_date"]]

        result = subprocess.run(
            cmd, capture_output=True, text=True,
        )

        if result.returncode != 0:
            raise RuntimeError("Summary script failed")

        summary = json.loads(result.stdout.strip())
        return summary

    summary_task = PythonOperator(
        task_id="generate_summary",
        python_callable=run_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ──────────────────────────────────────────────
    # 4️⃣ Final Validation Task
    # ──────────────────────────────────────────────

    def validate_pipeline(**context):
        summary = context["ti"].xcom_pull(task_ids="generate_summary")

        sync_failed  = summary.get("sync_failed_today",  0)
        merge_failed = summary.get("merge_failed_today", 0)

        if sync_failed > 0 or merge_failed > 0:
            raise RuntimeError(
                f"Pipeline failed: {sync_failed} sync failure(s), "
                f"{merge_failed} merge failure(s)."
            )

        print("Pipeline successful.")
        print(json.dumps(summary, indent=4))

    validate_task = PythonOperator(
        task_id="validate_pipeline",
        python_callable=validate_pipeline,
    )

    # ──────────────────────────────────────────────
    # Dependencies
    # ──────────────────────────────────────────────

    run_task >> merge_task >> summary_task >> validate_task
