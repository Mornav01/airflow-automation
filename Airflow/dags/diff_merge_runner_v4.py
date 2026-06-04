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

from db_config import STATE_DIR, PK_CSV, DIFF_MERGE_TABLES_CSV  # noqa: E402

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
    dag_id="diff_merge_runner_v4",
    description="Diff + Merge runner v4 — supports run_date param to retry failed tables for a specific date",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["diff_merge"],
    params={
        "tables_csv": DIFF_MERGE_TABLES_CSV,
        "pk_csv":     PK_CSV,
        "state_dir":  STATE_DIR,
        "run_date":   "",   # YYYY-MM-DD — leave empty to use today's date
    },
) as dag:

    # ──────────────────────────────────────────────
    # 1️⃣ Run Diff + Merge
    # ──────────────────────────────────────────────

    def run_diff_merge(**context):
        import logging
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_diff_merge_v4.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            "-u",                                  # unbuffered stdout/stderr in child
            script_path,
            "--mode", "run",
            "--tables_csv", params["tables_csv"],
            "--pk_csv", params["pk_csv"],
            "--state_dir", params["state_dir"],
        ]

        if params.get("run_date"):
            cmd += ["--run_date", params["run_date"]]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,              # merge stderr into stdout stream
            text=True,
            bufsize=1,                             # line-buffered on the parent side
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

        for line in proc.stdout:                   # stream live to Airflow logs
            logging.info(line.rstrip())

        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(f"Diff + Merge execution failed (exit {rc})")

    run_task = PythonOperator(
        task_id="run_diff_merge",
        python_callable=run_diff_merge,
    )

    # ──────────────────────────────────────────────
    # 2️⃣ Run Decryption (Always Runs after diff+merge)
    # ──────────────────────────────────────────────

    def run_decryption(**context):
        import logging
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_diff_decryption.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            "-u",
            script_path,
            "--tables_filepath", params["tables_csv"],
            "--pk_csv",          params["pk_csv"],
            "--state_dir",       params["state_dir"],
        ]

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
            raise RuntimeError(f"Decryption step failed (exit {rc})")

    decryption_task = PythonOperator(
        task_id="run_decryption",
        python_callable=run_decryption,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ──────────────────────────────────────────────
    # 3️⃣ Generate Summary (Always Runs)
    # ──────────────────────────────────────────────

    def run_summary(**context):
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_diff_merge_v4.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            script_path,
            "--mode", "summary",
            "--tables_csv", params["tables_csv"],
            "--state_dir", params["state_dir"],
        ]

        if params.get("run_date"):
            cmd += ["--run_date", params["run_date"]]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError("Summary script failed")

        # Parse JSON output
        summary = json.loads(result.stdout.strip())

        # Push to XCom
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
        summary = context["ti"].xcom_pull(
            task_ids="generate_summary"
        )

        diff_failed       = summary.get("diff_failed_today", 0)
        merge_failed      = summary.get("merge_failed_today", 0)
        decryption_failed = summary.get("decryption_failed_today", 0)

        if diff_failed > 0 or merge_failed > 0 or decryption_failed > 0:
            raise RuntimeError(
                f"Pipeline failed: {diff_failed} diff failures, "
                f"{merge_failed} merge failures, "
                f"{decryption_failed} decryption failures."
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

    run_task >> decryption_task >> summary_task >> validate_task
