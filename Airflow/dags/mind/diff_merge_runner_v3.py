from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from datetime import timedelta

import pendulum
import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────
local_tz = pendulum.timezone("Asia/Kolkata")

# Resolve Airflow/scripts/ regardless of where inside dags/ this file lives.
_AIRFLOW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_SCRIPTS_DIR = os.path.join(_AIRFLOW_DIR, "scripts")
sys.path.insert(0, _SCRIPTS_DIR)

from db_config import STATE_DIR, PK_CSV, DIFF_MERGE_TABLES_CSV  # noqa: E402

PYTHON_EXECUTABLE = os.environ.get("DEID_PYTHON", "/opt/miniconda3/envs/air_deid/bin/python")
WEBHOOK_URL = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL", "")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


# ──────────────────────────────────────────────────────────────
# Webhook notification
# ──────────────────────────────────────────────────────────────
def _send_webhook(context, status: str) -> None:
    if not WEBHOOK_URL:
        print("No GOOGLE_CHAT_WEBHOOK_URL configured, skipping notification.")
        return

    dag     = context.get("dag")
    ti      = context.get("task_instance")
    dag_run = context.get("dag_run")
    msg     = str(context.get("exception")) if status == "FAILED" else "Completed successfully"

    payload = {
        "text": (
            f"*Airflow DAG Alert*\n"
            f"• *Status:* {status}\n"
            f"• *DAG:* {dag.dag_id if dag else 'unknown'}\n"
            f"• *Task:* {ti.task_id if ti else 'unknown'}\n"
            f"• *Run ID:* {dag_run.run_id if dag_run else context.get('run_id', 'unknown')}\n"
            f"• *Log URL:* {getattr(ti, 'log_url', 'n/a')}\n"
            f"• *Message:* {msg}"
        )
    }

    try:
        response = requests.post(
            WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to send webhook alert: {e}")


def on_success_callback(context):
    _send_webhook(context, "SUCCESS")


def on_failure_callback(context):
    _send_webhook(context, "FAILED")


# ──────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────
with DAG(
    dag_id="diff_merge_runner_v3",
    description="Diff + Merge pipeline using Python diff script (not stored procedure)",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    tags=["mind", "diff_merge"],
    params={
        "tables_csv": DIFF_MERGE_TABLES_CSV,
        "pk_csv":     PK_CSV,
        "state_dir":  STATE_DIR,
    },
) as dag:

    # ── Task 1: Diff + Merge ───────────────────────────────────
    def run_diff_merge(**context):
        params = context["params"]
        script_path = os.path.join(_SCRIPTS_DIR, "state_diff_merge_v3.py")

        cmd = [
            PYTHON_EXECUTABLE, script_path,
            "--mode",       "run",
            "--tables_csv", params["tables_csv"],
            "--pk_csv",     params["pk_csv"],
            "--state_dir",  params["state_dir"],
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

        if result.stdout:
            logging.info(result.stdout)
        if result.stderr:
            logging.info(result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"Diff + Merge failed (rc={result.returncode}):\n"
                f"{result.stderr or result.stdout}"
            )

    run_task = PythonOperator(
        task_id="run_diff_merge",
        python_callable=run_diff_merge,
    )

    # ── Task 2: Decryption (always runs after diff+merge) ──────
    def run_decryption(**context):
        params = context["params"]
        script_path = os.path.join(_SCRIPTS_DIR, "state_diff_decryption.py")

        cmd = [
            PYTHON_EXECUTABLE, script_path,
            "--tables_filepath", params["tables_csv"],
            "--pk_csv",          params["pk_csv"],
            "--state_dir",       params["state_dir"],
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

        if result.stdout:
            logging.info(result.stdout)
        if result.stderr:
            logging.info(result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"Decryption failed (rc={result.returncode}):\n"
                f"{result.stderr or result.stdout}"
            )

    decryption_task = PythonOperator(
        task_id="run_decryption",
        python_callable=run_decryption,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Task 3: Summary (always runs) ──────────────────────────
    def run_summary(**context):
        params = context["params"]
        script_path = os.path.join(_SCRIPTS_DIR, "state_diff_merge_v3.py")

        cmd = [
            PYTHON_EXECUTABLE, script_path,
            "--mode",       "summary",
            "--tables_csv", params["tables_csv"],
            "--state_dir",  params["state_dir"],
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Summary script failed (rc={result.returncode}):\n"
                f"{result.stderr or result.stdout}"
            )

        return json.loads(result.stdout.strip())

    summary_task = PythonOperator(
        task_id="generate_summary",
        python_callable=run_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Task 4: Validate ───────────────────────────────────────
    def validate_pipeline(**context):
        summary = context["ti"].xcom_pull(task_ids="generate_summary")

        diff_failed       = summary.get("diff_failed_today", 0)
        merge_failed      = summary.get("merge_failed_today", 0)
        decryption_failed = summary.get("decryption_failed_today", 0)

        logging.info("Pipeline summary:\n%s", json.dumps(summary, indent=4))

        if diff_failed > 0 or merge_failed > 0 or decryption_failed > 0:
            raise RuntimeError(
                f"Pipeline completed with failures — "
                f"diff: {diff_failed}, merge: {merge_failed}, decryption: {decryption_failed}"
            )

        logging.info("Pipeline successful.")

    validate_task = PythonOperator(
        task_id="validate_pipeline",
        python_callable=validate_pipeline,
    )

    # ── Dependencies ───────────────────────────────────────────
    run_task >> decryption_task >> summary_task >> validate_task
