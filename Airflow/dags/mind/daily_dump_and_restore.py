from __future__ import annotations
import os
import json
import requests
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────
local_tz = pendulum.timezone("Asia/Kolkata")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/Users/ndai/Desktop/Air_DEID/airflow-automation/Airflow")
VENV_PYTHON = "/opt/miniconda3/envs/air_deid/bin/python"
# Unified, config-driven dump script (behaviour set via DUMP_* in .env).
SCRIPT_DAILY_DUMP = os.path.join(AIRFLOW_HOME, "scripts", "daily_dump.py")
# Unified, config-driven restore script (DB list via RESTORE_DB_CONFIGS in .env).
SCRIPT_RESTORE = os.path.join(AIRFLOW_HOME, "scripts", "restore_incremental_logs.py")

# Set GOOGLE_CHAT_WEBHOOK_URL in Airflow/.env or worker environment
WEBHOOK_URL = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL", "")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# ──────────────────────────────────────────────────────────────
# Webhook Notification
# ──────────────────────────────────────────────────────────────
def _send_webhook(context, status: str) -> None:
    if not WEBHOOK_URL:
        print("No GOOGLE_CHAT_WEBHOOK_URL configured, skipping notification.")
        return

    dag = context.get("dag")
    ti = context.get("task_instance")
    msg = str(context.get("exception")) if status == "FAILED" else "Completed successfully"

    payload = {
        "text": (
            f"*Airflow DAG Alert*\n"
            f"• *Status:* {status}\n"
            f"• *DAG:* {dag.dag_id if dag else 'unknown'}\n"
            f"• *Task:* {ti.task_id if ti else 'unknown'}\n"
            f"• *Run ID:* {context.get('run_id', 'unknown')}\n"
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
# DAG Definition
# ──────────────────────────────────────────────────────────────
with DAG(
    dag_id="daily_dump_and_restore_pipeline",
    description="Daily SMB dump followed by incremental log restore",
    schedule=None,#"0 10 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    tags=["mind", "dump", "restore"],
) as dag:

    run_daily_dump = BashOperator(
        task_id="run_daily_dump",
        bash_command=f"set -e\n{VENV_PYTHON} {SCRIPT_DAILY_DUMP}\n",
    )

    run_restore_logs = BashOperator(
        task_id="run_restore_logs",
        bash_command=f"set -e\n{VENV_PYTHON} {SCRIPT_RESTORE}\n",
    )

    run_daily_dump >> run_restore_logs
