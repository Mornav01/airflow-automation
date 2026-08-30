from __future__ import annotations

import json
import os
from datetime import timedelta

import pendulum
import requests

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────
local_tz = pendulum.timezone("Asia/Kolkata")

DEID_PYTHON   = os.environ.get("DEID_PYTHON", "/opt/miniconda3/envs/air_deid/bin/python")
WEBHOOK_URL   = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL", "")
PROJECT_DIR   = os.environ.get("DASHBOARD_PROJECT_DIR", "/Users/ndai/Documents/udm/de_udm/dashboards")

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
    dag_id="provider_dashboard",
    description="Run provider dashboard table update script",
    start_date=pendulum.datetime(2026, 4, 9, 12, 0, tz=local_tz),
    schedule=None,#"0 12 * * *",  # every day at 12:00 IST
    catchup=False,
    default_args=default_args,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    tags=["mind", "dashboard", "mssql", "reporting"],
) as dag:

    run_dashboard_sql = BashOperator(
        task_id="run_dashboard_table_mssql",
        bash_command=f"{DEID_PYTHON} dashboard_table_mssql.py",
        cwd=PROJECT_DIR,
    )
