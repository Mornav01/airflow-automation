from __future__ import annotations
import os
import json
import requests
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

# ──────────────────────────────────────────────
# 🧭 Configuration
# ──────────────────────────────────────────────
local_tz = pendulum.timezone("Asia/Kolkata")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/Airflow")
VENV_PYTHON = "/opt/miniconda3/envs/airenv/bin/python"
# Unified, config-driven dump script (behaviour set via DUMP_* in .env).
SCRIPT_DAILY_DUMP = os.path.join(AIRFLOW_HOME, "scripts", "daily_dump.py")
SCRIPT_RESTORE = os.path.join(AIRFLOW_HOME, "scripts", "restore_backups.py")


# Webhook URL (you can set this in your environment for security)
WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/AAQAQEnoR7w/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=M_3uc6cM-rMxQGN8bDHXhqbpje080EL17QQOit-DMhk"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,  # disabled email alerts
    "email_on_retry": False,
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# ──────────────────────────────────────────────
# 🧠 Webhook Notification Logic
# ──────────────────────────────────────────────
def _send_webhook(context, status: str):
    """Send webhook alert for DAG success or failure."""
    dag = context.get("dag")
    ti = context.get("task_instance")
    dag_id = dag.dag_id if dag else "unknown"
    task_id = ti.task_id if ti else "unknown"
    run_id = context.get("run_id", "unknown")
    log_url = getattr(ti, "log_url", "<no log url>")
    msg = str(context.get("exception")) if status == "FAILED" else "Completed successfully ✅"

    payload = {
        "text": (
            f"*Airflow DAG Alert* 🚨\n"
            f"• *Status:* {status}\n"
            f"• *DAG:* {dag_id}\n"
            f"• *Task:* {task_id}\n"
            f"• *Run ID:* {run_id}\n"
            f"• *Log URL:* {log_url}\n"
            f"• *Message:* {msg}"
        )
    }

    try:
        if not WEBHOOK_URL:
            print("⚠️ No WEBHOOK_URL configured. Skipping webhook alert.")
            return

        response = requests.post(
            WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        print(f"✅ Webhook alert sent ({status})")
    except Exception as e:
        print(f"❌ Failed to send webhook alert: {e}")

def on_success_callback(context):
    _send_webhook(context, "SUCCESS")

def on_failure_callback(context):
    _send_webhook(context, "FAILED")

# ──────────────────────────────────────────────
# 🗓️ DAG Definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="daily_dump_and_restore_pipeline",
    description="Run dump first, then restore logs",
    schedule="0 10 * * 6",  # Runs every Saturday at 10 AM    
    catchup=False,
    default_args=default_args,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    tags=["dump", "restore", "pipeline"],
) as dag:

    # ✅ Task 1 — Run Daily Dump
    run_daily_dump = BashOperator(
        task_id="run_daily_dump_script",
        bash_command=f"""
        set -e
        echo "Running Daily Dump..."
        {VENV_PYTHON} {SCRIPT_DAILY_DUMP}
        """,
        env={"PATH": os.environ.get("PATH", "")},
    )

    # ✅ Task 2 — Restore Incremental Logs
    run_restore_logs = BashOperator(
        task_id="run_restore_logs_docker",
        bash_command=f"""
        set -e
        echo "Running Restore Logs..."
        {VENV_PYTHON} {SCRIPT_RESTORE}
        """,
        env={"PATH": os.environ.get("PATH", "")},
    )

    # ✅ Dependency
    run_daily_dump >> run_restore_logs