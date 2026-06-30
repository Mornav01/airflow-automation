from __future__ import annotations
import json
import logging
import os
from datetime import timedelta

import pendulum
import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 🧭 Configuration
# ──────────────────────────────────────────────
local_tz = pendulum.timezone("Asia/Kolkata")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/Users/ndaidcnd/Desktop/Air_DEID/airflow-automation/Airflow/")
VENV_PYTHON = "/opt/miniconda3/envs/air_deid/bin/python"
SCRIPT = os.path.join(AIRFLOW_HOME, "scripts", "dent_binlog_check.py")

# Webhook URL for Google Chat notifications
# You can set this in Airflow Variables: Variable.set("gchat_webhook_url", "your_url")
# Accessing Variable at module level can cause issues if DB isn't initialized, so we'll get it lazily
WEBHOOK_URL = None

def get_webhook_url():
    """Get webhook URL lazily to avoid issues during DAG import."""
    global WEBHOOK_URL
    if WEBHOOK_URL is None:
        try:
            WEBHOOK_URL = Variable.get("gchat_webhook_url", default_var=None)
        except Exception as e:
            logger.warning("Could not fetch gchat_webhook_url from Airflow Variables: %s", e)
            WEBHOOK_URL = None
    return WEBHOOK_URL

# ──────────────────────────────────────────────
# 🧠 Webhook Notification Logic
# ──────────────────────────────────────────────
def _send_webhook(context, status: str, task_name: str = None):
    """Send webhook alert for task success or failure."""
    dag = context.get("dag")
    ti = context.get("task_instance")
    dag_id = dag.dag_id if dag else "unknown"
    task_id = task_name or (ti.task_id if ti else "unknown")
    run_id = context.get("run_id", "unknown")
    log_url = getattr(ti, "log_url", "<no log url>") if ti else "<no log url>"
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
        webhook_url = get_webhook_url()
        if not webhook_url:
            logger.warning("No WEBHOOK_URL configured. Skipping webhook alert.")
            return

        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        logger.info("Webhook alert sent (%s)", status)
    except Exception as e:
        logger.error("Failed to send webhook alert: %s", e)

def on_success_callback(context):
    _send_webhook(context, "SUCCESS")

def on_failure_callback(context):
    _send_webhook(context, "FAILED")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_success_callback": on_success_callback,
    "on_failure_callback": on_failure_callback,
}

# ──────────────────────────────────────────────
# 🗓️ DAG Definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="run_daily_binlog_check",
    # schedule="30 4 * * *",  # 4:30 AM UTC --> 10:00 AM IST daily
    schedule="00 00 * * *",
    # start_date=pendulum.datetime(2026, 2, 15, tz=local_tz),
    start_date=pendulum.datetime(2026, 2, 15),
    catchup=False,
    default_args=default_args,
    tags=["dent", "daily", "binlog", "webhook"],
) as dag:

    run_script = BashOperator(
        task_id="run_daily_binlog_check_script",
        bash_command=f"{VENV_PYTHON} {SCRIPT}",
        env={"PATH": os.environ.get("PATH", "")},
    )

    run_script
