"""
Airflow DAG: Daily incremental update of clinicalbin_decrypted_26.

Runs scripts/mind/decrypt_cnotes.py daily after the dump+restore pipeline.
Retries every 2 hours up to 9 times so a delayed restore (up to ~midnight)
won't cause a permanent failure.

Credentials and paths are read from Airflow/.env via the script's own load_dotenv.
Webhook alerts use GOOGLE_CHAT_WEBHOOK_URL from the environment.
"""

from __future__ import annotations
import json
import os
import shlex
from datetime import timedelta

import pendulum
import requests

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────
local_tz = pendulum.timezone("Asia/Kolkata")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/Users/ndai/Desktop/Air_DEID/airflow-automation/Airflow")
VENV_PYTHON  = os.environ.get("DEID_PYTHON", "/opt/miniconda3/envs/air_deid/bin/python")
SCRIPT_PATH  = os.path.join(AIRFLOW_HOME, "scripts", "mind", "decrypt_cnotes.py")
WEBHOOK_URL  = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL", "")
BATCH_SIZE   = int(os.environ.get("CNOTES_BATCH_SIZE", "10000"))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # Retry every 2 hours. DAG starts at 06:00 IST; 9 retries covers until ~midnight,
    # allowing the restore pipeline time to finish before this task succeeds.
    "retries": 9,
    "retry_delay": timedelta(hours=2),
}


# ──────────────────────────────────────────────────────────────
# Webhook notification
# ──────────────────────────────────────────────────────────────
def _send_webhook(context, status: str) -> None:
    if not WEBHOOK_URL:
        print("No GOOGLE_CHAT_WEBHOOK_URL configured, skipping notification.")
        return

    dag    = context.get("dag")
    ti     = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    msg    = str(context.get("exception")) if status == "FAILED" else "Completed successfully"

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
# DAG Definition
# ──────────────────────────────────────────────────────────────
with DAG(
    dag_id="clinicalbin_decrypt_daily",
    description="Daily incremental sync of clinicalbin_decrypted_26 from MSSQL to MySQL",
    schedule=None,#"0 6 * * *",
    start_date=pendulum.datetime(2026, 3, 10, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["mind", "clinicalbin", "decrypt"],
    params={
        "daily_cutoff": None,   # YYYY-MM-DD override; None = yesterday
        "batch_size": BATCH_SIZE,
    },
) as dag:

    _script = shlex.quote(SCRIPT_PATH)
    _cmd = (
        f"set -e\n"
        f"{VENV_PYTHON} {_script}"
        " {% if params.daily_cutoff %}--daily-cutoff {{ params.daily_cutoff }}{% endif %}"
        " --batch-size {{ params.batch_size }}\n"
    )

    run_decrypt = BashOperator(
        task_id="run_decrypt_cnotes",
        bash_command=_cmd,
        cwd=AIRFLOW_HOME,
        env={
            "AIRFLOW_DAG_ID":         "{{ dag.dag_id }}",
            "AIRFLOW_RUN_ID":         "{{ run_id }}",
            "AIRFLOW_EXECUTION_DATE": "{{ ds }}",
        },
        append_env=True,
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
    )
