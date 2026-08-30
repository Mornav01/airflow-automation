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

from db_config import DEID_POST_PROCESS_TABLES_CSV  # noqa: E402

DEID_PYTHON = os.environ.get("DEID_PYTHON", "/opt/miniconda3/envs/air_deid/bin/python")
WEBHOOK_URL = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL", "")

_CONFIGS_DIR    = os.path.join(_SCRIPTS_DIR, "configs")
_DEFAULT_CONFIG = os.path.join(_CONFIGS_DIR, "config_incremental.yaml")
_DEFAULT_TABLES = DEID_POST_PROCESS_TABLES_CSV

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
    dag_id="deid_post_process_runner_v2",
    description="Post-deidentification v2: status check → merge → delete",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    tags=["mind", "deid", "v2"],
    params={
        "config_yaml":     _DEFAULT_CONFIG,
        "tables_filepath": _DEFAULT_TABLES,
        "deid_status_max_workers": 8,
    },
) as dag:

    # ── Task 1: Check Deidentification Status ─────────────────
    def run_status_check(**context):
        params = context["params"]

        script_path = os.path.join(_SCRIPTS_DIR, "state_deid_status_check_v2.py")

        # v3 status-check uses mssql+pymssql; run under the Airflow interpreter
        # (air_env has pymssql), not DEID_PYTHON (new_deid = deid-only).
        cmd = [
            sys.executable,
            script_path,
            "--config-yaml",     params["config_yaml"],
            "--tables-filepath", params["tables_filepath"],
            "--max-workers",     str(params.get("deid_status_max_workers", 8)),
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
                f"Deidentification status check failed (rc={result.returncode}):\n"
                f"{result.stderr or result.stdout}"
            )

    status_check_task = PythonOperator(
        task_id="check_deid_status_v2",
        python_callable=run_status_check,
    )

    # ── Task 2: Merge Deidentified Data ───────────────────────
    def run_deid_merge(**context):
        params = context["params"]

        script_path = os.path.join(_SCRIPTS_DIR, "state_deid_merge.py")

        cmd = [
            DEID_PYTHON,
            script_path,
            "--tables_filepath", params["tables_filepath"],
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
                f"Deidentified merge failed (rc={result.returncode}):\n"
                f"{result.stderr or result.stdout}"
            )

    merge_task = PythonOperator(
        task_id="merge_deid_data",
        python_callable=run_deid_merge,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Task 3: Delete Old Original DIFFMERGED Data ───────────
    def run_deid_delete(**context):
        params = context["params"]

        script_path = os.path.join(_SCRIPTS_DIR, "state_deid_delete.py")

        cmd = [
            DEID_PYTHON,
            script_path,
            "--tables_filepath", params["tables_filepath"],
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
                f"Original DIFFMERGED delete failed (rc={result.returncode}):\n"
                f"{result.stderr or result.stdout}"
            )

    delete_task = PythonOperator(
        task_id="delete_original_diffmerged",
        python_callable=run_deid_delete,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Dependencies ───────────────────────────────────────────
    status_check_task >> merge_task >> delete_task
