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

from db_config import (  # noqa: E402
    STATE_DIR,
    MAPPING_SQL_FILE, MASTER_PII_SQL_FILE, MASTER_INSURANCE_SQL_FILE,
    DEID_TABLES_CSV,
)

DEID_PYTHON  = os.environ.get("DEID_PYTHON", "/opt/miniconda3/envs/air_deid/bin/python")
WEBHOOK_URL  = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL", "")

MAPPING_MASTER_STATE_DIR = os.path.join(STATE_DIR, "mapping_master")
MAPPING_STATE_FILE = os.path.join(MAPPING_MASTER_STATE_DIR, "mapping_update_state.json")
PII_STATE_FILE     = os.path.join(MAPPING_MASTER_STATE_DIR, "pii_data_state.json")
MASTER_STATE_FILE  = os.path.join(MAPPING_MASTER_STATE_DIR, "master_insurance_state.json")

_CONFIGS_DIR   = os.path.join(_SCRIPTS_DIR, "configs")
_DEFAULT_CONFIG = os.path.join(_CONFIGS_DIR, "config_incremental.yaml")

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
    dag_id="deidentification_runner_v2",
    description="Mapping Master → Deidentification Runner (deid v2)",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    tags=["mind", "deid", "mapping", "v2"],
    params={
        "mapping_sql_file":          MAPPING_SQL_FILE,
        "master_pii_sql_file":       MASTER_PII_SQL_FILE,
        "master_insurance_sql_file": MASTER_INSURANCE_SQL_FILE,
        "config_yaml":               _DEFAULT_CONFIG,
        "tables_filepath":           DEID_TABLES_CSV,
    },
) as dag:

    # ── Task 1: Run Mapping Master Jobs ───────────────────────
    def run_mapping_jobs(**context):
        params = context["params"]

        mapping_script = os.path.join(_SCRIPTS_DIR, "mssql_incremental_mapping_update_activeflag.py")
        master_script  = os.path.join(_SCRIPTS_DIR, "mssql_incremental_master_recreate.py")

        _env = {**os.environ, "PYTHONUNBUFFERED": "1"}

        steps = [
            (
                "mapping_update",
                [DEID_PYTHON, mapping_script, "--sql_file", params["mapping_sql_file"]],
            ),
            (
                "pii_data",
                [DEID_PYTHON, master_script, "--mode", "pii_data", "--sql_file", params["master_pii_sql_file"]],
            ),
            (
                "master_insurance",
                [DEID_PYTHON, master_script, "--mode", "master_insurance", "--sql_file", params["master_insurance_sql_file"]],
            ),
        ]

        failed = []
        for name, cmd in steps:
            result = subprocess.run(cmd, capture_output=True, text=True, env=_env)
            if result.stdout:
                logging.info("[%s] %s", name, result.stdout)
            if result.stderr:
                logging.info("[%s] stderr: %s", name, result.stderr)
            if result.returncode != 0:
                logging.error("[%s] failed with rc=%d", name, result.returncode)
                failed.append(name)
            else:
                logging.info("[%s] completed successfully", name)

        if failed:
            raise RuntimeError(f"Mapping jobs failed: {failed}")

    run_mapping_task = PythonOperator(
        task_id="run_mapping_jobs",
        python_callable=run_mapping_jobs,
    )

    # ── Task 2: Validate Mapping State Files ──────────────────
    def validate_mapping_master(**_):
        state_files = {
            "mapping_update":   MAPPING_STATE_FILE,
            "pii_data":         PII_STATE_FILE,
            "master_insurance": MASTER_STATE_FILE,
        }

        failed_components = []

        for name, path in state_files.items():
            if not os.path.exists(path):
                raise RuntimeError(f"State file not found for {name}: {path}")

            with open(path) as f:
                state = json.load(f)

            logging.info("State [%s]:\n%s", name, json.dumps(state, indent=4))

            if not state.get("success", False):
                failed_components.append(name)

        if failed_components:
            raise RuntimeError(
                f"Mapping pipeline failed. Failed components: {', '.join(failed_components)}"
            )

    validate_mapping_task = PythonOperator(
        task_id="validate_mapping_master",
        python_callable=validate_mapping_master,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Task 3: Init deid state files (before deid run) ───────
    def init_deid_state(**context):
        params = context["params"]
        script_path = os.path.join(_SCRIPTS_DIR, "state_deid_init_v2.py")

        result = subprocess.run(
            [DEID_PYTHON, script_path, "--tables-filepath", params["tables_filepath"]],
            capture_output=True, text=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

        if result.stdout:
            logging.info(result.stdout)
        if result.stderr:
            logging.info(result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"Deid state init failed (rc={result.returncode}):\n"
                f"{result.stderr or result.stdout}"
            )

    init_state_task = PythonOperator(
        task_id="init_deid_state",
        python_callable=init_deid_state,
    )

    # ── Task 4: Run deid v2 ────────────────────────────────────
    def run_deid_v2(**context):
        params = context["params"]

        script = os.path.join(_SCRIPTS_DIR, "manual_deidentification_run_v2.py")
        cmd = [
            DEID_PYTHON, script,
            "--config", params["config_yaml"],
            "--tables", params["tables_filepath"],
        ]

        # No capture_output — deid run is long-lived and spawns Celery workers;
        # streaming output directly to Airflow logs is preferable to buffering.
        result = subprocess.run(cmd, env={**os.environ, "PYTHONUNBUFFERED": "1"})

        if result.returncode != 0:
            raise RuntimeError(f"deid run failed with exit code {result.returncode}")

    run_deid_task = PythonOperator(
        task_id="run_deid_v2",
        python_callable=run_deid_v2,
        execution_timeout=None,  # deid run can take hours — no timeout
    )

    # ── Dependencies ───────────────────────────────────────────
    run_mapping_task >> validate_mapping_task >> init_state_task >> run_deid_task
