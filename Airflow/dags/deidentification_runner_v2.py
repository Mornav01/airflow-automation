from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────

local_tz = pendulum.timezone("Asia/Kolkata")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts")))

from db_config import (  # noqa: E402  # type: ignore[import-untyped]
    STATE_DIR,
    MAPPING_SQL_FILE, MASTER_PII_SQL_FILE, MASTER_INSURANCE_SQL_FILE,
    DEID_TABLES_CSV,
)

PYTHON_EXECUTABLE = os.environ.get("AIRFLOW_PYTHON", "python")
# deid v2 requires the new_deid conda env (has polars, celery, deid package).
# Falls back to the same Python Airflow is using if DEID_PYTHON is not set.
DEID_PYTHON = os.environ.get("DEID_PYTHON", PYTHON_EXECUTABLE)

MAPPING_MASTER_STATE_DIR = os.path.join(STATE_DIR, "mapping_master")
MAPPING_STATE_FILE = os.path.join(MAPPING_MASTER_STATE_DIR, "mapping_update_state.json")
PII_STATE_FILE     = os.path.join(MAPPING_MASTER_STATE_DIR, "pii_data_state.json")
MASTER_STATE_FILE  = os.path.join(MAPPING_MASTER_STATE_DIR, "master_insurance_state.json")

# Default config paths — stored inside scripts/configs/
_CONFIGS_DIR      = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts", "configs"))
_DEFAULT_CONFIG   = os.path.join(_CONFIGS_DIR, "mind_config_incremental.yaml")

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="deidentification_runner_v2",
    description="Mapping Master → Deidentification Runner (deid v2)",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["deid", "mapping", "v2"],
    params={
        # Mapping Master params
        "mapping_sql_file":          MAPPING_SQL_FILE,
        "master_pii_sql_file":       MASTER_PII_SQL_FILE,
        "master_insurance_sql_file": MASTER_INSURANCE_SQL_FILE,
        # Deidentification v2 params
        "config_yaml":     _DEFAULT_CONFIG,
        "tables_filepath": DEID_TABLES_CSV,
    },
) as dag:

    # ──────────────────────────────────────────────
    # 1. Run Mapping Master Jobs  (unchanged)
    # ──────────────────────────────────────────────

    def run_mapping_jobs(**context):
        params = context["params"]

        mapping_script = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "mssql_incremental_mapping_update_activeflag.py")
        )
        master_script = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "mssql_incremental_master_recreate.py")
        )

        subprocess.run([
            PYTHON_EXECUTABLE, mapping_script,
            "--sql_file", params["mapping_sql_file"],
        ])

        subprocess.run([
            PYTHON_EXECUTABLE, master_script,
            "--mode",     "pii_data",
            "--sql_file", params["master_pii_sql_file"],
        ])

        subprocess.run([
            PYTHON_EXECUTABLE, master_script,
            "--mode",     "master_insurance",
            "--sql_file", params["master_insurance_sql_file"],
        ])

    run_mapping_task = PythonOperator(
        task_id="run_mapping_jobs",
        python_callable=run_mapping_jobs,
    )

    # ──────────────────────────────────────────────
    # 2. Validate Mapping State Files  (unchanged)
    # ──────────────────────────────────────────────

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

            print(f"\n===== STATE: {name} =====")
            print(json.dumps(state, indent=4))

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

    # ──────────────────────────────────────────────
    # 3. Init JSON state files (before deid run)
    #
    #    Resets deidentification_status="started", is_merged=False,
    #    is_deleted=False for each table so merge/delete steps
    #    always see a fresh state for the current run.
    # ──────────────────────────────────────────────

    def init_deid_state(**context):
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_deid_init_v2.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            script_path,
            "--tables-filepath", params["tables_filepath"],
        ]

        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise RuntimeError("Deidentification state init failed")

    init_state_task = PythonOperator(
        task_id="init_deid_state",
        python_callable=init_deid_state,
    )

    # ──────────────────────────────────────────────
    # 4. Run deid v2  (replaces run_deid + start_workers)
    #
    #    deid run spawns its own Celery workers, orchestrates
    #    all batches, and exits only when everything is done.
    #    Exit code 0 = all tables completed, non-zero = failure.
    # ──────────────────────────────────────────────

    def run_deid_v2(**context):
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "manual_deidentification_run_v2.py")
        )
        cmd = [
            DEID_PYTHON, script_path,
            "--config", params["config_yaml"],
            "--tables", params["tables_filepath"],
        ]

        result = subprocess.run(cmd)

        if result.returncode != 0:
            raise RuntimeError(
                f"deid run failed with exit code {result.returncode}"
            )

    run_deid_task = PythonOperator(
        task_id="run_deid_v2",
        python_callable=run_deid_v2,
        execution_timeout=None,  # deid run can take hours — no timeout
    )

    # ──────────────────────────────────────────────
    # Dependencies
    # ──────────────────────────────────────────────

    run_mapping_task >> validate_mapping_task >> init_state_task >> run_deid_task
