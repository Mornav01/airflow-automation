from __future__ import annotations

import os
import json
import subprocess
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

import sys

local_tz = pendulum.timezone("Asia/Kolkata")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts")))

from db_config import (  # noqa: E402
    V2_MAPPING_SQL_FILE,
    V2_MASTER_PII_SQL_FILE,
    V2_MASTER_INSURANCE_SQL_FILE,
)

PYTHON_EXECUTABLE = os.environ.get("AIRFLOW_PYTHON", "python")

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

MAPPING_MASTER_V2_STATE_DIR = os.path.join(os.environ["STATE_DIR"], "mapping_master_v2")

MAPPING_STATE_FILE = os.path.join(MAPPING_MASTER_V2_STATE_DIR, "v2_mapping_update_state.json")
PII_STATE_FILE     = os.path.join(MAPPING_MASTER_V2_STATE_DIR, "v2_pii_data_state.json")
MASTER_STATE_FILE  = os.path.join(MAPPING_MASTER_V2_STATE_DIR, "v2_master_insurance_state.json")

# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="mapping_master_v2_runner",
    description="V2 Mapping + Master Multi-Mode Runner",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["mapping", "master", "v2"],
    params={
        "v2_mapping_sql_file":          V2_MAPPING_SQL_FILE,
        "v2_master_pii_sql_file":       V2_MASTER_PII_SQL_FILE,
        "v2_master_insurance_sql_file": V2_MASTER_INSURANCE_SQL_FILE,
    },
) as dag:

    # ──────────────────────────────────────────────
    # 1️⃣ Run All Jobs In One Task
    # ──────────────────────────────────────────────

    def run_all_jobs(**context):
        params = context["params"]

        # --- 1. V2 Mapping Update ---
        mapping_script = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "mssql_incremental_mapping_update_v2.py")
        )

        subprocess.run(
            [
                PYTHON_EXECUTABLE,
                mapping_script,
                "--sql_file",
                params["v2_mapping_sql_file"],
            ],
            check=True,
        )

        # --- 2. V2 Master PII Data ---
        transfer_script = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "mssql_incremental_master_recreate_v2.py")
        )

        subprocess.run(
            [
                PYTHON_EXECUTABLE,
                transfer_script,
                "--mode",
                "pii_data",
                "--sql_file",
                params["v2_master_pii_sql_file"],
            ],
            check=True,
        )

        # --- 3. V2 Master Insurance ---
        subprocess.run(
            [
                PYTHON_EXECUTABLE,
                transfer_script,
                "--mode",
                "master_insurance",
                "--sql_file",
                params["v2_master_insurance_sql_file"],
            ],
            check=True,
        )

    run_task = PythonOperator(
        task_id="run_all_jobs",
        python_callable=run_all_jobs,
    )

    # ──────────────────────────────────────────────
    # 2️⃣ Validate All State Files
    # ──────────────────────────────────────────────

    def validate_pipeline(**context):

        state_files = {
            "v2_mapping_update": MAPPING_STATE_FILE,
            "v2_pii_data":       PII_STATE_FILE,
            "v2_master_insurance": MASTER_STATE_FILE,
        }

        failed_components = []

        for name, path in state_files.items():

            if not os.path.exists(path):
                raise RuntimeError(f"State file not found for {name}: {path}")

            with open(path, "r") as f:
                state = json.load(f)

            print(f"\n===== STATE: {name} =====")
            print(json.dumps(state, indent=4))

            if not state.get("success", False):
                failed_components.append(name)

        if failed_components:
            raise RuntimeError(
                f"Pipeline failed. Failed components: {', '.join(failed_components)}"
            )

    validate_task = PythonOperator(
        task_id="validate_pipeline",
        python_callable=validate_pipeline,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    run_task >> validate_task
