from __future__ import annotations

import json
import os
import sys
import subprocess
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────

local_tz = pendulum.timezone("Asia/Kolkata")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts")))

from db_config import STATE_DIR  # noqa: E402  # type: ignore[import-untyped]

PYTHON_EXECUTABLE = os.environ.get("AIRFLOW_PYTHON", "python")

MAPPING_MASTER_STATE_DIR = os.path.join(STATE_DIR, "mapping_master")

MAPPING_STATE_FILE = os.path.join(MAPPING_MASTER_STATE_DIR, "mapping_update_state.json")
PII_STATE_FILE     = os.path.join(MAPPING_MASTER_STATE_DIR, "pii_data_state.json")
MASTER_STATE_FILE  = os.path.join(MAPPING_MASTER_STATE_DIR, "master_insurance_state.json")

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="deidentification_runner",
    description="Mapping Master → Deidentification Runner",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["deid", "mapping"],
    params={
        # Mapping Master params
        "mapping_sql_file":          "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/Airflow/scripts/sql/mapping_ecw.sql",
        "master_pii_sql_file":       "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/Airflow/scripts/sql/master_pii_data_table_ecw.sql",
        "master_insurance_sql_file": "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/Airflow/scripts/sql/master_insurance_query_ecw.sql",
        # Deidentification params
        "num_workers":          5,
        "dbdetailsmodel_name":  "arizona_mobiledoc_incremental",
        "tables_filepath":      "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/input/test.csv",
    },
) as dag:

    # ──────────────────────────────────────────────
    # Run Mapping Master Jobs
    # ──────────────────────────────────────────────

    def run_mapping_jobs(**context):
        params = context["params"]

        mapping_script = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "mssql_incremental_mapping_update.py")
        )
        master_script = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "mssql_incremental_master_recreate.py")
        )

        # 1a. Mapping Update
        subprocess.run([
            PYTHON_EXECUTABLE, mapping_script,
            "--sql_file", params["mapping_sql_file"],
        ])

        # 1b. Master PII Data
        subprocess.run([
            PYTHON_EXECUTABLE, master_script,
            "--mode",     "pii_data",
            "--sql_file", params["master_pii_sql_file"],
        ])

        # 1c. Master Insurance
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
    # Validate Mapping State Files
    # ──────────────────────────────────────────────

    def validate_mapping_master(**_):
        state_files = {
            "mapping_update":    MAPPING_STATE_FILE,
            "pii_data":          PII_STATE_FILE,
            "master_insurance":  MASTER_STATE_FILE,
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
                f"Mapping pipeline failed. Failed components: {', '.join(failed_components)}"
            )

    validate_mapping_task = PythonOperator(
        task_id="validate_mapping_master",
        python_callable=validate_mapping_master,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # ──────────────────────────────────────────────
    # Run Deidentification Task Creation
    # ──────────────────────────────────────────────

    def run_deid(**context):
        params = context["params"]

        run_script = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_deidentification_run_polar.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            run_script,
            "--dbdetailsmodel_name", params["dbdetailsmodel_name"],
            "--tables_filepath",     str(params["tables_filepath"]),
        ]

        result = subprocess.run(cmd)

        if result.returncode != 0:
            raise RuntimeError("Deidentification Task Creation failed")

    run_deid_task = PythonOperator(
        task_id="run_deid",
        python_callable=run_deid,
    )

    # ──────────────────────────────────────────────
    # Start Workers
    # ──────────────────────────────────────────────

    start_workers_script = os.path.abspath(
        os.path.join(SCRIPT_DIR, "..", "scripts", "start_workers_terminal.sh")
    )

    start_workers = BashOperator(
        task_id="start_workers",
        bash_command=f"bash {start_workers_script} {{{{ params.num_workers }}}}",
    )

    # ──────────────────────────────────────────────
    # Dependencies
    # ──────────────────────────────────────────────

    run_mapping_task >> validate_mapping_task >> run_deid_task >> start_workers
