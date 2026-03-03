from __future__ import annotations

import os
import subprocess
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
PYTHON_EXECUTABLE = os.environ.get("AIRFLOW_PYTHON", "python")

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="deid_post_process_runner",
    description="Post-deidentification: status check → merge → delete",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["deid"],
    params={
        "dbdetailsmodel_name": "arizona_mobiledoc_base",
        "tables_filepath": "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/input/arizona_priority_tables.csv",
        "pk_csv": "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/input/arizona_primary_keys.csv",
        "state_dir": "/Users/ndaicnsarizona/Desktop/production_state_incremental/state",
    },
) as dag:

    # ──────────────────────────────────────────────
    # 1️⃣ Check Deidentification Status
    # ──────────────────────────────────────────────

    def run_status_check(**context):
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_deid_status_check.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            script_path,
            "--dbdetailsmodel_name", params["dbdetailsmodel_name"],
            "--tables_filepath",     params["tables_filepath"],
            "--state_dir",           params["state_dir"],
        ]

        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise RuntimeError("Deidentification status check failed")

    status_check_task = PythonOperator(
        task_id="check_deid_status",
        python_callable=run_status_check,
    )

    # ──────────────────────────────────────────────
    # 2️⃣ Merge Deidentified Data (runs even if status check fails
    #    so partial successes still get merged)
    # ──────────────────────────────────────────────

    def run_deid_merge(**context):
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_deid_merge.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            script_path,
            "--tables_filepath", params["tables_filepath"],
            "--pk_csv",          params["pk_csv"],
            "--state_dir",       params["state_dir"],
        ]

        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise RuntimeError("Deidentified merge failed")

    merge_task = PythonOperator(
        task_id="merge_deid_data",
        python_callable=run_deid_merge,
        trigger_rule=TriggerRule.ALL_DONE,  # run even if status_check partially failed
    )

    # ──────────────────────────────────────────────
    # 3️⃣ Delete Old Diff Data
    # ──────────────────────────────────────────────

    def run_deid_delete(**context):
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_deid_delete.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            script_path,
            "--tables_filepath", params["tables_filepath"],
            "--state_dir",       params["state_dir"],
        ]

        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise RuntimeError("Deidentified-diff delete failed")

    delete_task = PythonOperator(
        task_id="delete_deid_diff",
        python_callable=run_deid_delete,
        trigger_rule=TriggerRule.ALL_DONE,  # run even if merge partially failed
    )

    # ──────────────────────────────────────────────
    # Dependencies
    # ──────────────────────────────────────────────

    status_check_task >> merge_task >> delete_task
