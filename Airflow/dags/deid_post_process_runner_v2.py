from __future__ import annotations

import os
import sys
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
sys.path.insert(0, os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts")))

from db_config import DEID_POST_PROCESS_TABLES_CSV  # noqa: E402

PYTHON_EXECUTABLE = os.environ.get("AIRFLOW_PYTHON", "python")

# Default config paths — stored inside scripts/configs/
_CONFIGS_DIR    = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts", "configs"))
_DEFAULT_CONFIG = os.path.join(_CONFIGS_DIR, "config_incremental.yaml")
_DEFAULT_TABLES = DEID_POST_PROCESS_TABLES_CSV

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="deid_post_process_runner_v2",
    description="Post-deidentification v2: status check → merge → delete",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["deid", "v2"],
    params={
        "config_yaml":     _DEFAULT_CONFIG,
        "tables_filepath": _DEFAULT_TABLES,
    },
) as dag:

    # ──────────────────────────────────────────────
    # 1. Check Deidentification Status
    #
    #    Reads state.db (written by deid run) to determine
    #    per-table status, then writes JSON state files for
    #    compatibility with the merge and delete scripts.
    # ──────────────────────────────────────────────

    def run_status_check(**context):
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_deid_status_check_v2.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            script_path,
            "--config-yaml",     params["config_yaml"],
            "--tables-filepath", params["tables_filepath"],
        ]

        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise RuntimeError("Deidentification status check (v2) failed")

    status_check_task = PythonOperator(
        task_id="check_deid_status_v2",
        python_callable=run_status_check,
    )

    # ──────────────────────────────────────────────
    # 2. Merge Deidentified Data  (unchanged)
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
        ]

        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise RuntimeError("Deidentified merge failed")

    merge_task = PythonOperator(
        task_id="merge_deid_data",
        python_callable=run_deid_merge,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ──────────────────────────────────────────────
    # 3. Delete Old Original DIFFMERGED Data  (unchanged)
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
        ]

        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise RuntimeError("Original DIFFMERGED delete failed")

    delete_task = PythonOperator(
        task_id="delete_original_diffmerged",
        python_callable=run_deid_delete,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ──────────────────────────────────────────────
    # Dependencies
    # ──────────────────────────────────────────────

    status_check_task >> merge_task >> delete_task
