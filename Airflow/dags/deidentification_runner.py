from __future__ import annotations

import os
import subprocess
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────

local_tz = pendulum.timezone("Asia/Kolkata")

BASE_REPO_DIR = "/Users/ndaicnsarizona/Desktop/deidentification/"
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
    dag_id="deidentification_runner",
    description="Deidentification Runner for given set of tables",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["deid"],
    params={
        "num_workers": 5,
        "dbdetailsmodel_name": "arizona_mobiledoc_nov_diff",
        "tables_filepath": "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/input/tables_deid_test.csv",
    },
) as dag:

    # ──────────────────────────────────────────────
    # 1️⃣ Close Existing Screen Sessions
    # ──────────────────────────────────────────────
    close_screen_script = os.path.abspath(
        os.path.join(SCRIPT_DIR, "..", "scripts", "close_screen_sessions.sh")
    )

    close_screen_sessions = BashOperator(
        task_id="close_screen_sessions",
        bash_command=f"bash {close_screen_script} ",  # trailing space prevents Jinja template file lookup
    )

    # ──────────────────────────────────────────────
    # 2️⃣ Run Deidentification Task Creation
    # ──────────────────────────────────────────────
    def run_deid(**context):
        params = context["params"]

        run_script = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_deidentification_run.py")
        )

        cmd = [
            PYTHON_EXECUTABLE,
            run_script,
            "--dbdetailsmodel_name",
            params["dbdetailsmodel_name"],
            "--tables_filepath",
            str(params["tables_filepath"]),
        ]

        result = subprocess.run(cmd)

        if result.returncode != 0:
            raise RuntimeError("Deidentification Task Creation failed")

    run_deid_task = PythonOperator(
        task_id="run_deid",
        python_callable=run_deid,
    )

    # ──────────────────────────────────────────────
    # 3️⃣ Start Workers
    # ──────────────────────────────────────────────
    start_workers_script = os.path.abspath(
        os.path.join(SCRIPT_DIR, "..", "scripts", "start_workers_1.sh")
    )

    start_workers = BashOperator(
        task_id="start_workers",
        bash_command=f"bash {start_workers_script} {{{{ params.num_workers }}}}",
    )

    # ──────────────────────────────────────────────
    # Dependencies
    # ──────────────────────────────────────────────
    close_screen_sessions >> run_deid_task >> start_workers