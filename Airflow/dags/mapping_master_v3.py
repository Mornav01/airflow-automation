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

local_tz = pendulum.timezone("Asia/Kolkata")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts")))

from db_config import V3_MAPPING_CONFIG_FILE  # noqa: E402

PYTHON_EXECUTABLE = os.environ.get("AIRFLOW_PYTHON", "python")

MAPPING_SCRIPT = os.path.abspath(
    os.path.join(SCRIPT_DIR, "..", "scripts", "incremental_mapping_update_v3.py")
)
MASTER_SCRIPT = os.path.abspath(
    os.path.join(SCRIPT_DIR, "..", "scripts", "master_recreate_v3.py")
)

STATE_BASE         = os.path.join(os.environ["STATE_DIR"], "mapping_master_v3")
MAPPING_STATE_FILE = os.path.join(STATE_BASE, "v3_mapping_update_state.json")
MASTER_STATE_FILE  = os.path.join(STATE_BASE, "v3_master_state.json")

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="mapping_master_v3_runner",
    description="V3 Mapping + Master Runner",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["mapping", "master", "v3"],
    params={
        # Default from .env (V3_MAPPING_CONFIG_FILE) — override in Airflow UI at runtime
        "config_file": V3_MAPPING_CONFIG_FILE,
        # Set to true to run both scripts in dry-run mode (no DB writes)
        "dry_run": False,
    },
) as dag:

    # ─────────────────────────────────────────────────────────
    # Task 1: Run mapping then master scripts
    # ─────────────────────────────────────────────────────────

    def run_all_jobs(**context):
        params      = context["params"]
        config_file = params["config_file"]
        extra_args  = ["--dry-run"] if params["dry_run"] else []

        def _stream(cmd):
            """Run a subprocess and stream its output line-by-line to the Airflow task log."""
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
            for line in proc.stdout:
                print(line, end="", flush=True)
            rc = proc.wait()
            if rc != 0:
                raise RuntimeError(f"Script failed (exit {rc}): {cmd[2]}")

        # 1. V3 Mapping Update — populates patient_mapping_table + child tables
        print(f"[mapping] Running with config: {config_file}", flush=True)
        _stream([PYTHON_EXECUTABLE, "-u", MAPPING_SCRIPT, "--config", config_file] + extra_args)

        # 2. V3 Master Recreate — builds pii_data_table / master tables
        print(f"[master] Running with config: {config_file}", flush=True)
        _stream([PYTHON_EXECUTABLE, "-u", MASTER_SCRIPT, "--config", config_file] + extra_args)

    run_task = PythonOperator(
        task_id="run_all_jobs",
        python_callable=run_all_jobs,
    )

    # ─────────────────────────────────────────────────────────
    # Task 2: Validate state files
    # ─────────────────────────────────────────────────────────

    def validate_pipeline(**context):
        def _is_success(state: dict) -> bool:
            if "success" in state:
                return bool(state["success"])
            # nested format: {"pii_data": {"success": true}, ...}
            sub_states = [v for v in state.values() if isinstance(v, dict)]
            return bool(sub_states) and all(v.get("success", False) for v in sub_states)

        state_files = {
            "v3_mapping": MAPPING_STATE_FILE,
            "v3_master":  MASTER_STATE_FILE,
        }

        failed = []

        for name, path in state_files.items():
            if not os.path.exists(path):
                raise RuntimeError(f"State file not found for {name}: {path}")

            with open(path) as f:
                state = json.load(f)

            print(f"\n===== STATE: {name} =====")
            print(json.dumps(state, indent=4))

            if not _is_success(state):
                failed.append(name)

        if failed:
            raise RuntimeError(
                f"Pipeline failed. Failed components: {', '.join(failed)}"
            )

    validate_task = PythonOperator(
        task_id="validate_pipeline",
        python_callable=validate_pipeline,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    run_task >> validate_task
