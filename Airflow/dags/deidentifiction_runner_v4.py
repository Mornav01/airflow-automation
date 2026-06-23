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
    MAPPING_CONFIG_FILE,
    DEID_TABLES_CSV,
    PROJECT_NAME,
)

PYTHON_EXECUTABLE = os.environ.get("AIRFLOW_PYTHON", "python")
DEID_PYTHON       = os.environ.get("DEID_PYTHON", PYTHON_EXECUTABLE)

MAPPING_SCRIPT = os.path.abspath(
    os.path.join(SCRIPT_DIR, "..", "scripts", "incremental_mapping_update_v5.py")
)
MASTER_SCRIPT = os.path.abspath(
    os.path.join(SCRIPT_DIR, "..", "scripts", "master_recreate_v3.py")
)

STATE_BASE         = os.path.join(os.environ["STATE_DIR"], "mapping_master_v5")
MAPPING_STATE_FILE = os.path.join(STATE_BASE, "v5_mapping_update_state.json")
MASTER_STATE_FILE  = os.path.join(os.environ["STATE_DIR"], "mapping_master_v3", "v3_master_state.json")

# Deid config
_CONFIGS_DIR    = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts", "configs"))
_DEFAULT_CONFIG = os.path.join(_CONFIGS_DIR, PROJECT_NAME, "config_incremental.yaml")

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="deidentification_runner_v4",
    description="V5 Mapping Master → Deidentification Runner (deid pg)",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["deid", "mapping", "v5", "v4"],
    params={
        # V3 Mapping params
        "config_file": MAPPING_CONFIG_FILE,
        "dry_run":     False,
        # Deidentification v2 params
        "config_yaml":     _DEFAULT_CONFIG,
        "tables_filepath": DEID_TABLES_CSV,
    },
) as dag:

    # ──────────────────────────────────────────────
    # 1. Run V5 Mapping + Master Jobs
    # ──────────────────────────────────────────────

    def run_mapping_jobs(**context):
        params      = context["params"]
        config_file = params["config_file"]
        extra_args  = ["--dry-run"] if params["dry_run"] else []

        def _stream(cmd):
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

        print(f"[mapping] Running with config: {config_file}", flush=True)
        _stream([PYTHON_EXECUTABLE, "-u", MAPPING_SCRIPT, "--config", config_file] + extra_args)

        print(f"[master] Running with config: {config_file}", flush=True)
        _stream([PYTHON_EXECUTABLE, "-u", MASTER_SCRIPT, "--config", config_file] + extra_args)

    run_mapping_task = PythonOperator(
        task_id="run_mapping_jobs",
        python_callable=run_mapping_jobs,
    )

    # ──────────────────────────────────────────────
    # 2. Validate Mapping State Files
    # ──────────────────────────────────────────────

    def validate_mapping_master(**_):
        def _is_success(state: dict) -> bool:
            if "success" in state:
                return bool(state["success"])
            # nested format: {"pii_data": {"success": true}, ...}
            sub_states = [v for v in state.values() if isinstance(v, dict)]
            return bool(sub_states) and all(v.get("success", False) for v in sub_states)

        state_files = {
            "v5_mapping": MAPPING_STATE_FILE,
            "master":     MASTER_STATE_FILE,
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
                f"Mapping pipeline failed. Failed components: {', '.join(failed)}"
            )

    validate_mapping_task = PythonOperator(
        task_id="validate_mapping_master",
        python_callable=validate_mapping_master,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ──────────────────────────────────────────────
    # 3. Init JSON state files (before deid run)
    # ──────────────────────────────────────────────

    def init_deidentification_state(**context):
        params = context["params"]

        script_path = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "scripts", "state_deidentification_init_v4.py")
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
        task_id="init_deidentification_state",
        python_callable=init_deidentification_state,
    )

    # ──────────────────────────────────────────────
    # 4. Run deid pg
    # ──────────────────────────────────────────────

    def run_deid_pg(**context):
        params = context["params"]

        cmd = [
            DEID_PYTHON, "-m", "deid", "run",
            "--config",     params["config_yaml"],
            "--tables-csv", params["tables_filepath"],
            "--rerun",
        ]

        result = subprocess.run(cmd)

        if result.returncode != 0:
            raise RuntimeError(
                f"deid run failed with exit code {result.returncode}"
            )

    run_deid_task = PythonOperator(
        task_id="run_deid_pg",
        python_callable=run_deid_pg,
        execution_timeout=None,
    )

    # ──────────────────────────────────────────────
    # Dependencies
    # ──────────────────────────────────────────────

    run_mapping_task >> validate_mapping_task >> init_state_task >> run_deid_task
