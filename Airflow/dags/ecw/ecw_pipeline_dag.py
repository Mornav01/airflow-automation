from __future__ import annotations

import logging
import os
import sys

import yaml

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.task.trigger_rule import TriggerRule

# Add dags/ directory to sys.path so the shared package is importable.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from shared.notifications import default_args  # noqa: E402 — must follow sys.path insert
from shared.tasks import (  # noqa: E402
    _SCRIPTS_DIR,
    _run_streaming,
    make_check_db_availability,
    make_mapping_update_v5,
    make_master_insurance,
    make_pii_data,
    make_restore,
    task_check_continue,
    task_check_deid_status,
    task_delete_diffmerged,
    task_init_deid_state,
    task_merge_deid_data,
    task_run_deid,
    task_run_diff_merge,
    task_s3_transfer,
    task_validate_diff_merge,
    task_validate_mapping,
)

logger = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Kolkata")


# ──────────────────────────────────────────────────────────────────────────────
# eCW-specific paths
# ──────────────────────────────────────────────────────────────────────────────
_ECW_SCRIPTS_DIR = os.path.join(_SCRIPTS_DIR, "ecw")
_DEFAULT_CONFIG  = os.path.join(_ECW_SCRIPTS_DIR, "ecw_config_incremental.yaml")
_MAPPING_CONFIG  = os.path.join(_ECW_SCRIPTS_DIR, "ecw_mapping_config.yaml")


# ──────────────────────────────────────────────────────────────────────────────
# Client config — tunable pipeline behaviour per practice.
# Each Mac Studio sets PRACTICE_NAME=<name> in its .env.
# scripts/ecw/clients/<name>.yaml is committed to git.
# ──────────────────────────────────────────────────────────────────────────────
PRACTICE_NAME = os.environ.get("PRACTICE_NAME", "")


def _load_client_config() -> dict:
    if not PRACTICE_NAME:
        logger.warning("PRACTICE_NAME not set in .env — DAG will use hardcoded defaults")
        return {}
    path = os.path.join(_ECW_SCRIPTS_DIR, "clients", f"{PRACTICE_NAME}.yaml")
    try:
        with open(path) as _f:
            return yaml.safe_load(_f) or {}
    except FileNotFoundError:
        logger.warning("Client config not found: %s — using hardcoded defaults", path)
        return {}


_CLIENT_CFG = _load_client_config()


# ──────────────────────────────────────────────────────────────────────────────
# Runtime config — all values sourced from .env (loaded by shared/__init__.py)
# ──────────────────────────────────────────────────────────────────────────────
DEID_PYTHON = os.environ.get("DEID_PYTHON", "/opt/miniconda3/envs/air_deid/bin/python")
_ENV        = {**os.environ, "PYTHONUNBUFFERED": "1"}

STATE_DIR                    = os.environ.get("STATE_DIR", "")
DEID_TABLES_CSV              = os.environ.get("DEID_TABLES_CSV", "")
DEID_POST_PROCESS_TABLES_CSV = os.environ.get("DEID_POST_PROCESS_TABLES_CSV", "")
PK_CSV                       = os.environ.get("PK_CSV", "")
DIFF_MERGE_TABLES_CSV        = os.environ.get("DIFF_MERGE_TABLES_CSV", "")


# ──────────────────────────────────────────────────────────────────────────────
# eCW-specific task callables
# ──────────────────────────────────────────────────────────────────────────────

# eCW pnotes decryption supports both MySQL and MSSQL source databases.
# Set SOURCE_DB_TYPE=mysql (default) or mssql in the practice .env.
# staging_schema is passed explicitly; credentials come from the environment.
_SOURCE_DB_TYPE = os.environ.get("SOURCE_DB_TYPE", "mysql")


def task_decrypt_pnotes(**context):
    params = context["params"]
    script = os.path.join(_ECW_SCRIPTS_DIR, "decrypt_pnotes.py")
    cmd = [
        DEID_PYTHON, script,
        "--staging_schema", params["staging_schema"],
        "--db_type", _SOURCE_DB_TYPE,
    ]
    rc, out = _run_streaming(cmd, env=_ENV)
    if rc != 0:
        raise RuntimeError(f"decrypt_pnotes failed (rc={rc}):\n{out[-4000:]}")


# ──────────────────────────────────────────────────────────────────────────────
# EHR-specific instantiations of shared factory tasks
# ──────────────────────────────────────────────────────────────────────────────
task_check_db_availability = make_check_db_availability(_ECW_SCRIPTS_DIR)
task_restore               = make_restore(_ECW_SCRIPTS_DIR)
task_mapping_update   = make_mapping_update_v5(_MAPPING_CONFIG)
task_pii_data         = make_pii_data(_ECW_SCRIPTS_DIR)
task_master_insurance = make_master_insurance(_ECW_SCRIPTS_DIR)


# ══════════════════════════════════════════════════════════════════════════════
# DAG definition
# ══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="ecw_pipeline",
    description="Full eCW pipeline: db-check → restore → diff+merge → mapping+pnotes → deid v2 → post-process",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["ecw", "deid", "pipeline"],
    params={
        # Diff+Merge
        "diff_merge_tables_csv": DIFF_MERGE_TABLES_CSV,
        "pk_csv":                PK_CSV,
        "state_dir":             STATE_DIR,
        # Mapping / Master delta schemas
        "mapping_schema": os.environ.get("MAPPING_DB_NAME", "mapping"),
        "master_schema":  os.environ.get("MASTER_DB_NAME", "master"),
        "staging_schema": os.environ.get("STAGING_DB_NAME", ""),
        # Deid
        "config_yaml":                  _DEFAULT_CONFIG,
        "deid_tables_csv":              DEID_TABLES_CSV,
        "deid_post_process_tables_csv": DEID_POST_PROCESS_TABLES_CSV,
        # S3 Transfer
        "s3_deid_schema": os.environ.get("DEID_MERGED_NAME", "deidentified_mobiledoc_diff_merged"),
        "parser_workers":  _CLIENT_CFG.get("parser_workers", None),
        # Control / parallelism — defaults from clients/<practice>.yaml
        "skip_restore":            _CLIENT_CFG.get("skip_restore", False),
        "stop_after_diff_merge":   _CLIENT_CFG.get("stop_after_diff_merge", False),
        "diff_merge_max_workers":  _CLIENT_CFG.get("diff_merge_max_workers", 16),
        "deid_init_max_workers":   _CLIENT_CFG.get("deid_init_max_workers", 16),
        "deid_status_max_workers": _CLIENT_CFG.get("deid_status_max_workers", 8),
        "deid_merge_max_workers":  _CLIENT_CFG.get("deid_merge_max_workers", 8),
        "deid_delete_max_workers": _CLIENT_CFG.get("deid_delete_max_workers", 8),
    },
) as dag:

    check_db_availability = PythonOperator(
        task_id="check_db_availability",
        python_callable=task_check_db_availability,
    )

    restore = PythonOperator(
        task_id="restore",
        python_callable=task_restore,
    )

    run_diff_merge = PythonOperator(
        task_id="run_diff_merge",
        python_callable=task_run_diff_merge,
    )

    validate_diff_merge = PythonOperator(
        task_id="validate_diff_merge",
        python_callable=task_validate_diff_merge,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    check_continue = ShortCircuitOperator(
        task_id="check_continue",
        python_callable=task_check_continue,
    )

    # ── Parallel deidentification prep (mapping tables + pnotes) ──────────────
    mapping_update = PythonOperator(
        task_id="mapping_update",
        python_callable=task_mapping_update,
    )

    pii_data = PythonOperator(
        task_id="pii_data",
        python_callable=task_pii_data,
    )

    master_insurance = PythonOperator(
        task_id="master_insurance",
        python_callable=task_master_insurance,
    )

    decrypt_pnotes = PythonOperator(
        task_id="decrypt_pnotes",
        python_callable=task_decrypt_pnotes,
    )

    validate_mapping = PythonOperator(
        task_id="validate_mapping",
        python_callable=task_validate_mapping,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # init_deid_state fans in from both validate_mapping (mapping branch) and
    # decrypt_pnotes so deid never starts until mapping tables AND pnotes are ready.
    init_deid_state = PythonOperator(
        task_id="init_deid_state",
        python_callable=task_init_deid_state,
    )

    run_deid = PythonOperator(
        task_id="run_deid",
        python_callable=task_run_deid,
        execution_timeout=None,
    )

    check_deid_status = PythonOperator(
        task_id="check_deid_status",
        python_callable=task_check_deid_status,
    )

    merge_deid_data = PythonOperator(
        task_id="merge_deid_data",
        python_callable=task_merge_deid_data,
    )

    s3_transfer = PythonOperator(
        task_id="s3_transfer",
        python_callable=task_s3_transfer,
    )

    delete_original_diffmerged = PythonOperator(
        task_id="delete_original_diffmerged",
        python_callable=task_delete_diffmerged,
    )

    # ── Pipeline ───────────────────────────────────────────────────────────────
    # Verify the DBs are free before restore flips them into RESTORING/standby mode,
    # then apply the logs dumped by the separate daily-dump DAG.
    check_db_availability >> restore >> run_diff_merge >> validate_diff_merge >> check_continue

    # All four prep tasks start in parallel after the checkpoint
    check_continue >> [mapping_update, pii_data, master_insurance, decrypt_pnotes]

    # Mapping branch: validate once all three mapping tasks finish
    [mapping_update, pii_data, master_insurance] >> validate_mapping

    # deid init waits for mapping validation AND pnotes decryption
    [validate_mapping, decrypt_pnotes] >> init_deid_state

    init_deid_state >> run_deid >> check_deid_status

    # Post-deid: merge and S3 transfer run in parallel, then cleanup
    check_deid_status >> [merge_deid_data, s3_transfer]
    merge_deid_data >> delete_original_diffmerged
