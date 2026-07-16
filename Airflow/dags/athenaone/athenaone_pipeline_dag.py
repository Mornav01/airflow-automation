from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime

import yaml

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.task.trigger_rule import TriggerRule

# Add dags/ directory to sys.path so the shared package is importable.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from shared.notifications import default_args  # noqa: E402 — must follow sys.path insert
from shared.tasks import (  # noqa: E402
    _MASTER_STATE_FILE,
    _PII_STATE_FILE,
    _SCRIPTS_DIR,
    _run_streaming,
    make_master_insurance,
    make_pii_data,
    task_check_deid_status,
    task_delete_diffmerged,
    task_init_deid_state,
    task_merge_deid_data,
    task_run_deid,
    task_s3_transfer,
)

logger = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Kolkata")


# ──────────────────────────────────────────────────────────────────────────────
# AthenaOne-specific paths
# ──────────────────────────────────────────────────────────────────────────────
_ATHENAONE_SCRIPTS_DIR = os.path.join(_SCRIPTS_DIR, "athenaone")
_DEFAULT_CONFIG        = os.path.join(_ATHENAONE_SCRIPTS_DIR, "athenaone_config_incremental.yaml")
_MAPPING_MASTER_CONFIG = os.path.join(_ATHENAONE_SCRIPTS_DIR, "athenaone_mapping_master_config.yaml")
_SNOWFLAKE_INGEST      = os.path.join(_ATHENAONE_SCRIPTS_DIR, "snowflake_ingest.py")
_MAPPING_SCRIPT        = os.path.join(_ATHENAONE_SCRIPTS_DIR, "incremental_mapping_update_v5_athenaone.py")

# State files:
#   mapping  → chosen by the v5 mapping script (STATE_DIR/mapping_master_v5/…)
#   pii/master → the shared master-delta tasks write _PII_STATE_FILE / _MASTER_STATE_FILE
#                (STATE_DIR/mapping_master/…), imported from shared.tasks.
_STATE_DIR          = os.environ.get("STATE_DIR", "")
_MAPPING_STATE_FILE = os.path.join(_STATE_DIR, "mapping_master_v5", "v5_mapping_update_state.json")


# ──────────────────────────────────────────────────────────────────────────────
# Client config — tunable pipeline behaviour per practice.
# Each machine sets PRACTICE_NAME=<name> in its .env; clients/<name>.yaml is
# committed and supplies DAG param defaults (overridable in the Airflow UI).
# ──────────────────────────────────────────────────────────────────────────────
PRACTICE_NAME = os.environ.get("PRACTICE_NAME", "")


def _load_client_config() -> dict:
    if not PRACTICE_NAME:
        logger.warning("PRACTICE_NAME not set in .env — DAG will use hardcoded defaults")
        return {}
    path = os.path.join(_ATHENAONE_SCRIPTS_DIR, "clients", f"{PRACTICE_NAME}.yaml")
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

DEID_TABLES_CSV              = os.environ.get("DEID_TABLES_CSV", "")
DEID_POST_PROCESS_TABLES_CSV = os.environ.get("DEID_POST_PROCESS_TABLES_CSV", "")


# ──────────────────────────────────────────────────────────────────────────────
# VPN control — the Snowflake pull runs over the practice OpenVPN tunnel.
# Ported from the reference repo's services/vpn_service.py, made env-driven so the
# binary/config/log paths are not hardcoded. Only AthenaOne needs this, so it
# lives in the AthenaOne DAG rather than shared/tasks.py.
# ──────────────────────────────────────────────────────────────────────────────
_OPENVPN_BIN         = os.environ.get("ATHENAONE_OPENVPN_BIN", "/opt/homebrew/sbin/openvpn")
_OPENVPN_CONFIG      = os.environ.get("ATHENAONE_OPENVPN_CONFIG", "/etc/openvpn/nd-platform.ovpn")
_OPENVPN_LOG         = os.environ.get("ATHENAONE_OPENVPN_LOG", "/tmp/openvpn_airflow.log")
_VPN_CONNECT_TIMEOUT = int(os.environ.get("ATHENAONE_VPN_CONNECT_TIMEOUT", "60"))


def _vpn_should_skip(params) -> bool:
    """VPN is skipped when explicitly disabled for the practice, or when the
    ingest itself is skipped (nothing needs the tunnel)."""
    if params.get("skip_vpn", False):
        logger.info("skip_vpn=True — VPN step skipped")
        return True
    if params.get("skip_ingest", False):
        logger.info("skip_ingest=True — no extraction needs the VPN; skipping VPN step")
        return True
    return False


def task_vpn_connect(**context):
    """Start OpenVPN as a background daemon and wait until the tunnel is up.
    Raises if the tunnel is not up within the timeout so the pull never runs
    off-tunnel."""
    if _vpn_should_skip(context["params"]):
        return

    # Clear the previous log so we never match an old run's entries.
    try:
        os.remove(_OPENVPN_LOG)
    except (FileNotFoundError, PermissionError):
        pass

    subprocess.run(
        ["sudo", _OPENVPN_BIN, "--config", _OPENVPN_CONFIG, "--daemon", "--log", _OPENVPN_LOG],
        check=True,
    )

    deadline = time.time() + _VPN_CONNECT_TIMEOUT
    while time.time() < deadline:
        time.sleep(2)
        if subprocess.run(["pgrep", "-x", "openvpn"], capture_output=True).returncode == 0:
            time.sleep(5)  # let the tunnel finish establishing before the pull
            logger.info("VPN connected (config=%s)", _OPENVPN_CONFIG)
            return
    raise RuntimeError(f"VPN did not connect within {_VPN_CONNECT_TIMEOUT}s — see {_OPENVPN_LOG}")


def task_vpn_disconnect(**context):
    """Stop all OpenVPN processes. Idempotent — runs with trigger_rule ALL_DONE
    as a non-gating side branch so the tunnel is always torn down, even if the
    ingest failed."""
    if _vpn_should_skip(context["params"]):
        return

    result = subprocess.run(["sudo", "killall", "openvpn"], capture_output=True, text=True)
    # killall returns 1 when no process was found — not an error.
    if result.returncode not in (0, 1):
        raise RuntimeError(f"vpn_disconnect failed: {result.stderr}")
    logger.info("VPN disconnected")


# ──────────────────────────────────────────────────────────────────────────────
# AthenaOne-specific task callables
# ──────────────────────────────────────────────────────────────────────────────

def task_snowflake_ingest(**context):
    """Snowflake → MySQL ingestion (the 10-step pull). AthenaOne's equivalent of
    eCW/Greenway's restore + diff_merge."""
    if context["params"].get("skip_ingest", False):
        logger.info("skip_ingest=True — skipping Snowflake ingestion")
        return

    rc, out = _run_streaming([DEID_PYTHON, _SNOWFLAKE_INGEST], env=_ENV)
    if rc != 0:
        raise RuntimeError(f"snowflake_ingest failed (rc={rc}):\n{out[-4000:]}")


def task_check_continue(**context):
    """Returns False (skips downstream) when stop_after_ingest=True."""
    if context["params"].get("stop_after_ingest", False):
        logger.info("stop_after_ingest=True — skipping mapping/master + deid stages")
        return False
    logger.info("Continuing to mapping/master + deid stages")
    return True


def _run_v5(script: str, params: dict):
    """Invoke a v5 mapping/master script with cwd=scripts/athenaone so the
    relative sql_file paths in the config resolve, and so `from config import …`
    finds scripts/athenaone/config.py."""
    cmd = [DEID_PYTHON, "-u", script, "--config", params["mapping_master_config"]]
    if params.get("dry_run", False):
        cmd.append("--dry-run")
    rc, out = _run_streaming(cmd, env=_ENV, cwd=_ATHENAONE_SCRIPTS_DIR)
    if rc != 0:
        raise RuntimeError(f"{os.path.basename(script)} failed (rc={rc}):\n{out[-4000:]}")


def task_run_mapping(**context):
    """V5 incremental mapping update — populates patient/chart/encounter mapping
    tables in mapping_pg. Kept as the athena-family v5 mapper (handles the
    patientid/enterpriseid identity resolution)."""
    _run_v5(_MAPPING_SCRIPT, context["params"])


# Master tables are TRUE DELTA (upsert the incremental Snowflake pull), via the
# shared factory tasks — same master_pii_delta.py / master_insurance_delta.py
# contract as eCW/Greenway.
task_pii_data         = make_pii_data(_ATHENAONE_SCRIPTS_DIR)
task_master_insurance = make_master_insurance(_ATHENAONE_SCRIPTS_DIR)


def task_validate_mapping_master(**_):
    """Assert mapping + both master-delta state files report success today.
    Handles both state formats: the v5 mapping writes `last_processed_date`
    (local date); the shared master-delta tasks write `completed_at` (UTC)."""
    today  = date.today().isoformat()
    failed = []

    # v5 mapping — last_processed_date (local)
    if not os.path.exists(_MAPPING_STATE_FILE):
        raise RuntimeError(f"State file missing for v5_mapping: {_MAPPING_STATE_FILE}")
    with open(_MAPPING_STATE_FILE) as f:
        mstate = json.load(f)
    logger.info("State [v5_mapping]:\n%s", json.dumps(mstate, indent=4))
    if not mstate.get("success", False):
        failed.append("v5_mapping (success=false)")
    elif mstate.get("last_processed_date", "") != today:
        failed.append(f"v5_mapping (stale: {mstate.get('last_processed_date')!r}, not today {today})")

    # master deltas — completed_at (UTC → local date)
    for name, path in {"pii_data": _PII_STATE_FILE, "master_insurance": _MASTER_STATE_FILE}.items():
        if not os.path.exists(path):
            raise RuntimeError(f"State file missing for {name}: {path}")
        with open(path) as f:
            state = json.load(f)
        logger.info("State [%s]:\n%s", name, json.dumps(state, indent=4))
        if not state.get("success", False):
            failed.append(f"{name} (success=false)")
            continue
        completed_at = state.get("completed_at", "")
        try:
            run_date = datetime.fromisoformat(completed_at).astimezone().date().isoformat()
        except ValueError:
            failed.append(f"{name} (invalid/missing completed_at: {completed_at!r})")
            continue
        if run_date != today:
            failed.append(f"{name} (stale: last success {run_date}, not today {today})")

    if failed:
        raise RuntimeError(f"Mapping/master failed or stale. Components: {', '.join(failed)}")


# ══════════════════════════════════════════════════════════════════════════════
# DAG definition
# ══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="athenaone_pipeline",
    description="Full AthenaOne pipeline: snowflake ingest → mapping (v5) → master delta → deid v2 → post-process → s3",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["athenaone", "deid", "pipeline"],
    params={
        # Mapping (v5 config-driven)
        "mapping_master_config": _MAPPING_MASTER_CONFIG,
        "dry_run":               False,
        # Master delta (shared master_pii_delta.py / master_insurance_delta.py):
        # read the fresh Snowflake pull (staging = incremental schema), upsert into
        # master_pg keyed via mapping_pg.
        "master_schema":  "master_pg",
        "mapping_schema": "mapping_pg",
        "staging_schema": os.environ.get("ATHENAONE_INCREMENTAL_SCHEMA", ""),
        # Deid
        "config_yaml":                  _DEFAULT_CONFIG,
        "deid_tables_csv":              DEID_TABLES_CSV,
        "deid_post_process_tables_csv": DEID_POST_PROCESS_TABLES_CSV,
        # S3 Transfer
        "s3_deid_schema": os.environ.get("DEID_STAGING_NAME", "deidentified_staging"),
        "parser_workers": _CLIENT_CFG.get("parser_workers", None),
        # Control / parallelism — defaults from clients/<practice>.yaml
        "skip_vpn":               _CLIENT_CFG.get("skip_vpn", False),
        "skip_ingest":            _CLIENT_CFG.get("skip_ingest", False),
        "stop_after_ingest":      _CLIENT_CFG.get("stop_after_ingest", False),
        "deid_init_max_workers":   _CLIENT_CFG.get("deid_init_max_workers", 16),
        "deid_status_max_workers": _CLIENT_CFG.get("deid_status_max_workers", 8),
        "deid_merge_max_workers":  _CLIENT_CFG.get("deid_merge_max_workers", 8),
        "deid_delete_max_workers": _CLIENT_CFG.get("deid_delete_max_workers", 8),
    },
) as dag:

    vpn_connect = PythonOperator(
        task_id="vpn_connect",
        python_callable=task_vpn_connect,
    )

    snowflake_ingest = PythonOperator(
        task_id="snowflake_ingest",
        python_callable=task_snowflake_ingest,
        execution_timeout=None,
    )

    # Non-gating side branch: always tear down the tunnel after the pull, even on
    # failure. Does NOT gate the mapping/deid path (ALL_DONE), so an ingest
    # failure still propagates through check_continue.
    vpn_disconnect = PythonOperator(
        task_id="vpn_disconnect",
        python_callable=task_vpn_disconnect,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    check_continue = ShortCircuitOperator(
        task_id="check_continue",
        python_callable=task_check_continue,
    )

    run_mapping = PythonOperator(
        task_id="run_mapping",
        python_callable=task_run_mapping,
    )

    pii_data = PythonOperator(
        task_id="pii_data",
        python_callable=task_pii_data,
    )

    master_insurance = PythonOperator(
        task_id="master_insurance",
        python_callable=task_master_insurance,
    )

    validate_mapping_master = PythonOperator(
        task_id="validate_mapping_master",
        python_callable=task_validate_mapping_master,
        trigger_rule=TriggerRule.ALL_DONE,
    )

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
    # VPN up → ingest (Snowflake→MySQL) → checkpoint → mapping → master deltas → validate.
    # vpn_disconnect is a side branch (ALL_DONE) so the tunnel always tears down;
    # the mapping/deid path continues from snowflake_ingest via check_continue.
    vpn_connect >> snowflake_ingest >> vpn_disconnect
    snowflake_ingest >> check_continue >> run_mapping
    run_mapping >> [pii_data, master_insurance] >> validate_mapping_master

    # validate → deid init → run deid → status check
    validate_mapping_master >> init_deid_state >> run_deid >> check_deid_status

    # Post-deid: merge and S3 transfer run in parallel, then cleanup
    check_deid_status >> [merge_deid_data, s3_transfer]
    merge_deid_data >> delete_original_diffmerged
