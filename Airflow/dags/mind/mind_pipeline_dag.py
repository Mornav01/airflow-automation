from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import traceback as _traceback
from datetime import date, datetime, timedelta, timezone

import pendulum
import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.task.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Load .env at parse time so os.environ.get() calls below work correctly even
# when the Airflow worker process doesn't inherit the shell environment.
# Uses setdefault — does NOT override values already set in the environment.
# ──────────────────────────────────────────────────────────────────────────────
def _load_env_file(path: str) -> None:
    try:
        with open(path) as _f:
            for _line in _f:
                _line = _line.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _key, _, _val = _line.partition("=")
                _key = _key.strip()
                _val = _val.strip().strip('"').strip("'")
                os.environ.setdefault(_key, _val)
    except FileNotFoundError:
        logger.warning("No .env file found at %s", path)


_load_env_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env"))


# ──────────────────────────────────────────────────────────────────────────────
# Path resolution
# Resolve Airflow/scripts/ regardless of where inside dags/ this file lives.
# ──────────────────────────────────────────────────────────────────────────────
_AIRFLOW_DIR      = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_SCRIPTS_DIR      = os.path.join(_AIRFLOW_DIR, "scripts")
_MIND_SCRIPTS_DIR = os.path.join(_SCRIPTS_DIR, "mind")
_CONFIGS_DIR      = os.path.join(_SCRIPTS_DIR, "configs")


# ──────────────────────────────────────────────────────────────────────────────
# Runtime config — all values sourced from .env (loaded above)
# ──────────────────────────────────────────────────────────────────────────────
local_tz = pendulum.timezone("Asia/Kolkata")

DEID_PYTHON = os.environ.get("DEID_PYTHON", "/opt/miniconda3/envs/air_deid/bin/python")
WEBHOOK_URL = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL", "")

_DEFAULT_CONFIG = os.path.join(_CONFIGS_DIR, "mind_config_incremental.yaml")

STATE_DIR                 = os.environ.get("STATE_DIR", "")
DEID_TABLES_CSV           = os.environ.get("DEID_TABLES_CSV", "")
DEID_POST_PROCESS_TABLES_CSV = os.environ.get("DEID_POST_PROCESS_TABLES_CSV", "")
PK_CSV                    = os.environ.get("PK_CSV", "")
DIFF_MERGE_TABLES_CSV     = os.environ.get("DIFF_MERGE_TABLES_CSV", "")

MAPPING_MASTER_STATE_DIR = os.path.join(STATE_DIR, "mapping_master")
MAPPING_STATE_FILE = os.path.join(MAPPING_MASTER_STATE_DIR, "mapping_update_state.json")
PII_STATE_FILE     = os.path.join(MAPPING_MASTER_STATE_DIR, "pii_data_state.json")
MASTER_STATE_FILE  = os.path.join(MAPPING_MASTER_STATE_DIR, "master_insurance_state.json")

_ENV = {**os.environ, "PYTHONUNBUFFERED": "1"}


# ──────────────────────────────────────────────────────────────────────────────
# Webhook notification — Google Chat Cards V2
# ──────────────────────────────────────────────────────────────────────────────
_STATUS_ICON = {
    "SUCCESS":         "✅",
    "FAILED":          "❌",
    "RETRY":           "🔄",
    "SKIPPED":         "⏭️",
    "UPSTREAM_FAILED": "🚫",
}


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h else f"{m}m {s}s"


def _send_webhook(context: dict, status: str) -> None:
    if not WEBHOOK_URL:
        logger.warning("GOOGLE_CHAT_WEBHOOK_URL not configured — skipping notification.")
        return

    dag     = context.get("dag")
    ti      = context.get("task_instance")
    dag_run = context.get("dag_run")
    task    = context.get("task")
    exc     = context.get("exception")

    dag_id   = dag.dag_id if dag else "unknown"
    task_id  = ti.task_id if ti else "unknown"
    run_id   = (dag_run.run_id if dag_run else context.get("run_id", "unknown"))
    log_url  = getattr(ti, "log_url", None) or ""
    hostname = getattr(ti, "hostname", None) or "—"

    # Attempt counters: try_number is 1-based; max_tries = retries setting (0 = no retries)
    try_number = getattr(ti, "try_number", 1) if ti else 1
    max_tries  = (getattr(ti, "max_tries", 0) + 1) if ti else 1

    # Duration
    duration_str = "—"
    if ti and getattr(ti, "start_date", None):
        end   = getattr(ti, "end_date", None) or datetime.now(timezone.utc)
        start = ti.start_date
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if hasattr(end, "tzinfo") and end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        duration_str = _format_duration((end - start).total_seconds())

    # Logical / execution date
    exec_date_str = "—"
    if dag_run:
        ed = getattr(dag_run, "logical_date", None) or getattr(dag_run, "execution_date", None)
        if ed:
            exec_date_str = ed.strftime("%Y-%m-%d %H:%M UTC")

    start_str = "—"
    if ti and getattr(ti, "start_date", None):
        start_str = ti.start_date.strftime("%Y-%m-%d %H:%M:%S UTC")

    # Exception: message + traceback tail extracted from the live exception object
    exc_text = ""
    if exc:
        exc_header = f"<b>{type(exc).__name__}:</b> {str(exc)[:400]}"
        tb = getattr(exc, "__traceback__", None)
        tb_str = ""
        if tb:
            lines = "".join(_traceback.format_tb(tb)).strip().split("\n")
            tb_str = "\n".join(lines[-14:])[:900]
        exc_text = exc_header + (f"\n<pre>{tb_str}</pre>" if tb_str else "")

    # Next retry delay (only relevant for RETRY events)
    retry_str = ""
    if status == "RETRY" and task:
        rd = getattr(task, "retry_delay", None)
        if rd:
            retry_str = f"⏱ Next attempt in {_format_duration(rd.total_seconds())}"

    icon = _STATUS_ICON.get(status, "ℹ️")

    def _dt(label: str, value: str) -> dict:
        return {"decoratedText": {"topLabel": label, "text": value, "wrapText": True}}

    core_widgets = [
        _dt("DAG",        dag_id),
        _dt("Task",       f"<b>{task_id}</b>"),
        _dt("Run ID",     run_id),
        _dt("Exec Date",  exec_date_str),
        _dt("Attempt",    f"{try_number} of {max_tries}"),
        _dt("Started",    start_str),
        _dt("Duration",   duration_str),
        _dt("Worker",     hostname),
    ]

    sections: list = [{"header": "Task Details", "widgets": core_widgets}]

    if exc_text:
        sections.append({
            "header":      "Error Details",
            "collapsible": True,
            "widgets":     [{"textParagraph": {"text": exc_text}}],
        })

    if retry_str:
        sections.append({"widgets": [{"textParagraph": {"text": retry_str}}]})

    if log_url:
        sections.append({
            "widgets": [{
                "buttonList": {
                    "buttons": [{
                        "text":    "Open Task Logs",
                        "onClick": {"openLink": {"url": log_url}},
                    }]
                }
            }]
        })

    payload = {
        "cardsV2": [{
            "cardId": f"{dag_id}__{task_id}__{status}",
            "card": {
                "header": {
                    "title":    f"{icon} {status} — {task_id}",
                    "subtitle": f"DAG: {dag_id}",
                },
                "sections": sections,
            },
        }]
    }

    try:
        resp = requests.post(
            WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.error("Failed to send GChat webhook alert: %s", e)


def on_success_callback(context):
    _send_webhook(context, "SUCCESS")


def on_failure_callback(context):
    _send_webhook(context, "FAILED")


def on_retry_callback(context):
    _send_webhook(context, "RETRY")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_success_callback": on_success_callback,
    "on_failure_callback": on_failure_callback,
    "on_retry_callback":   on_retry_callback,
}


# ══════════════════════════════════════════════════════════════════════════════
# Task callables — all defined at module scope for clarity and testability
# ══════════════════════════════════════════════════════════════════════════════

# ── Stage 0: DB availability guard (before restore) ──────────────────────────
def task_check_db_availability(**context):
    """Fail if any target DB is not ONLINE (e.g. mid-restore) or has active user
    sessions, so the restore never disrupts a job already running on the DB."""
    if context["params"].get("skip_restore", False):
        logger.info("skip_restore=True — skipping DB availability check")
        return

    # Shared, config-driven guard (DB list from RESTORE_DB_CONFIGS in .env).
    script = os.path.join(_SCRIPTS_DIR, "check_db_availability.py")
    result = subprocess.run([DEID_PYTHON, script], capture_output=True, text=True, env=_ENV)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"db availability check failed (rc={result.returncode}):\n{result.stderr or result.stdout}"
        )


# ── Stage 1: Restore incremental logs ─────────────────────────────────────────
# The daily dump (SMB sync) runs in the separate unified daily_dump DAG; the
# pipeline only applies the dumped transaction logs, gated by skip_restore.
def task_restore(**context):
    if context["params"].get("skip_restore", False):
        logger.info("skip_restore=True — skipping restore, proceeding to decrypt")
        return

    # Unified, config-driven restore (DB list from RESTORE_DB_CONFIGS in .env).
    script = os.path.join(_SCRIPTS_DIR, "restore_incremental_logs.py")
    result = subprocess.run([DEID_PYTHON, script], capture_output=True, text=True, env=_ENV)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"restore failed (rc={result.returncode}):\n{result.stderr or result.stdout}"
        )


# ── Stage 2: Decrypt Clinical Notes + Delta Staging ──────────────────────────
def task_decrypt_cnotes(**_):
    script = os.path.join(_MIND_SCRIPTS_DIR, "decrypt_cnotes.py")
    result = subprocess.run([DEID_PYTHON, script], capture_output=True, text=True, env=_ENV)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"decrypt_cnotes failed (rc={result.returncode}):\n{result.stderr or result.stdout}"
        )


# ── Stage 3: Clinicalbin Deid (after delta staging) ─────────────────────────
def task_clinicalbin_deid(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "manual_deidentification_run_v2.py")
    cmd = [
        DEID_PYTHON, script,
        "--config", params["clinicalbin_deid_config"],
        "--tables", params["clinicalbin_tables_csv"],
    ]
    # No capture_output — long-lived process, stream directly to Airflow logs.
    result = subprocess.run(cmd, env=_ENV)
    if result.returncode != 0:
        raise RuntimeError(f"clinicalbin deid failed (rc={result.returncode})")


# ── Stage 4: Diff + Merge ─────────────────────────────────────────────────────
def task_run_diff_merge(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "state_diff_merge_v3.py")
    cmd = [
        DEID_PYTHON, script,
        "--mode",        "run",
        "--tables_csv",  params["diff_merge_tables_csv"],
        "--pk_csv",      params["pk_csv"],
        "--state_dir",   params["state_dir"],
        "--max_workers", str(params.get("diff_merge_max_workers", 8)),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_ENV)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"diff+merge failed (rc={result.returncode}):\n{result.stderr or result.stdout}"
        )


# ── Stage 5: Validate Diff+Merge (trigger_rule=ALL_DONE) ─────────────────────
def task_validate_diff_merge(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "state_diff_merge_v3.py")
    cmd = [
        DEID_PYTHON, script,
        "--mode",       "summary",
        "--tables_csv", params["diff_merge_tables_csv"],
        "--state_dir",  params["state_dir"],
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_ENV)
    if result.returncode != 0:
        raise RuntimeError(
            f"diff+merge summary failed (rc={result.returncode}):\n{result.stderr or result.stdout}"
        )

    summary = json.loads(result.stdout.strip())
    logger.info("Diff+Merge summary:\n%s", json.dumps(summary, indent=4))

    diff_failed       = summary.get("diff_failed_today", 0)
    merge_failed      = summary.get("merge_failed_today", 0)
    decryption_failed = summary.get("decryption_failed_today", 0)

    if diff_failed > 0 or merge_failed > 0 or decryption_failed > 0:
        raise RuntimeError(
            f"Diff+Merge pipeline has failures — "
            f"diff: {diff_failed}, merge: {merge_failed}, decryption: {decryption_failed}"
        )
    logger.info("Diff+Merge validation passed.")


# ── Stage 6: ShortCircuit — optionally stop before deidentification ───────────
def task_check_continue(**context):
    """
    Returns False (skips all downstream tasks) when stop_after_diff_merge=True.
    Useful for running just the diff+merge part of the pipeline in isolation.
    """
    stop = context["params"].get("stop_after_diff_merge", False)
    if stop:
        logger.info("stop_after_diff_merge=True — skipping deidentification stages")
        return False
    logger.info("Continuing to deidentification stages")
    return True


# ── Stage 7a: Mapping Update (parallel) ──────────────────────────────────────
def task_mapping_update(**context):
    params = context["params"]
    script = os.path.join(_MIND_SCRIPTS_DIR, "mapping_delta.py")
    cmd = [
        DEID_PYTHON, script,
        "--mapping_schema", params["mapping_schema"],
        "--staging_schema", params["staging_schema"],
        "--state_file",     MAPPING_STATE_FILE,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_ENV)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"mapping_update failed (rc={result.returncode}):\n{result.stderr or result.stdout}")


# ── Stage 7b: PII Data Update (parallel) ─────────────────────────────────────
def task_pii_data(**context):
    params = context["params"]
    script = os.path.join(_MIND_SCRIPTS_DIR, "master_pii_delta.py")
    cmd = [
        DEID_PYTHON, script,
        "--master_schema",  params["master_schema"],
        "--staging_schema", params["staging_schema"],
        "--state_file",     PII_STATE_FILE,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_ENV)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"pii_data failed (rc={result.returncode}):\n{result.stderr or result.stdout}")


# ── Stage 7c: Master Insurance Update (parallel) ─────────────────────────────
def task_master_insurance(**context):
    params = context["params"]
    script = os.path.join(_MIND_SCRIPTS_DIR, "master_insurance_delta.py")
    cmd = [
        DEID_PYTHON, script,
        "--master_schema",  params["master_schema"],
        "--staging_schema", params["staging_schema"],
        "--state_file",     MASTER_STATE_FILE,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_ENV)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"master_insurance failed (rc={result.returncode}):\n{result.stderr or result.stdout}")


# ── Stage 8: Validate Mapping ─────────────────────────────────────────────────
def task_validate_mapping(**_):
    state_files = {
        "mapping_update":   MAPPING_STATE_FILE,
        "pii_data":         PII_STATE_FILE,
        "master_insurance": MASTER_STATE_FILE,
    }
    failed = []
    for name, path in state_files.items():
        if not os.path.exists(path):
            raise RuntimeError(f"State file missing for {name}: {path}")
        with open(path) as f:
            state = json.load(f)
        logger.info("State [%s]:\n%s", name, json.dumps(state, indent=4))
        if not state.get("success", False):
            failed.append(name)
    if failed:
        raise RuntimeError(f"Mapping pipeline failed. Components: {', '.join(failed)}")


# ── Stage 9: Init deid state ──────────────────────────────────────────────────
def task_init_deid_state(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "state_deid_init_v2.py")
    cmd = [
        DEID_PYTHON, script,
        "--tables-filepath", params["deid_tables_csv"],
        "--max-workers",     str(params.get("deid_init_max_workers", 16)),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_ENV)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"deid state init failed (rc={result.returncode}):\n{result.stderr or result.stdout}"
        )


# ── Stage 10: Run deid v2 ────────────────────────────────────────────────────
def task_run_deid(**context):
    params = context["params"]
    # state-gated runner: skips tables already at deidentification_status='success'.
    script = os.path.join(_SCRIPTS_DIR, "state_deidentification_run.py")
    cmd = [
        DEID_PYTHON, script,
        "--config", params["config_yaml"],
        "--tables", params["deid_tables_csv"],
    ]
    # No capture_output — deid run is long-lived and spawns Celery workers;
    # streaming output directly to Airflow logs is preferable to buffering.
    result = subprocess.run(cmd, env=_ENV)
    if result.returncode != 0:
        raise RuntimeError(f"deid run failed (rc={result.returncode})")


# ── Stage 11: Check deid status ───────────────────────────────────────────────
def task_check_deid_status(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "state_deid_status_check_v2.py")
    # v3 status-check uses mssql+pymssql; run under the Airflow interpreter
    # (air_env has pymssql), not DEID_PYTHON (new_deid = deid-only).
    cmd = [
        sys.executable, script,
        "--config-yaml",     params["config_yaml"],
        "--tables-filepath", params["deid_post_process_tables_csv"],
        "--max-workers",     str(params.get("deid_status_max_workers", 8)),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_ENV)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"deid status check failed (rc={result.returncode}):\n{result.stderr or result.stdout}"
        )


# ── Stage 12: Merge deid data (trigger_rule=ALL_DONE) ────────────────────────
def task_merge_deid_data(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "state_deid_merge.py")
    cmd = [
        DEID_PYTHON, script,
        "--tables_filepath", params["deid_post_process_tables_csv"],
        "--max_workers",     str(params.get("deid_merge_max_workers", 8)),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_ENV)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"deid merge failed (rc={result.returncode}):\n{result.stderr or result.stdout}"
        )


# ── Stage 13: S3 Transfer (STAGE 5 — state-gated upload → count → delete) ─────
def _s3_stage_cmd(mode, params):
    script = os.path.join(_SCRIPTS_DIR, "state_s3_transfer.py")
    return [
        DEID_PYTHON, script,
        "--mode",            mode,
        "--schema",          params.get("s3_deid_schema", "deidentified_staging"),
        "--tables-filepath", params["deid_post_process_tables_csv"],
        "--run_date",        date.today().isoformat(),
        "--max-workers",     str(params.get("parser_workers") or 4),
    ]


def _run_s3_stage(mode, context):
    result = subprocess.run(_s3_stage_cmd(mode, context["params"]),
                            check=False, cwd=_SCRIPTS_DIR, env=_ENV)
    if result.returncode != 0:
        raise RuntimeError(f"s3 {mode} failed (rc={result.returncode})")


def task_s3_upload(**context):
    _run_s3_stage("upload", context)


def task_s3_count(**context):
    _run_s3_stage("count", context)


def task_s3_delete(**context):
    _run_s3_stage("delete", context)


# ── Stage 14: Delete processed diff rows (trigger_rule=ALL_DONE) ─────────────
def task_delete_diffmerged(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "state_deid_delete.py")
    cmd = [
        DEID_PYTHON, script,
        "--tables_filepath", params["deid_post_process_tables_csv"],
        "--max_workers",     str(params.get("deid_delete_max_workers", 8)),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_ENV)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"delete_diffmerged failed (rc={result.returncode}):\n{result.stderr or result.stdout}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# DAG definition
# ══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="mind_pipeline",
    description="Full MIND pipeline: db-check → restore → decrypt → diff+merge → mapping → deid v2 → post-process",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["mind", "deid", "pipeline", "v2"],
    params={
        # Diff+Merge
        "diff_merge_tables_csv": DIFF_MERGE_TABLES_CSV,
        "pk_csv":                PK_CSV,
        "state_dir":             STATE_DIR,
        # Mapping / Master delta schemas
        "mapping_schema":  os.environ.get("MAPPING_DB_NAME", "mapping"),
        "master_schema":   os.environ.get("MASTER_DB_NAME", "master"),
        "staging_schema":  os.environ.get("STAGING_DB_NAME", ""),
        # MSSQL deid
        "config_yaml":                  _DEFAULT_CONFIG,
        "deid_tables_csv":              DEID_TABLES_CSV,
        "deid_post_process_tables_csv": DEID_POST_PROCESS_TABLES_CSV,
        # Clinicalbin deid (separate deid run for MySQL clinicalbin delta)
        "clinicalbin_deid_config": os.path.join(_CONFIGS_DIR, "mind_config_clinicalbin.yaml"),
        "clinicalbin_tables_csv":  os.environ.get("CLINICALBIN_TABLES_CSV", ""),
        # S3 Transfer
        "s3_deid_schema": os.environ.get("DEID_MERGED_NAME", "deidentified_mobiledoc_diff_merged"),
        "parser_workers":  None,
        # Control / parallelism
        "skip_restore":           False,
        "stop_after_diff_merge":  False,
        "diff_merge_max_workers": 16,
        "deid_init_max_workers":  16,
        "deid_status_max_workers": 8,
        "deid_merge_max_workers": 8,
        "deid_delete_max_workers": 8,
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

    decrypt_cnotes = PythonOperator(
        task_id="decrypt_cnotes",
        python_callable=task_decrypt_cnotes,
    )

    clinicalbin_deid = PythonOperator(
        task_id="clinicalbin_deid",
        python_callable=task_clinicalbin_deid,
        execution_timeout=None,
    )

    # ── MSSQL diff+merge branch ───────────────────────────────────────────────
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

    # Mapping jobs run in parallel (like DENT's [mapping_delta, master_pii_delta, master_insurance_delta])
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

    validate_mapping = PythonOperator(
        task_id="validate_mapping",
        python_callable=task_validate_mapping,
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

    # Post-deid runs sequentially: merge → delete original → STAGE 5 (s3 upload → count → delete)
    merge_deid_data = PythonOperator(
        task_id="merge_deid_data",
        python_callable=task_merge_deid_data,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    s3_upload = PythonOperator(
        task_id="s3_upload",
        python_callable=task_s3_upload,
    )

    s3_count = PythonOperator(
        task_id="s3_count",
        python_callable=task_s3_count,
    )

    s3_delete = PythonOperator(
        task_id="s3_delete",
        python_callable=task_s3_delete,
    )

    delete_original_diffmerged = PythonOperator(
        task_id="delete_original_diffmerged",
        python_callable=task_delete_diffmerged,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Pipeline ───────────────────────────────────────────────────────────────
    # Stage 1-2: linear start
    # Verify the DBs are free before restore flips them into RESTORING/standby mode,
    # then apply the logs dumped by the separate daily-dump DAG.
    check_db_availability >> restore >> decrypt_cnotes

    # diff+merge starts in parallel with clinicalbin deid setup after decrypt+staging
    decrypt_cnotes >> run_diff_merge

    # Branch B — MSSQL: diff+merge → validate → checkpoint → parallel mapping/master update
    run_diff_merge >> validate_diff_merge >> check_continue >> [mapping_update, pii_data, master_insurance]
    [mapping_update, pii_data, master_insurance] >> validate_mapping

    # clinicalbin deid starts once decrypt+staging AND mapping/master tables are ready
    [decrypt_cnotes, validate_mapping] >> clinicalbin_deid

    # validate mapping → init deid state → run deid → status check
    validate_mapping >> init_deid_state >> run_deid >> check_deid_status

    # Both deid branches must fully complete, then strictly sequential:
    # merge → trim original diff-merged → ship de-identified merged accumulator to S3.
    [clinicalbin_deid, check_deid_status] >> merge_deid_data >> delete_original_diffmerged >> s3_upload >> s3_count >> s3_delete
