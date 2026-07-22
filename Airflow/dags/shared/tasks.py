"""
shared.tasks — Task callables shared across all pipeline DAGs (MIND, eCW, …).

DEID_PYTHON and the subprocess environment are resolved lazily at call time so
they always reflect the live os.environ (populated by shared/__init__.py before
this module is first imported).

State-file paths (mapping_update, pii_data, master_insurance) are computed from
STATE_DIR at import time.  Since shared/__init__.py loads the .env before Python
executes this module body, the values will be correct.

Scripts that are EHR-specific (daily_dump, restore, mapping_delta, …) are
injected via factory functions so each DAG controls its own scripts_dir.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import date, datetime

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Paths — resolved relative to this file's location
# dags/shared/tasks.py → dags/shared/ → dags/ → Airflow/ → scripts/
# ──────────────────────────────────────────────────────────────────────────────
_AIRFLOW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_SCRIPTS_DIR = os.path.join(_AIRFLOW_DIR, "scripts")


# ──────────────────────────────────────────────────────────────────────────────
# Lazy env helpers — called at task execution time so values are always current
# ──────────────────────────────────────────────────────────────────────────────
def _deid_python() -> str:
    return os.environ.get("DEID_PYTHON", "/opt/miniconda3/envs/air_deid/bin/python")


def _env() -> dict:
    return {**os.environ, "PYTHONUNBUFFERED": "1"}


def _run_streaming(cmd, *, env: dict | None = None, cwd: str | None = None) -> tuple[int, str]:
    """Run *cmd*, streaming its merged stdout+stderr to the Airflow task log
    line by line as the child produces it (realtime), while also collecting the
    full output.  Returns ``(returncode, output_text)``.

    stderr is folded into stdout so the log ordering is chronological.  Do NOT
    use this for commands whose stdout must be parsed separately from stderr
    (e.g. JSON emitted on stdout) — use ``subprocess.run(capture_output=True)``
    there so the streams stay distinct.

    *cwd* sets the child's working directory (default: inherited) — needed by
    scripts that read config-relative paths, e.g. the AthenaOne v5 mapping/master
    scripts whose YAML references sql/ paths relative to scripts/athenaone/.
    """
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,   # fold stderr in → chronological ordering
        text=True,
        bufsize=1,                  # line-buffered
        env=env if env is not None else _env(),
        cwd=cwd,
    )
    lines: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:        # drains the pipe as the child writes → realtime
        line = line.rstrip("\n")
        lines.append(line)
        logger.info(line)
    rc = proc.wait()
    return rc, "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# State-file paths (constant per machine, derived from STATE_DIR in .env)
# ──────────────────────────────────────────────────────────────────────────────
_STATE_DIR               = os.environ.get("STATE_DIR", "")
_MAPPING_MASTER_DIR      = os.path.join(_STATE_DIR, "mapping_master")
_MAPPING_STATE_FILE      = os.path.join(_MAPPING_MASTER_DIR, "mapping_update_state.json")
_PII_STATE_FILE          = os.path.join(_MAPPING_MASTER_DIR, "pii_data_state.json")
_MASTER_STATE_FILE       = os.path.join(_MAPPING_MASTER_DIR, "master_insurance_state.json")


# ══════════════════════════════════════════════════════════════════════════════
# Direct shared task callables
# ══════════════════════════════════════════════════════════════════════════════

def task_run_diff_merge(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "state_diff_merge_v3.py")
    cmd = [
        _deid_python(), script,
        "--mode",        "run",
        "--tables_csv",  params["diff_merge_tables_csv"],
        "--pk_csv",      params["pk_csv"],
        "--state_dir",   params["state_dir"],
        "--max_workers", str(params.get("diff_merge_max_workers", 8)),
    ]
    rc, out = _run_streaming(cmd)
    if rc != 0:
        raise RuntimeError(f"diff+merge failed (rc={rc}):\n{out[-4000:]}")


def task_validate_diff_merge(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "state_diff_merge_v3.py")
    cmd = [
        _deid_python(), script,
        "--mode",       "summary",
        "--tables_csv", params["diff_merge_tables_csv"],
        "--state_dir",  params["state_dir"],
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_env())
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


def task_check_continue(**context):
    """Returns False (skips downstream) when stop_after_diff_merge=True."""
    stop = context["params"].get("stop_after_diff_merge", False)
    if stop:
        logger.info("stop_after_diff_merge=True — skipping deidentification stages")
        return False
    logger.info("Continuing to deidentification stages")
    return True


def task_validate_mapping(**_):
    state_files = {
        "mapping_update":   _MAPPING_STATE_FILE,
        "pii_data":         _PII_STATE_FILE,
        "master_insurance": _MASTER_STATE_FILE,
    }
    today = date.today().isoformat()
    failed = []
    for name, path in state_files.items():
        if not os.path.exists(path):
            raise RuntimeError(f"State file missing for {name}: {path}")
        with open(path) as f:
            state = json.load(f)
        logger.info("State [%s]:\n%s", name, json.dumps(state, indent=4))

        if not state.get("success", False):
            failed.append(f"{name} (success=false)")
            continue

        # A state file is written only on success and is never deleted, so a
        # stale success from a previous run could mask a failure/no-run today.
        # Require completed_at to be the current day (converted to local time,
        # matching date.today() used elsewhere in the pipeline).
        completed_at = state.get("completed_at", "")
        try:
            run_date = datetime.fromisoformat(completed_at).astimezone().date().isoformat()
        except ValueError:
            failed.append(f"{name} (invalid/missing completed_at: {completed_at!r})")
            continue
        if run_date != today:
            failed.append(f"{name} (stale: last success {run_date}, not today {today})")

    if failed:
        raise RuntimeError(f"Mapping pipeline failed or stale. Components: {', '.join(failed)}")


def task_init_deid_state(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "state_deid_init_v2.py")
    cmd = [
        _deid_python(), script,
        "--tables-filepath", params["deid_tables_csv"],
        "--max-workers",     str(params.get("deid_init_max_workers", 16)),
    ]
    rc, out = _run_streaming(cmd)
    if rc != 0:
        raise RuntimeError(f"deid state init failed (rc={rc}):\n{out[-4000:]}")


def task_run_deid(**context):
    params = context["params"]
    # Route through the manual_deidentification_run_v2.py wrapper rather than
    # calling `deid run` directly, so the deid CLI surface is invoked from a
    # single place (--rerun is the wrapper's default).
    script = os.path.join(_SCRIPTS_DIR, "manual_deidentification_run_v2.py")
    cmd = [
        _deid_python(), script,
        "--config", params["config_yaml"],
        "--tables", params["deid_tables_csv"],
    ]
    # No capture_output — long-lived process; stream directly to Airflow logs.
    result = subprocess.run(cmd, env=_env())
    if result.returncode != 0:
        raise RuntimeError(f"deid run failed (rc={result.returncode})")


def task_check_deid_status(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "state_deid_status_check_v2.py")
    cmd = [
        _deid_python(), script,
        "--config-yaml",     params["config_yaml"],
        "--tables-filepath", params["deid_post_process_tables_csv"],
        "--max-workers",     str(params.get("deid_status_max_workers", 8)),
    ]
    rc, out = _run_streaming(cmd)
    if rc != 0:
        raise RuntimeError(f"deid status check failed (rc={rc}):\n{out[-4000:]}")


def task_auto_qc(**context):
    """Run the deid Auto-QC framework over a table list and emit two CSVs.

    Shells out to ``python -m deid auto-qc`` (in DEID_PYTHON's env) which runs Part 1 (structured/
    unstructured scan), Part 2 (mapping/count gate + delta-identity) and Part 3 (master PHI audit)
    per table, writing ``auto_qc_summary_<ts>.csv`` + ``auto_qc_findings_<ts>.csv`` to ``out_dir``.
    ``auto_qc_max_workers`` QCs that many tables concurrently. The CLI exits non-zero if any table
    fails/errors, so this task fails and the failure notification fires.

    Like the deid runner, DEID_REPO_DIR is prepended to PYTHONPATH so ``deid`` resolves to THIS
    checkout (which has the ``auto-qc`` command) rather than whatever is pip-installed.
    """
    params = context["params"]
    cmd = [
        _deid_python(), "-m", "deid", "auto-qc",
        "--config",                params["config_yaml"],
        "--rules-csv",             params["auto_qc_rules_csv"],
        "--out-dir",               params["auto_qc_out_dir"],
        "--max-workers",           str(params.get("auto_qc_max_workers", 4)),
        "--residual-pii-backend",  params.get("auto_qc_residual_backend", "regex"),
        "--stamp",
    ]
    if params.get("auto_qc_tables_filepath"):
        cmd += ["--tables-filepath", params["auto_qc_tables_filepath"]]
    if params.get("auto_qc_pii_master_conn_str"):
        cmd += ["--pii-master-conn-str", params["auto_qc_pii_master_conn_str"]]
    if params.get("auto_qc_delta_after"):
        cmd += ["--delta-after", params["auto_qc_delta_after"]]
    if params.get("auto_qc_no_gate"):
        cmd += ["--no-gate"]

    env = _env()
    repo = os.environ.get("DEID_REPO_DIR")
    if repo:
        repo = os.path.abspath(repo)
        env["PYTHONPATH"] = repo + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")

    rc, out = _run_streaming(cmd, env=env)
    if rc != 0:
        raise RuntimeError(f"auto-qc failed (rc={rc}):\n{out[-4000:]}")


def task_merge_deid_data(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "state_deid_merge.py")
    cmd = [
        _deid_python(), script,
        "--tables_filepath", params["deid_post_process_tables_csv"],
        "--max_workers",     str(params.get("deid_merge_max_workers", 8)),
    ]
    rc, out = _run_streaming(cmd)
    if rc != 0:
        raise RuntimeError(f"deid merge failed (rc={rc}):\n{out[-4000:]}")


def task_s3_transfer(**context):
    params   = context["params"]
    run_date = date.today().isoformat()

    transfer_schema = params.get("s3_deid_schema", "deidentified_staging")
    max_workers     = params.get("parser_workers", None)

    script = os.path.join(_SCRIPTS_DIR, "s3_transfer.py")
    try:
        date_folder = datetime.strptime(run_date, "%Y-%m-%d").strftime("%m%d%Y")
    except ValueError:
        raise RuntimeError(f"Invalid run_date format: {run_date}. Expected YYYY-MM-DD")
    # Base S3 folder is maintained centrally in .env (differs per EHR); the
    # per-run date folder is appended here.
    dest_base = os.environ.get("S3_DESTINATION_PREFIX", "EHR").strip("/")
    destination_prefix = f"{dest_base}/{date_folder}"

    cmd = [
        _deid_python(), script,
        "--schema",             transfer_schema,
        "--run_date",           run_date,
        "--destination_prefix", destination_prefix,
    ]
    if max_workers:
        cmd.extend(["--max_workers", str(max_workers)])
        logger.info("S3 Transfer will use %s workers", max_workers)

    logger.info("Running S3 Transfer: schema=%s, run_date=%s", transfer_schema, run_date)
    logger.debug("Command: %s", " ".join(cmd))

    result = subprocess.run(cmd, check=False, cwd=os.path.dirname(script), env=_env())
    if result.returncode != 0:
        raise RuntimeError(f"s3_transfer failed (rc={result.returncode})")


def task_delete_diffmerged(**context):
    params = context["params"]
    script = os.path.join(_SCRIPTS_DIR, "state_deid_delete.py")
    cmd = [
        _deid_python(), script,
        "--tables_filepath", params["deid_post_process_tables_csv"],
        "--max_workers",     str(params.get("deid_delete_max_workers", 8)),
    ]
    rc, out = _run_streaming(cmd)
    if rc != 0:
        raise RuntimeError(f"delete_diffmerged failed (rc={rc}):\n{out[-4000:]}")


# ══════════════════════════════════════════════════════════════════════════════
# Factory functions — return EHR-specific variants of tasks that only differ
# in which scripts_dir they execute scripts from.
# ══════════════════════════════════════════════════════════════════════════════

def task_daily_dump(**context):
    """Run the unified, config-driven daily dump (scripts/daily_dump.py).

    A single script now covers every source (shared SMB folder, Azure Blob,
    local folder check); behaviour is selected entirely by DUMP_* env vars, so
    the same DAG + script runs on every machine unchanged.  The dump only reads
    the source and writes backup files — it never touches the databases — so it
    is safe to run on a daily schedule independently of the pipeline.
    """
    if context["params"].get("skip_dump", False):
        logger.info("skip_dump=True — skipping daily dump")
        return

    script = os.path.join(_SCRIPTS_DIR, "daily_dump.py")
    rc, out = _run_streaming([_deid_python(), script])
    if rc != 0:
        raise RuntimeError(f"daily_dump failed (rc={rc}):\n{out[-4000:]}")


def make_daily_dump(scripts_dir: str = ""):
    """Back-compat factory — returns the unified :func:`task_daily_dump`.

    The *scripts_dir* argument is ignored now that a single shared
    scripts/daily_dump.py serves every EHR (kept only so any lingering caller
    keeps working).
    """
    return task_daily_dump


def task_check_unzip_required(**_) -> bool:
    """ShortCircuit gate: run the unzip step only when DUMP_UNZIP_ENABLED is set.

    Returns True (continue to the unzip task) when the machine receives zipped
    dumps, else False (ShortCircuitOperator skips the downstream unzip task).
    """
    enabled = os.environ.get("DUMP_UNZIP_ENABLED", "").strip().lower() in ("1", "true", "yes", "on")
    if enabled:
        logger.info("DUMP_UNZIP_ENABLED — running the unzip step")
    else:
        logger.info("DUMP_UNZIP_ENABLED not set — skipping the unzip step")
    return enabled


def task_unzip_dump(**context):
    """Extract archives dumped by the dump step (scripts/daily_dump.py --task unzip).

    Config-driven via DUMP_UNZIP_* env vars.  Gated upstream by
    :func:`task_check_unzip_required`, and skipped entirely when skip_dump=True.
    """
    if context["params"].get("skip_dump", False):
        logger.info("skip_dump=True — skipping unzip")
        return

    script = os.path.join(_SCRIPTS_DIR, "daily_dump.py")
    rc, out = _run_streaming([_deid_python(), script, "--task", "unzip"])
    if rc != 0:
        raise RuntimeError(f"unzip failed (rc={rc}):\n{out[-4000:]}")


def task_check_db_availability(**context):
    """Guard the restore step (shared scripts/check_db_availability.py).

    Fails if any target database is not ONLINE (e.g. a restore is already in
    progress) or has active user sessions, so the restore never disrupts a
    deid / diff-merge job already running against the DB.  The DB list and
    connection settings come from the env (RESTORE_DB_CONFIGS), so one shared
    script serves every MSSQL EHR.
    """
    if context["params"].get("skip_restore", False):
        logger.info("skip_restore=True — skipping DB availability check")
        return

    script = os.path.join(_SCRIPTS_DIR, "check_db_availability.py")
    rc, out = _run_streaming([_deid_python(), script])
    if rc != 0:
        raise RuntimeError(f"db availability check failed (rc={rc}):\n{out[-4000:]}")


def task_restore(**context):
    """Apply transaction-log backups (shared scripts/restore_incremental_logs.py).

    Applies the .trn logs dumped by the daily-dump DAG to the databases listed in
    RESTORE_DB_CONFIGS.  Gated by skip_restore so practices that source their data
    differently can opt out.
    """
    if context["params"].get("skip_restore", False):
        logger.info("skip_restore=True — skipping restore")
        return

    script = os.path.join(_SCRIPTS_DIR, "restore_incremental_logs.py")
    rc, out = _run_streaming([_deid_python(), script])
    if rc != 0:
        raise RuntimeError(f"restore failed (rc={rc}):\n{out[-4000:]}")


def make_check_db_availability(scripts_dir: str = ""):
    """Back-compat factory — returns the shared :func:`task_check_db_availability`.

    *scripts_dir* is ignored now that a single scripts/check_db_availability.py
    serves every EHR.
    """
    return task_check_db_availability


def make_restore(scripts_dir: str = ""):
    """Back-compat factory — returns the shared :func:`task_restore`.

    *scripts_dir* is ignored now that a single scripts/restore_incremental_logs.py
    serves every EHR.
    """
    return task_restore

    return task_restore


def make_mapping_update(scripts_dir: str):
    """Return a task_mapping_update callable that runs mapping_delta.py from scripts_dir."""
    def task_mapping_update(**context):
        params = context["params"]
        script = os.path.join(scripts_dir, "mapping_delta.py")
        cmd = [
            _deid_python(), script,
            "--mapping_schema", params["mapping_schema"],
            "--staging_schema", params["staging_schema"],
            "--state_file",     _MAPPING_STATE_FILE,
        ]
        rc, out = _run_streaming(cmd)
        if rc != 0:
            raise RuntimeError(f"mapping_update failed (rc={rc}):\n{out[-4000:]}")

    return task_mapping_update


def make_mapping_update_v5(config_file: str):
    """Return a task_mapping_update callable that runs the v5 mapping script in
    pipeline mode (config-driven, env connections).

    The v5 script (scripts/incremental_mapping_update_v5.py) replaces the
    per-EHR mapping_delta.py. It reads its structural config from *config_file*
    and, given --staging_schema/--mapping_schema, builds the source connection
    from SOURCE_DB_* (staging/delta) and the dest from DB_* (MySQL mapping),
    then writes --state_file in the format task_validate_mapping expects.
    Run with cwd=scripts/ so the config's relative sql/<ehr>/*.sql paths resolve.
    """
    def task_mapping_update(**context):
        params = context["params"]
        script = os.path.join(_SCRIPTS_DIR, "incremental_mapping_update_v5.py")
        cmd = [
            _deid_python(), "-u", script,
            "--config",         config_file,
            "--staging_schema", params["staging_schema"],
            "--mapping_schema", params["mapping_schema"],
            "--state_file",     _MAPPING_STATE_FILE,
        ]
        rc, out = _run_streaming(cmd, cwd=_SCRIPTS_DIR)
        if rc != 0:
            raise RuntimeError(f"mapping_update (v5) failed (rc={rc}):\n{out[-4000:]}")

    return task_mapping_update


def make_pii_data(scripts_dir: str):
    """Return a task_pii_data callable that runs master_pii_delta.py from scripts_dir."""
    def task_pii_data(**context):
        params = context["params"]
        script = os.path.join(scripts_dir, "master_pii_delta.py")
        cmd = [
            _deid_python(), script,
            "--master_schema",  params["master_schema"],
            "--staging_schema", params["staging_schema"],
            "--mapping_schema", params["mapping_schema"],
            "--state_file",     _PII_STATE_FILE,
        ]
        rc, out = _run_streaming(cmd)
        if rc != 0:
            raise RuntimeError(f"pii_data failed (rc={rc}):\n{out[-4000:]}")

    return task_pii_data


def make_master_insurance(scripts_dir: str):
    """Return a task_master_insurance callable that runs master_insurance_delta.py from scripts_dir."""
    def task_master_insurance(**context):
        params = context["params"]
        script = os.path.join(scripts_dir, "master_insurance_delta.py")
        cmd = [
            _deid_python(), script,
            "--master_schema",  params["master_schema"],
            "--staging_schema", params["staging_schema"],
            "--mapping_schema", params["mapping_schema"],
            "--state_file",     _MASTER_STATE_FILE,
        ]
        rc, out = _run_streaming(cmd)
        if rc != 0:
            raise RuntimeError(f"master_insurance failed (rc={rc}):\n{out[-4000:]}")

    return task_master_insurance
