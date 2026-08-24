from __future__ import annotations

"""
deid_base_run_and_purge
=======================

Trigger-only DAG that, for a given deid v2 config + tables CSV:

  1. run_deid           — runs deidentification to completion (deid run --rerun,
                          blocking) via scripts/manual_deidentification_run_v2.py.
  2. row_count_check    — verifies each table with the same strategy as
                          state_deid_status_check_v2.py:
                              source == destination + deid_failed
                          (connections built from the passed config.yaml; the
                          failed-rows count uses the engine's own
                          get_schema_table_name). Logs the names of any tables that
                          fail the check and fails the task on any mismatch — but
                          first publishes the passing tables via XCom so the drop
                          step can still purge them.
  3. drop_merged_tables — DROP TABLE (per table) from MERGED_DB_NAME (MSSQL) and
                          DEID_MERGED_NAME (MySQL). Connections are built from .env,
                          mirroring db_config.py's cred→DB mapping. Runs even if the
                          check failed (TriggerRule.ALL_DONE) but drops ONLY the tables
                          that passed the row-count check (from XCom) — the failed
                          tables are left intact for inspection.
                          Skipped unless the delete_merged_tables param is ticked.

No per-table JSON state is written or read; the only state store touched is the
failed_rows table, and only to read a count.

Inputs (params, defaulted from .env, overridable at trigger time):
  - config_yaml           : DEID_BASE_PURGE_CONFIG
  - tables_csv            : DEID_BASE_PURGE_TABLES_CSV
  - skip_deid             : bool, default False — tick in the trigger-DAG UI to
                            skip run_deid and go straight to row_count_check
                            (useful for re-running just the check + purge)
  - delete_merged_tables  : bool, default False — tick in the trigger-DAG UI to
                            actually drop the merged tables
"""

import csv
import logging
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import yaml

import pendulum

import sqlalchemy as sa
from sqlalchemy.engine import URL

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

# Add dags/ directory to sys.path so the shared package is importable.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from shared.notifications import default_args  # noqa: E402 — must follow sys.path insert
from shared.tasks import _SCRIPTS_DIR, _env    # noqa: E402

logger = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Kolkata")


# ──────────────────────────────────────────────────────────────────────────────
# Config / CSV helpers — the row-count side mirrors state_deid_status_check_v2.py
# so the verification strategy matches exactly.
# ──────────────────────────────────────────────────────────────────────────────
_ENV_RE = re.compile(r"\$\{(\w+)\}")


def _interp(obj):
    """Recursively replace ${VAR} with os.environ[VAR]."""
    if isinstance(obj, str):
        def _sub(m):
            var = m.group(1)
            val = os.environ.get(var)
            if val is None:
                raise ValueError(f"Environment variable '{var}' not set (referenced in config)")
            return val
        return _ENV_RE.sub(_sub, obj)
    if isinstance(obj, dict):
        return {k: _interp(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interp(v) for v in obj]
    return obj


def _load_cfg(path: str) -> dict:
    with open(path) as f:
        return _interp(yaml.safe_load(f))


def _conn_str(db: dict) -> str:
    """Build a SQLAlchemy URL from a config db block (== state_deid_status_check_v2._conn_str).
    URL.create percent-encodes special characters in the password (e.g. '@')."""
    drivers = {
        "mysql":      "mysql+pymysql",
        "mssql":      "mssql+pymssql",
        "postgresql": "postgresql+psycopg2",
    }
    return URL.create(
        drivers[db["type"]],
        username=db["username"],
        password=db["password"],
        host=db["host"],
        port=int(db["port"]),
        database=db["database"],
    ).render_as_string(hide_password=False)


def _resolve_db_url(cfg: dict, cfg_path: str, name_key: str, path_key: str, default_path: str) -> str:
    """Resolve a *_db_name (MySQL on destination_db's server) / *_db_path (SQLite) pair
    the same way the deid engine / state_deid_status_check_v2 does."""
    name = cfg.get(name_key)
    if name:
        sibling = dict(cfg["destination_db"])
        sibling["database"] = name
        return _conn_str(sibling)
    raw_path = cfg.get(path_key, default_path)
    base = Path(cfg_path).parent.resolve()
    resolved = Path(raw_path) if Path(raw_path).is_absolute() else (base / raw_path).resolve()
    return f"sqlite:///{resolved}"


def _tables(csv_path: str) -> list[str]:
    with open(csv_path, newline="") as f:
        return [
            row[0].strip()
            for row in csv.reader(f)
            if row and row[0].strip() and not row[0].strip().startswith("#")
        ]


def _engine(url: str):
    kwargs = {"pool_pre_ping": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return sa.create_engine(url, **kwargs)


def _count(engine, table: str) -> int:
    """COUNT(*) for a table; a MISSING table counts as 0 (== get_row_count)."""
    q = engine.dialect.identifier_preparer.quote(table)
    try:
        with engine.connect() as conn:
            return conn.execute(sa.text(f"SELECT COUNT(*) FROM {q}")).scalar() or 0
    except sa.exc.DBAPIError as e:
        if any(s in str(e).lower() for s in ("doesn't exist", "1146", "invalid object name", "42s02")):
            return 0
        raise


def _failed_count(engine, failed_table: str, table: str, config_key: str) -> int:
    """Rows the engine failed to deidentify for this table/config_key (== get_failed_count).
    A missing failed_rows table → 0."""
    q = engine.dialect.identifier_preparer.quote(failed_table)
    try:
        with engine.connect() as conn:
            return conn.execute(
                sa.text(f"SELECT COUNT(*) FROM {q} WHERE table_name = :t AND config_key = :ck"),
                {"t": table, "ck": config_key},
            ).scalar() or 0
    except sa.exc.DBAPIError as e:
        logger.warning("could not query %s (treating as 0 failed rows): %s", failed_table, e)
        return 0


# ──────────────────────────────────────────────────────────────────────────────
# Tasks
# ──────────────────────────────────────────────────────────────────────────────
def task_run_deid(**context):
    """Run deidentification to completion via manual_deidentification_run_v2.py
    (which invokes `deid run --rerun`). Blocks; streams output to the Airflow log.

    Skipped entirely when the skip_deid param is ticked, so the DAG can be
    re-triggered to run only row_count_check + drop_merged_tables."""
    p = context["params"]
    if p.get("skip_deid", False):
        raise AirflowSkipException("skip_deid is True; skipping run_deid")

    config_yaml = p["config_yaml"]
    tables_csv = p["tables_csv"]
    if not config_yaml or not os.path.exists(config_yaml):
        raise RuntimeError(f"config_yaml not found: {config_yaml!r}")
    if not tables_csv or not os.path.exists(tables_csv):
        raise RuntimeError(f"tables_csv not found: {tables_csv!r}")

    script = os.path.join(_SCRIPTS_DIR, "manual_deidentification_run_v2.py")
    cmd = [sys.executable, script, "--config", config_yaml, "--tables", tables_csv]
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, env=_env())
    if result.returncode != 0:
        raise RuntimeError(f"deid run failed (rc={result.returncode})")


def task_row_count_check(**context):
    """Verify each table: source == destination + deid_failed (== state_deid_status_check_v2).

    Publishes the passing tables via XCom (key='passing_tables') so drop_merged_tables
    can purge them even if this task fails, then logs the failed table names and raises
    if any table's counts don't reconcile."""
    p = context["params"]
    config_yaml = p["config_yaml"]
    max_workers = int(p.get("max_workers", 8))

    # Import the engine's exact failed-table naming (it sanitizes the source db name).
    repo = os.path.abspath(os.environ["DEID_REPO_DIR"])
    if repo not in sys.path:
        sys.path.insert(0, repo)
    from deid.models.failed_rows import get_schema_table_name  # noqa: E402

    cfg = _load_cfg(config_yaml)
    config_key = cfg.get("config_key", "default")
    source_db_name = cfg["source_db"]["database"]

    se = _engine(_conn_str(cfg["source_db"]))
    de = _engine(_conn_str(cfg["destination_db"]))
    fe = _engine(_resolve_db_url(cfg, config_yaml, "failed_rows_db_name", "failed_rows_db_path", "./failed_rows.db"))
    failed_table = get_schema_table_name(source_db_name)

    tables = _tables(p["tables_csv"])
    logger.info("Verifying row counts for %d tables (config_key=%s)", len(tables), config_key)

    def _check(table):
        s = _count(se, table)
        d = _count(de, table)
        f = _failed_count(fe, failed_table, table, config_key)
        logger.info("[%s] source=%d dest=%d failed=%d", table, s, d, f)
        return (table, s, d, f, s == d + f)

    passes, mismatches = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for table, s, d, f, ok in ex.map(_check, tables):
            if ok:
                passes.append(table)
            else:
                mismatches.append((table, s, d, f))

    # Publish the passing tables BEFORE any raise, so drop_merged_tables (ALL_DONE)
    # can purge exactly these — the failed ones are excluded and left for inspection.
    context["ti"].xcom_push(key="passing_tables", value=passes)
    logger.info("Row-count check: %d passed, %d failed", len(passes), len(mismatches))

    if mismatches:
        failed_names = [t for t, *_ in mismatches]
        logger.error(
            "Row-count check FAILED for %d table(s): %s",
            len(failed_names), ", ".join(failed_names),
        )
        detail = ", ".join(f"{t}(src={s},dst={d},failed={f})" for t, s, d, f in mismatches)
        raise RuntimeError(f"row count mismatch (source != dest + failed): {detail}")

    logger.info("Row-count check passed for all %d tables", len(tables))


def task_drop_merged_tables(**context):
    """DROP TABLE per table from MERGED_DB_NAME (MSSQL) and DEID_MERGED_NAME (MySQL).
    Connections built from .env, mirroring db_config.py's cred→DB mapping:
      MERGED_DB_NAME (MSSQL)     ← SOURCE_DB_* creds
      DEID_MERGED_NAME (MySQL)   ← DB_* creds

    Drops ONLY the tables that passed the row-count check (published by
    row_count_check via XCom); tables that failed the check are left intact.
    """
    p = context["params"]
    if not p.get("delete_merged_tables", False):
        raise AirflowSkipException("delete_merged_tables is False; skipping drop of merged tables")

    tables = context["ti"].xcom_pull(task_ids="row_count_check", key="passing_tables")
    if not tables:
        raise AirflowSkipException(
            "no tables passed the row-count check (XCom empty/missing); nothing to drop"
        )

    src_drivers = {
        "mssql":      "mssql+pymssql",
        "mysql":      "mysql+pymysql",
        "postgresql": "postgresql+psycopg2",
    }
    merged_db = os.environ["MERGED_DB_NAME"]
    deid_merged_db = os.environ["DEID_MERGED_NAME"]

    mssql_url = URL.create(
        src_drivers[os.environ.get("SOURCE_DB_TYPE", "mssql")],
        username=os.environ["SOURCE_DB_USER"],
        password=os.environ["SOURCE_DB_PASS"],
        host=os.environ.get("SOURCE_DB_HOST") or os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("SOURCE_DB_PORT", "1433")),
        database=merged_db,
    ).render_as_string(hide_password=False)

    mysql_url = URL.create(
        "mysql+pymysql",
        username=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "3306")),
        database=deid_merged_db,
    ).render_as_string(hide_password=False)

    me = _engine(mssql_url)   # MSSQL mobiledoc_diff_merged
    dm = _engine(mysql_url)   # MySQL deidentified_mobiledoc_diff_merged

    logger.info(
        "Dropping %d passing table(s) from %s (MSSQL) and %s (MySQL)",
        len(tables), merged_db, deid_merged_db,
    )
    for table in tables:
        for eng, dbname in ((me, merged_db), (dm, deid_merged_db)):
            q = eng.dialect.identifier_preparer.quote(table)
            with eng.begin() as conn:
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {q}"))
            logger.info("[%s] dropped from %s", table, dbname)


# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="deid_base_run_and_purge",
    description="Run deid to completion, verify row counts (source == dest + failed), "
                "then DROP the merged tables from MERGED_DB_NAME + DEID_MERGED_NAME (fresh).",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["deid", "base", "purge"],
    params={
        "config_yaml": os.environ.get("DEID_BASE_PURGE_CONFIG", ""),
        "tables_csv":  os.environ.get("DEID_BASE_PURGE_TABLES_CSV", ""),
        "max_workers": 8,
        "skip_deid": Param(
            False, type="boolean",
            description="Tick to skip run_deid and go straight to row_count_check "
                        "(and drop_merged_tables, if also ticked). Default False = run deid.",
        ),
        "delete_merged_tables": Param(
            False, type="boolean",
            description="Tick to DROP the merged tables (MERGED_DB_NAME + DEID_MERGED_NAME) after a passing row-count check. Default False = skip deletion.",
        ),
    },
) as dag:

    run_deid = PythonOperator(
        task_id="run_deid",
        python_callable=task_run_deid,
        execution_timeout=None,  # long-running; deid run blocks until all tables complete
    )

    row_count_check = PythonOperator(
        task_id="row_count_check",
        python_callable=task_row_count_check,
        # Must still run when run_deid was skipped (skip_deid=True); NONE_FAILED
        # allows a skipped upstream through but still blocks on an actual failure.
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    drop_merged_tables = PythonOperator(
        task_id="drop_merged_tables",
        python_callable=task_drop_merged_tables,
        # Run even if row_count_check failed on some tables; the task itself drops
        # only the passing tables (from XCom), leaving the failed ones intact.
        trigger_rule=TriggerRule.ALL_DONE,
    )

    run_deid >> row_count_check >> drop_merged_tables
