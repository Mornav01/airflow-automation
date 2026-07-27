#!/usr/bin/env python
"""
DAG 3 (v3) — Task 1: Deidentification Status Check

Replaces state_deid_status_check.py for the deid v2 system.
No Django dependency — reads state directly from state.db (SQLite).

For each table in the CSV:
  1. Skip if is_merged = True
  2. Skip if deidentification_status = 'success'
  3. Read table_state.status from state.db
       'completed'   → proceed to row-count check
       'failed'      → leave deidentification_status='started' (retry next run)
       anything else → still in progress, no change
  4. If completed → verify source_count == dest_count + deid_failed_count
       (strict equality, no tolerance). deid_failed_count comes from the
       failed_rows_{source_db} table in the failed-rows DB. A MISSING source/dest
       table counts as 0 rows (a zero-row source is never materialised by deid).
  5. On match → deidentification_status = 'success'. On mismatch → leave it
       'started' (no terminal 'failed' state; it retries on the next run).

Config is read entirely from the provided config.yaml — including config_key,
state_db_path (SQLite) or state_db_url (MySQL), and source/destination connection strings.
If state_db_url is present in the config it takes precedence over state_db_path.
The failed-rows DB is resolved the same way: failed_rows_db_url (MySQL) takes
precedence over failed_rows_db_path (SQLite).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import sqlalchemy as sa


@lru_cache(maxsize=16)
def _get_engine(conn_str: str):
    """Reused, thread-safe SQLAlchemy engine per distinct conn-string. Bounds the
    number of connection pools when tables are processed concurrently."""
    kwargs = {"pool_pre_ping": True}
    if conn_str.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return sa.create_engine(conn_str, **kwargs)
import yaml

# ============================================================
# ===================== CONFIG ===============================
# ============================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from db_config import STATE_DIR  # noqa: E402  # type: ignore[import-untyped]

# ============================================================
# ===================== YAML / CONFIG ========================
# ============================================================

_ENV_PATTERN = re.compile(r"\$\{(\w+)\}")


def _interpolate(obj):
    """Recursively replace ${VAR_NAME} with os.environ[VAR_NAME]."""
    if isinstance(obj, str):
        def _replace(match):
            var = match.group(1)
            val = os.environ.get(var)
            if val is None:
                raise ValueError(f"Environment variable '{var}' not set (referenced in config)")
            return val
        return _ENV_PATTERN.sub(_replace, obj)
    if isinstance(obj, dict):
        return {k: _interpolate(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate(i) for i in obj]
    return obj


def _conn_str(db_cfg: dict) -> str:
    drivers = {
        "mysql":      "mysql+pymysql",
        "mssql":      "mssql+pymssql",
        "postgresql": "postgresql+psycopg2",
    }
    driver = drivers[db_cfg["type"]]
    u = db_cfg["username"]
    p = db_cfg["password"]
    h = db_cfg["host"]
    port = db_cfg["port"]
    db = db_cfg["database"]
    return f"{driver}://{u}:{p}@{h}:{port}/{db}"


def load_deid_config(config_yaml_path: str) -> dict:
    """Load and interpolate deid v2 config.yaml, return as plain dict."""
    with open(config_yaml_path) as f:
        raw = yaml.safe_load(f)
    return _interpolate(raw)


def _resolve_db_url(
    cfg: dict, config_yaml_path: str, url_key: str, path_key: str, default_path: str
) -> str:
    """
    Resolve a <url_key> (MySQL, preferred) / <path_key> (SQLite) config pair
    into a single SQLAlchemy URL.

    If <url_key> is present it takes precedence. Otherwise <path_key> is treated
    as a SQLite path, resolved relative to the config yaml's directory when not
    already absolute. Mirrors the state_db_url/state_db_path precedence used by v2.
    """
    if cfg.get(url_key):
        return cfg[url_key]
    raw_path   = cfg.get(path_key, default_path)
    config_dir = Path(config_yaml_path).parent.resolve()
    resolved   = (
        Path(raw_path)
        if Path(raw_path).is_absolute()
        else (config_dir / raw_path).resolve()
    )
    return f"sqlite:///{resolved}"


# ============================================================
# ===================== LOGGING ==============================
# ============================================================

def setup_logging(state_dir: str):
    log_dir = os.path.join(state_dir, "deid_status_check_v3")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f"status_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ============================================================
# ===================== STATE MANAGER ========================
# ============================================================

class StateManager:

    def __init__(self, state_dir: str):
        self.state_dir = state_dir
        os.makedirs(self.state_dir, exist_ok=True)

    def _file_path(self, table: str) -> str:
        return os.path.join(self.state_dir, f"{table}.json")

    def load(self, table: str) -> dict:
        path = self._file_path(table)
        if not os.path.exists(path):
            return {"deidentification": {}}
        with open(path) as f:
            state = json.load(f)
        # Own only the deidentification key; preserve everything else already in
        # the file (e.g. STAGE-1's 'diff_merge').
        state.setdefault("deidentification", {})
        return state

    def save(self, table: str, state: dict):
        path = self._file_path(table)
        with open(path, "w") as f:
            json.dump(state, f, indent=4)


# ============================================================
# ===================== HELPERS ==============================
# ============================================================

def get_row_count(conn_str: str, table_name: str) -> int:
    engine = _get_engine(conn_str)
    quoted = engine.dialect.identifier_preparer.quote(table_name)
    try:
        with engine.connect() as conn:
            return conn.execute(sa.text(f"SELECT COUNT(*) FROM {quoted}")).scalar() or 0
    except sa.exc.DBAPIError as e:
        # A zero-row source table means the deid engine never creates the dest table.
        # Treat a MISSING table as 0 rows (do NOT fail the status check); re-raise
        # anything that isn't a missing-table error (auth, syntax, …).
        msg = str(e).lower()
        if any(s in msg for s in ("doesn't exist", "1146", "invalid object name", "42s02")):
            logging.warning("[%s] table not found → treating as 0 rows", table_name)
            return 0
        raise


def check_table_in_state_db(
    state_db_url: str, config_key: str, table_name: str
) -> tuple[str | None, str | None]:
    """
    Returns (status, failure_remarks) from the state DB for the given table.

    state_db_url is a full SQLAlchemy URL:
      - SQLite:  sqlite:///path/to/state.db
      - MySQL:   mysql+pymysql://user:pass@host:port/db

    status values from deid v2:
        'pending'     — not started yet
        'in_progress' — currently running
        'completed'   — finished successfully
        'failed'      — finished with errors

    Returns (None, None) if the table is not found in the state DB.
    """
    engine = _get_engine(state_db_url)
    with engine.connect() as conn:
        row = conn.execute(
            sa.text("""
                SELECT status, failure_remarks
                FROM   table_states
                WHERE  config_key  = :config_key
                  AND  table_name  = :table_name
                ORDER  BY id DESC
                LIMIT  1
            """),
            {"config_key": config_key, "table_name": table_name},
        ).fetchone()

    if row is None:
        return None, None
    return row[0], row[1]


def get_failed_count(
    failed_rows_conn_str: str, source_db_name: str, table_name: str, config_key: str
) -> int:
    """
    Count rows the deid engine failed to deidentify for this table/config_key,
    from the failed_rows_{source_db_name} table in the failed-rows DB.

    failed_rows_conn_str is a full SQLAlchemy URL (SQLite or MySQL), resolved
    the same way as state_db_url.

    Returns 0 (with a warning) if the failed_rows_{source_db_name} table does
    not exist — a source DB that never produced failures may have no such table.
    """
    engine = _get_engine(failed_rows_conn_str)

    failed_table = f"failed_rows_{source_db_name}"
    quoted       = engine.dialect.identifier_preparer.quote(failed_table)

    try:
        with engine.connect() as conn:
            result = conn.execute(
                sa.text(f"""
                    SELECT COUNT(*)
                    FROM   {quoted}
                    WHERE  table_name = :table_name
                      AND  config_key = :config_key
                """),
                {"table_name": table_name, "config_key": config_key},
            )
            return result.scalar() or 0
    except sa.exc.DBAPIError as e:
        # Covers SQLite OperationalError ("no such table") and MySQL 1146
        # ("table doesn't exist") — treat a missing failed_rows table as 0.
        logging.warning("[%s] could not query %s (treating as 0 failed rows): %s",
                        table_name, failed_table, e)
        return 0


# ============================================================
# ===================== CORE LOGIC ===========================
# ============================================================

def process_table(
    table_name: str,
    config_key: str,
    state_db_url: str,
    source_conn: str,
    dest_conn: str,
    failed_rows_conn_str: str,
    source_db_name: str,
    state_manager: StateManager,
):
    state = state_manager.load(table_name)
    deid  = state.get("deidentification", {})

    # ── Skip conditions ──────────────────────────────────────
    if deid.get("is_merged"):
        logging.info("[%s] is_merged=True → skip", table_name)
        return

    if deid.get("deidentification_status") == "success":
        logging.info("[%s] deidentification_status=success → skip", table_name)
        return

    # ── Read status from state.db ─────────────────────────────
    status, failure_remarks = check_table_in_state_db(state_db_url, config_key, table_name)

    if status is None:
        logging.warning("[%s] not found in state.db (config_key=%s) → skip", table_name, config_key)
        return

    logging.info("[%s] state.db status=%s", table_name, status)

    if status == "failed":
        # No terminal 'failed' state: leave deidentification_status as 'started' so
        # run_deid + status_check retry this table on the next run. Record the remark
        # for observability only.
        logging.warning("[%s] engine status=failed remarks=%s → leaving status='started' (will retry)", table_name, failure_remarks)
        deid["failure_remarks"] = failure_remarks
        state["deidentification"] = deid
        state_manager.save(table_name, state)
        return

    if status in ("pending", "in_progress"):
        logging.info("[%s] status=%s → still running, no change", table_name, status)
        return

    # ── status == 'completed' → validate row counts ───────────
    logging.info("[%s] completed — checking row counts", table_name)

    try:
        original_count    = get_row_count(source_conn, table_name)
        deid_count        = get_row_count(dest_conn,   table_name)
        deid_failed_count = get_failed_count(failed_rows_conn_str, source_db_name, table_name, config_key)

        deid["rows_count"]         = original_count
        deid["row_count_original"] = original_count
        deid["row_count_deid"]     = deid_count
        deid["deid_failed_count"]  = deid_failed_count

        # Strict equality (no tolerance): every source row must either reach the
        # destination or be accounted for as a failed row. Note 0 == 0 + 0 is a
        # success — an empty source matching an empty dest with no failures.
        expected_dest_count = deid_count + deid_failed_count
        if original_count == expected_dest_count:
            logging.info(
                "[%s] row counts match: source=%d = dest=%d + deid_failed=%d → success",
                table_name, original_count, deid_count, deid_failed_count,
            )
            deid["deidentification_status"] = "success"
        else:
            # No terminal 'failed' state: leave status 'started' so the next run
            # re-runs deid + this check for the table.
            logging.warning(
                "[%s] row count mismatch: source=%d != dest=%d + deid_failed=%d (expected=%d) → leaving status='started' (will retry)",
                table_name, original_count, deid_count, deid_failed_count, expected_dest_count,
            )

        state["deidentification"] = deid
        state_manager.save(table_name, state)

    except Exception:
        logging.exception("[%s] error during row count check", table_name)


# ============================================================
# =========================== MAIN ===========================
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-yaml",      required=True, help="Path to deid v2 config.yaml")
    parser.add_argument("--tables-filepath",  required=True, help="Path to tables CSV")
    parser.add_argument("--max-workers",      type=int, default=8,
                        help="Parallel threads for per-table status checks (default: 8)")
    args = parser.parse_args()

    setup_logging(STATE_DIR)

    # ── Load deid v2 config ───────────────────────────────────
    cfg = load_deid_config(args.config_yaml)

    config_key     = cfg.get("config_key", "default")
    source_conn    = _conn_str(cfg["source_db"])
    dest_conn      = _conn_str(cfg["destination_db"])
    source_db_name = cfg["source_db"]["database"]

    # Prefer *_db_url (MySQL); fall back to *_db_path (SQLite)
    state_db_url       = _resolve_db_url(cfg, args.config_yaml, "state_db_url",       "state_db_path",       "./state.db")
    failed_rows_db_url = _resolve_db_url(cfg, args.config_yaml, "failed_rows_db_url", "failed_rows_db_path", "./failed_rows.db")

    logging.info("config_key=%s  state_db=%s  failed_rows_db=%s",
                 config_key, state_db_url, failed_rows_db_url)

    # ── Read table list ───────────────────────────────────────
    with open(args.tables_filepath, newline="") as f:
        tables = [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]

    logging.info("Checking deidentification status for %d tables", len(tables))

    state_manager = StateManager(STATE_DIR)

    def _check(table):
        process_table(
            table_name=table,
            config_key=config_key,
            state_db_url=state_db_url,
            source_conn=source_conn,
            dest_conn=dest_conn,
            failed_rows_conn_str=failed_rows_db_url,
            source_db_name=source_db_name,
            state_manager=state_manager,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(_check, table): table for table in tables}
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception:
                logging.exception("[%s] unexpected error during status check", futures[future])

    logging.info("Status check complete")


if __name__ == "__main__":
    main()
