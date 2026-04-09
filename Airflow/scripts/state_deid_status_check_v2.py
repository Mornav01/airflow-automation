#!/usr/bin/env python
"""
DAG 3 (v2) — Task 1: Deidentification Status Check

Replaces state_deid_status_check.py for the deid v2 system.
No Django dependency — reads state directly from state.db (SQLite).

For each table in the CSV:
  1. Skip if is_merged = True
  2. Skip if deidentification_status = 'success'
  3. Read table_state.status from state.db
       'completed'   → proceed to row-count check
       'failed'      → mark deidentification_status=failed in JSON state
       anything else → still in progress, no change
  4. If completed → compare source vs destination row counts (10% tolerance)
  5. Update deidentification_status = 'success' | 'failed' in JSON state file

Config is read entirely from the provided config.yaml — including config_key,
state_db_path, and source/destination connection strings.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import sqlalchemy as sa
import yaml

# ============================================================
# ===================== CONFIG ===============================
# ============================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from db_config import STATE_DIR  # noqa: E402  # type: ignore[import-untyped]

ROW_COUNT_TOLERANCE = 0.10  # 10%

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


# ============================================================
# ===================== LOGGING ==============================
# ============================================================

def setup_logging(state_dir: str):
    log_dir = os.path.join(state_dir, "deid_status_check_v2")
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
            return {"diff": {}, "merge": {}, "deidentification": {}}
        with open(path) as f:
            state = json.load(f)
        state.setdefault("diff", {})
        state.setdefault("merge", {})
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
    engine = sa.create_engine(conn_str, pool_pre_ping=True)
    quoted = engine.dialect.identifier_preparer.quote(table_name)
    with engine.connect() as conn:
        result = conn.execute(sa.text(f"SELECT COUNT(*) FROM {quoted}"))
        return result.scalar()


def check_table_in_state_db(
    state_db_path: str, config_key: str, table_name: str
) -> tuple[str | None, str | None]:
    """
    Returns (status, failure_remarks) from state.db for the given table.

    status values from deid v2:
        'pending'     — not started yet
        'in_progress' — currently running
        'completed'   — finished successfully
        'failed'      — finished with errors

    Returns (None, None) if the table is not found in state.db.
    """
    engine = sa.create_engine(
        f"sqlite:///{state_db_path}",
        connect_args={"check_same_thread": False},
    )
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


# ============================================================
# ===================== CORE LOGIC ===========================
# ============================================================

def process_table(
    table_name: str,
    config_key: str,
    state_db_path: str,
    source_conn: str,
    dest_conn: str,
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
    status, failure_remarks = check_table_in_state_db(state_db_path, config_key, table_name)

    if status is None:
        logging.warning("[%s] not found in state.db (config_key=%s) → skip", table_name, config_key)
        return

    logging.info("[%s] state.db status=%s", table_name, status)

    if status == "failed":
        logging.warning("[%s] status=failed remarks=%s → deidentification_status=failed", table_name, failure_remarks)
        deid["deidentification_status"] = "failed"
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
        original_count = get_row_count(source_conn, table_name)
        deid_count     = get_row_count(dest_conn,   table_name)

        deid["rows_count"]         = original_count
        deid["row_count_original"] = original_count
        deid["row_count_deid"]     = deid_count

        if original_count and original_count > 0:
            diff_ratio = abs(original_count - deid_count) / original_count
            if diff_ratio > ROW_COUNT_TOLERANCE:
                logging.warning(
                    "[%s] row count mismatch: original=%d deid=%d diff=%.1f%% → failed",
                    table_name, original_count, deid_count, diff_ratio * 100,
                )
                deid["deidentification_status"] = "failed"
                state["deidentification"] = deid
                state_manager.save(table_name, state)
                return

        logging.info("[%s] row counts OK → deidentification_status=success", table_name)
        deid["deidentification_status"] = "success"
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
    args = parser.parse_args()

    setup_logging(STATE_DIR)

    # ── Load deid v2 config ───────────────────────────────────
    cfg = load_deid_config(args.config_yaml)

    config_key  = cfg.get("config_key", "default")
    source_conn = _conn_str(cfg["source_db"])
    dest_conn   = _conn_str(cfg["destination_db"])

    # Resolve state_db_path relative to the config file's directory
    raw_state_db = cfg.get("state_db_path", "./state.db")
    config_dir   = Path(args.config_yaml).parent.resolve()
    state_db_path = str(
        (config_dir / raw_state_db).resolve()
        if not Path(raw_state_db).is_absolute()
        else Path(raw_state_db)
    )

    logging.info("config_key=%s  state_db=%s", config_key, state_db_path)

    # ── Read table list ───────────────────────────────────────
    with open(args.tables_filepath, newline="") as f:
        tables = [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]

    logging.info("Checking deidentification status for %d tables", len(tables))

    state_manager = StateManager(STATE_DIR)

    for table in tables:
        process_table(
            table_name=table,
            config_key=config_key,
            state_db_path=state_db_path,
            source_conn=source_conn,
            dest_conn=dest_conn,
            state_manager=state_manager,
        )

    logging.info("Status check complete")


if __name__ == "__main__":
    main()
