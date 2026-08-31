#!/usr/bin/env python
"""
Ignore-Rows Reconciliation — Script 1: fill IGNORE_ROWS_DB_NAME

The deid engine silently drops a row (never writes it to its output) when it
can't find matching mapping data for it. Once a table's overall
deidentification_status reaches 'success', state_deid_delete.py deletes ALL of
that cycle's rows from MERGED_DB_NAME (table-level, cutoff-based — not per
row), including the ones that were silently dropped. Those rows are now gone
from both MERGED_DB_NAME and DEID_BASE_NAME; only BASE_DB_NAME (the permanent
full-history source) still has them.

For each table, given a table name:
  a) ids_in_base    = SELECT nd_auto_increment_id FROM BASE_DB_NAME.table
  b) ids_in_deid    = SELECT nd_auto_increment_id FROM DEID_BASE_NAME.table
     remaining      = ids_in_base - ids_in_deid
  c) ids_in_merged  = SELECT nd_auto_increment_id FROM MERGED_DB_NAME.table
     actual_ignored_rows_nd_ids = remaining - ids_in_merged
  d) previous_run_ids = SELECT nd_auto_increment_id FROM IGNORE_ROWS_DB_NAME.table
     new_ids           = actual_ignored_rows_nd_ids - previous_run_ids  → insert
     only_previous_ids = previous_run_ids - actual_ignored_rows_nd_ids → mark 'processed'
     (ids in both sets) → no-op

Full scan every run — no checkpoint/watermark state. IGNORE_ROWS_DB_NAME itself
(via its ndig_status column) is the durable state for this feature; no
{STATE_DIR}/{table}.json file is used, unlike diff-merge/deid.

IGNORE_ROWS_DB_NAME is a new MSSQL database on the SAME SQL Server instance as
BASE_DB_NAME, so its per-table schema is cloned from BASE_DB_NAME on first use
via a same-instance `SELECT TOP 0 * INTO` (SQL Server does not carry over
constraints/indexes with that statement — only column types + NOT NULL — so
the unique index on nd_auto_increment_id is added explicitly afterward).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from functools import lru_cache

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from db_config import BASE_DB, DIFFMERGED_CONN, MYSQL_DEID_BASE_CONN, IGNORE_ROWS_DB  # noqa: E402
from merge_engine import _raw_bulk_insert  # noqa: E402

_ID_COLUMN         = "nd_auto_increment_id"
_UNIQUE_INDEX_NAME = "ux_nd_auto_increment_id"
_CHUNK             = 1000


# ============================================================
# ===================== ENGINES / HELPERS =====================
# ============================================================

@lru_cache(maxsize=8)
def _get_engine(conn_str: str):
    """Reused, thread-safe engine per connection string (shared across the
    ThreadPoolExecutor in ignore_rows_run.py)."""
    return sa.create_engine(conn_str, pool_pre_ping=True)


def _extract_db(conn_str: str) -> str:
    return conn_str.split("/")[-1].split("?")[0]


def _table_exists_mssql(engine, table: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :tbl"
        ), {"tbl": table}).scalar() > 0


def _table_exists_mysql(engine, db: str, table: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = :db AND table_name = :tbl
        """), {"db": db, "tbl": table}).scalar() > 0


def _column_exists_mssql(engine, table: str, column: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = :tbl AND COLUMN_NAME = :col
        """), {"tbl": table, "col": column}).scalar() > 0


def _index_exists_mssql(engine, table: str, index_name: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT COUNT(*) FROM sys.indexes
            WHERE object_id = OBJECT_ID(:tbl) AND name = :idx
        """), {"tbl": table, "idx": index_name}).scalar() > 0


def _fetch_id_set_mssql(engine, table: str) -> set:
    with engine.connect() as conn:
        return {row[0] for row in conn.execute(text(f"SELECT {_ID_COLUMN} FROM {table}"))}


def _fetch_id_set_mysql(engine, db: str, table: str) -> set:
    with engine.connect() as conn:
        return {row[0] for row in conn.execute(text(f"SELECT {_ID_COLUMN} FROM `{db}`.`{table}`"))}


def _chunks(ids, size=_CHUNK):
    ids = list(ids)
    for i in range(0, len(ids), size):
        yield ids[i:i + size]


# ============================================================
# ===================== SCHEMA SETUP ==========================
# ============================================================

def _ensure_ignore_rows_table(ignore_engine, table: str) -> None:
    """Idempotent: clone BASE_DB_NAME.table's schema into IGNORE_ROWS_DB_NAME on
    first use, add the 4 ndig_* tracking columns, and a unique index on
    nd_auto_increment_id (SELECT TOP 0 * INTO carries column types/NOT NULL but
    not constraints/indexes, so the index is added explicitly)."""
    base_db = _extract_db(BASE_DB)

    if not _table_exists_mssql(ignore_engine, table):
        with ignore_engine.begin() as conn:
            conn.execute(text(f"""
                IF OBJECT_ID('{table}', 'U') IS NULL
                BEGIN
                    SELECT TOP 0 * INTO {table} FROM {base_db}.dbo.{table}
                END
            """))
        logging.info("[%s] created IGNORE_ROWS_DB_NAME.%s (schema cloned from BASE_DB_NAME)", table, table)

    if not _column_exists_mssql(ignore_engine, table, "ndig_status"):
        with ignore_engine.begin() as conn:
            conn.execute(text(f"""
                ALTER TABLE {table} ADD
                    ndig_processing_count INT NOT NULL DEFAULT 0,
                    ndig_last_attempt_date DATETIME NULL,
                    ndig_status VARCHAR(20) NOT NULL DEFAULT 'not_started',
                    ndig_created_date DATETIME NOT NULL DEFAULT GETDATE()
            """))
        logging.info("[%s] added ndig_* tracking columns", table)

    if not _index_exists_mssql(ignore_engine, table, _UNIQUE_INDEX_NAME):
        with ignore_engine.begin() as conn:
            conn.execute(text(f"CREATE UNIQUE INDEX {_UNIQUE_INDEX_NAME} ON {table} ({_ID_COLUMN})"))
        logging.info("[%s] created unique index on %s", table, _ID_COLUMN)


# ============================================================
# ===================== CORE LOGIC ============================
# ============================================================

def process_table(table: str) -> None:
    base_engine   = _get_engine(BASE_DB)
    deid_engine   = _get_engine(MYSQL_DEID_BASE_CONN)
    merged_engine = _get_engine(DIFFMERGED_CONN)
    ignore_engine = _get_engine(IGNORE_ROWS_DB)

    # ── Guard: table must have been through a normal diff-merge cycle ────────
    if not _table_exists_mssql(base_engine, table):
        logging.info("[%s] not present in BASE_DB_NAME — skip (not yet diff-merged)", table)
        return
    if not _column_exists_mssql(base_engine, table, _ID_COLUMN):
        logging.info("[%s] BASE_DB_NAME has no %s column yet — skip", table, _ID_COLUMN)
        return

    # ── Guard: table must have had at least one successful deid cycle. Do NOT
    # treat a missing DEID_BASE_NAME table as "zero ids" — that would flag
    # every single BASE_DB_NAME row as ignored. ───────────────────────────────
    deid_db = _extract_db(MYSQL_DEID_BASE_CONN)
    if not _table_exists_mysql(deid_engine, deid_db, table):
        logging.info("[%s] not present in DEID_BASE_NAME yet — skip", table)
        return

    _ensure_ignore_rows_table(ignore_engine, table)

    base_ids  = _fetch_id_set_mssql(base_engine, table)
    deid_ids  = _fetch_id_set_mysql(deid_engine, deid_db, table)
    remaining = base_ids - deid_ids

    merged_ids = _fetch_id_set_mssql(merged_engine, table) if _table_exists_mssql(merged_engine, table) else set()
    actual_ignored_rows_nd_ids = remaining - merged_ids

    previous_run_ids = _fetch_id_set_mssql(ignore_engine, table) if _table_exists_mssql(ignore_engine, table) else set()

    new_ids           = actual_ignored_rows_nd_ids - previous_run_ids
    only_previous_ids = previous_run_ids - actual_ignored_rows_nd_ids
    # common_ids = actual_ignored_rows_nd_ids & previous_run_ids → no-op, nothing to do

    logging.info(
        "[%s] base=%d deid=%d merged=%d ignored=%d previous=%d new=%d resolved=%d",
        table, len(base_ids), len(deid_ids), len(merged_ids),
        len(actual_ignored_rows_nd_ids), len(previous_run_ids), len(new_ids), len(only_previous_ids),
    )

    # ── Rows ignored last run but no longer ignored (now present in DEID_BASE_NAME) ─
    if only_previous_ids:
        id_param = sa.bindparam("ids", expanding=True)
        for chunk in _chunks(only_previous_ids):
            with ignore_engine.begin() as conn:
                conn.execute(
                    text(f"UPDATE {table} SET ndig_status = 'processed' "
                         f"WHERE {_ID_COLUMN} IN :ids AND ndig_status != 'processed'").bindparams(id_param),
                    {"ids": chunk},
                )
        logging.info("[%s] marked %d row(s) processed", table, len(only_previous_ids))

    # ── Newly-ignored rows: pull full rows from BASE_DB_NAME, insert with ndig_* defaults ─
    if new_ids:
        now = datetime.now()
        rows, id_param = [], sa.bindparam("ids", expanding=True)
        for chunk in _chunks(new_ids):
            with base_engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT * FROM {table} WHERE {_ID_COLUMN} IN :ids").bindparams(id_param),
                    {"ids": chunk},
                )
                cols = result.keys()
                rows.extend(dict(zip(cols, r)) for r in result.fetchall())

        df = pd.DataFrame(rows)
        df["ndig_processing_count"]  = 0
        df["ndig_last_attempt_date"] = now
        df["ndig_status"]            = "not_started"
        df["ndig_created_date"]      = now

        ignore_db = _extract_db(IGNORE_ROWS_DB)
        _raw_bulk_insert(ignore_engine, table, df, "mssql", ignore_db)
        logging.info("[%s] inserted %d new ignored row(s)", table, len(new_ids))


# ============================================================
# =========================== MAIN ============================
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ignore-rows reconciliation — Script 1 (single table)")
    parser.add_argument("--table", required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    process_table(args.table)
