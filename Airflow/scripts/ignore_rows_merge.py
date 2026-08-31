#!/usr/bin/env python
"""
Ignore-Rows Reconciliation — Script 2: copy IGNORE_ROWS_DB_NAME rows into MERGED_DB_NAME

Reads rows from IGNORE_ROWS_DB_NAME whose ndig_status != 'processed' (both
'not_started' and previously-retried 'sent_for_processing' rows) and copies
them — minus the 4 ndig_* tracking columns — into MERGED_DB_NAME, so they get a
second chance to flow through the normal deid pipeline. Rows whose
nd_auto_increment_id already exists in MERGED_DB_NAME are skipped (not
recopied). Only for rows actually copied THIS run:
  ndig_processing_count += 1, ndig_last_attempt_date = now, ndig_status = 'sent_for_processing'

Whether a copied row eventually reaches DEID_BASE_NAME (and so gets marked
'processed') is Script 1's job on a later run — this script never sets
'processed' itself.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from db_config import BASE_DB, DIFFMERGED_CONN, IGNORE_ROWS_DB  # noqa: E402
from merge_engine import _raw_bulk_insert  # noqa: E402
from ignore_rows_fill import (  # noqa: E402
    _get_engine, _extract_db, _table_exists_mssql, _fetch_id_set_mssql, _chunks, _ID_COLUMN,
)

_NDIG_COLUMNS = ["ndig_processing_count", "ndig_last_attempt_date", "ndig_status", "ndig_created_date"]


# ============================================================
# ===================== SCHEMA SETUP ==========================
# ============================================================

def _ensure_merged_table(merged_engine, table: str) -> None:
    """Idempotent: clone BASE_DB_NAME.table's schema into MERGED_DB_NAME if the
    table doesn't currently exist there (it may legitimately not — the normal
    pipeline's state_deid_delete.py drops processed rows and the table itself
    can be absent between cycles)."""
    if _table_exists_mssql(merged_engine, table):
        return
    base_db = _extract_db(BASE_DB)
    with merged_engine.begin() as conn:
        conn.execute(text(f"""
            IF OBJECT_ID('{table}', 'U') IS NULL
            BEGIN
                SELECT TOP 0 * INTO {table} FROM {base_db}.dbo.{table}
            END
        """))
    logging.info("[%s] created MERGED_DB_NAME.%s (schema cloned from BASE_DB_NAME)", table, table)


def _fetch_pending_ids(ignore_engine, table: str) -> set:
    with ignore_engine.connect() as conn:
        return {row[0] for row in conn.execute(text(
            f"SELECT {_ID_COLUMN} FROM {table} WHERE ndig_status != 'processed'"
        ))}


# ============================================================
# ===================== CORE LOGIC ============================
# ============================================================

def process_table(table: str) -> None:
    ignore_engine = _get_engine(IGNORE_ROWS_DB)
    merged_engine = _get_engine(DIFFMERGED_CONN)

    if not _table_exists_mssql(ignore_engine, table):
        logging.info("[%s] no IGNORE_ROWS_DB_NAME table yet — nothing to merge", table)
        return

    candidates = _fetch_pending_ids(ignore_engine, table)
    if not candidates:
        logging.info("[%s] no pending ignored rows", table)
        return

    _ensure_merged_table(merged_engine, table)

    already_in_merged = _fetch_id_set_mssql(merged_engine, table)
    to_copy = candidates - already_in_merged

    if not to_copy:
        logging.info("[%s] all %d candidate row(s) already present in MERGED_DB_NAME", table, len(candidates))
        return

    # ── Pull full rows from IGNORE_ROWS_DB_NAME, dropping the ndig_* columns ─
    rows, id_param = [], sa.bindparam("ids", expanding=True)
    for chunk in _chunks(to_copy):
        with ignore_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT * FROM {table} WHERE {_ID_COLUMN} IN :ids").bindparams(id_param),
                {"ids": chunk},
            )
            cols = [c for c in result.keys() if c not in _NDIG_COLUMNS]
            rows.extend({c: v for c, v in zip(result.keys(), r) if c not in _NDIG_COLUMNS}
                        for r in result.fetchall())

    df = pd.DataFrame(rows, columns=cols)
    merged_db = _extract_db(DIFFMERGED_CONN)
    _raw_bulk_insert(merged_engine, table, df, "mssql", merged_db)
    logging.info("[%s] copied %d ignored row(s) into MERGED_DB_NAME", table, len(to_copy))

    # ── Bump retry bookkeeping only for rows actually copied this run ───────
    now = datetime.now()
    id_param = sa.bindparam("ids", expanding=True)
    for chunk in _chunks(to_copy):
        with ignore_engine.begin() as conn:
            conn.execute(
                text(f"""
                    UPDATE {table}
                    SET ndig_processing_count = ndig_processing_count + 1,
                        ndig_last_attempt_date = :now,
                        ndig_status = 'sent_for_processing'
                    WHERE {_ID_COLUMN} IN :ids
                """).bindparams(id_param),
                {"now": now, "ids": chunk},
            )
    logging.info("[%s] updated retry bookkeeping for %d row(s)", table, len(to_copy))


# ============================================================
# =========================== MAIN ============================
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ignore-rows reconciliation — Script 2 (single table)")
    parser.add_argument("--table", required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    process_table(args.table)
