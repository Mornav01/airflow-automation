#!/usr/bin/env python
"""
DAG 3 — Task 3: Delete Processed Original Data from original_diff_merged (MSSQL)

The pipeline flow is:
    1. Diff + Merge    → accumulates original data into original_diff_merged
    2. Deidentification → reads from original_diff_merged and produces deidentified data
    3. Deid Merge      → merges deidentified data into deidentified_main schema
    4. THIS SCRIPT     → deletes the now-processed original rows from original_diff_merged
                         so the table does not grow unboundedly

Deletion condition:
    nd_extracted_date <= last_processed_datetime
    (i.e. every row that was already deidentified and merged into the main schema)

For each table in the CSV:
  1. Only run if is_merged = True AND deidentification_status = 'success'
  2. Skip if is_deleted = True
  3. DELETE FROM original_diff_merged.<table>
         WHERE nd_extracted_date <= last_processed_datetime
  4. On success set is_deleted = True in state
"""

import argparse
import concurrent.futures
import csv
import json
import logging
import os
import sys
from datetime import datetime

import sqlalchemy as sa

# ============================================================
# ===================== CONFIG ===============================
# ============================================================
from db_config import DIFFMERGED_CONN, STATE_DIR

# Original accumulated data DB — rows are deleted here after deidentification + merge
DEID_DIFF_CONN = DIFFMERGED_CONN

# Column that records when each row was extracted into the diff pipeline.
# Type in MSSQL is `datetime` (3-digit fractional seconds, 1/300 s grid).
EXTRACTED_DATE_COLUMN = "nd_extracted_date"


# ============================================================
# ===================== LOGGING ==============================
# ============================================================
def setup_logging(state_dir):
    log_dir = os.path.join(state_dir, "deid_delete")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f"deid_delete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
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
    def __init__(self, state_dir):
        self.state_dir = state_dir
        os.makedirs(self.state_dir, exist_ok=True)

    def _file_path(self, table):
        return os.path.join(self.state_dir, f"{table}.json")

    def load(self, table):
        path = self._file_path(table)
        if not os.path.exists(path):
            return {"deidentification": {}}
        with open(path, "r") as f:
            state = json.load(f)
        # Own only the deidentification key; preserve everything else already in
        # the file (e.g. STAGE-1's 'diff_merge').
        state.setdefault("deidentification", {})
        return state

    def save(self, table, state):
        path = self._file_path(table)
        with open(path, "w") as f:
            json.dump(state, f, indent=4)


# ============================================================
# ===================== CUTOFF PARSING =======================
# ============================================================
def _parse_cutoff(value):
    """
    Turn the state's last_processed_datetime into a real `datetime`.

    Why this exists — the JSON state holds an ISO *string*
    ('2026-07-28T07:39:27.023778'). Binding that directly made pyodbc send a
    VARCHAR, forcing SQL Server to string-parse it against a `datetime` column,
    which fails with 241 / 22007 on 6-digit fractional seconds. Binding a real
    datetime sends SQL_TIMESTAMP and skips string parsing entirely.

    Why the microseconds are dropped — `datetime` keeps fractional seconds on a
    1/300 s grid and rounds a supplied value to the NEAREST tick, which can round
    *upward* and delete rows extracted just after the cutoff, i.e. rows that were
    never deidentified. Flooring to the whole second is strictly conservative:
    worst case a handful of rows survive until the next cycle. (Sub-millisecond
    precision also trips 22008 'fractional second precision exceeds the scale'.)
    """
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return dt.replace(microsecond=0)


# ============================================================
# ===================== DELETE STEP ==========================
# ============================================================
def delete_old_diff_rows(table_name, cutoff_date, engine):
    sql = sa.text(
        f"DELETE FROM [{table_name}] "
        f"WHERE [{EXTRACTED_DATE_COLUMN}] <= :cutoff"
    ).bindparams(
        # Pin the bind type so a stray string can never silently regress to a
        # server-side string conversion again.
        sa.bindparam("cutoff", type_=sa.DateTime())
    )
    with engine.begin() as conn:
        result = conn.execute(sql, {"cutoff": cutoff_date})
        return result.rowcount


# ============================================================
# ===================== CORE LOGIC ===========================
# ============================================================
def process_table(table_name, state_manager, engine):
    state = state_manager.load(table_name)
    deid  = state.get("deidentification", {})

    deid_status = deid.get("deidentification_status")
    is_merged   = deid.get("is_merged", False)
    is_deleted  = deid.get("is_deleted", False)

    # ── Guard: only delete original data once deid + merge are complete ──
    if deid_status != "success" or not is_merged:
        logging.info(
            "[%s] not eligible for delete "
            "(deidentification_status=%s, is_merged=%s) → skip",
            table_name, deid_status, is_merged,
        )
        return

    # ── Skip: already deleted ─────────────────────────────────
    if is_deleted:
        logging.info("[%s] is_deleted=True → skip", table_name)
        return

    # ── Resolve cutoff datetime from state ────────────────────
    last_processed_datetime = deid.get("last_processed_datetime")
    if not last_processed_datetime:
        logging.warning("[%s] last_processed_datetime missing → skip", table_name)
        return

    try:
        cutoff = _parse_cutoff(last_processed_datetime)
    except (TypeError, ValueError) as e:
        logging.error("[%s] unparseable last_processed_datetime %r: %s",
                      table_name, last_processed_datetime, e)
        deid["delete_error"] = f"unparseable last_processed_datetime: {e}"
        state["deidentification"] = deid
        state_manager.save(table_name, state)
        raise

    # ── Run delete ────────────────────────────────────────────
    logging.info(
        "[%s] deleting rows where %s <= %s (from state %s)",
        table_name, EXTRACTED_DATE_COLUMN, cutoff, last_processed_datetime,
    )
    try:
        rows_deleted = delete_old_diff_rows(table_name, cutoff, engine)
        logging.info("[%s] deleted %d rows → is_deleted=True", table_name, rows_deleted)
        deid["is_deleted"]   = True
        deid["rows_deleted"] = rows_deleted
        deid.pop("delete_error", None)          # clear a stale error from a prior run
        state["deidentification"] = deid
        state_manager.save(table_name, state)
    except Exception as e:
        logging.exception("[%s] delete failed", table_name)
        deid["delete_error"] = str(e)
        state["deidentification"] = deid
        state_manager.save(table_name, state)
        # Re-raise so main() records the table and the DAG task actually fails.
        # Swallowing this previously let the task report success while nothing
        # was deleted and original_diff_merged kept growing.
        raise


# ============================================================
# =========================== MAIN ===========================
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables_filepath", required=True)
    parser.add_argument("--max_workers",     type=int, default=8)
    args = parser.parse_args()

    setup_logging(STATE_DIR)

    engine = sa.create_engine(
        DEID_DIFF_CONN,
        pool_size=args.max_workers,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
    )

    with open(args.tables_filepath, newline="") as f:
        tables = [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]

    logging.info("Deleting processed original data from original_diff_merged for %d tables "
                 "with max_workers=%d", len(tables), args.max_workers)

    state_manager = StateManager(STATE_DIR)
    failed_tables = []

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {executor.submit(process_table, table, state_manager, engine): table
                       for table in tables}
            for future in concurrent.futures.as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                except Exception:
                    logging.exception("[%s] unexpected error during delete", table)
                    failed_tables.append(table)
    finally:
        engine.dispose()

    if failed_tables:
        logging.error("Delete step failed for tables: %s", failed_tables)
        sys.exit(1)

    logging.info("Original data cleanup from original_diff_merged complete")


if __name__ == "__main__":
    main()
    