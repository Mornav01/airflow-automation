#!/usr/bin/env python
"""
DAG 3 — Task 3: Delete Old Diff Data from Deidentified-Diff DB

For each table in the CSV:
  1. Only run if is_merged = True AND deidentification_status = 'success'
  2. Skip if is_deleted = True
  3. DELETE FROM <table> WHERE nd_extracted_date <= date(last_processed_date)
  4. On success set is_deleted = True in state
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime

# ============================================================
# ===================== CONFIG ===============================
# ============================================================

STATE_DIRECTORY = "/Users/ndaicnsarizona/Desktop/production_state_incremental/state"

# Connection to the DEIDENTIFIED-DIFF DB (where rows are deleted)
DEID_DIFF_CONN = "mysql+pymysql://user:password@localhost:3306/mobiledoc_deidentified_diff"

# Column in the diff table that holds the extraction date
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
            return {"diff": {}, "merge": {}, "deidentification": {}}
        with open(path, "r") as f:
            state = json.load(f)
        state.setdefault("diff", {})
        state.setdefault("merge", {})
        state.setdefault("deidentification", {})
        return state

    def save(self, table, state):
        path = self._file_path(table)
        with open(path, "w") as f:
            json.dump(state, f, indent=4)


# ============================================================
# ===================== DELETE STEP ==========================
# ============================================================

def delete_old_diff_rows(table_name, cutoff_date, engine):
    """
    Delete rows from the deidentified-diff table where
    nd_extracted_date <= cutoff_date.

    Returns the number of rows deleted.
    """
    import sqlalchemy as sa

    sql = sa.text(
        f"DELETE FROM `{table_name}` "
        f"WHERE `{EXTRACTED_DATE_COLUMN}` <= :cutoff"
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

    # ── Guard: only run when fully done and merged ────────────
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

    # ── Resolve cutoff date from state ────────────────────────
    last_processed_date = deid.get("last_processed_date")
    if not last_processed_date:
        logging.warning("[%s] last_processed_date missing → skip", table_name)
        return

    # last_processed_date is already a date string (YYYY-MM-DD)
    cutoff = last_processed_date   # SQLAlchemy accepts 'YYYY-MM-DD' strings

    # ── Run delete ────────────────────────────────────────────
    logging.info(
        "[%s] deleting rows where %s <= %s",
        table_name, EXTRACTED_DATE_COLUMN, cutoff,
    )

    try:
        rows_deleted = delete_old_diff_rows(table_name, cutoff, engine)
        logging.info("[%s] deleted %d rows → is_deleted=True", table_name, rows_deleted)
        deid["is_deleted"]    = True
        deid["rows_deleted"]  = rows_deleted
    except Exception as e:
        logging.exception("[%s] delete failed", table_name)
        deid["delete_error"] = str(e)

    state["deidentification"] = deid
    state_manager.save(table_name, state)


# ============================================================
# =========================== MAIN ===========================
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables_filepath", required=True)
    parser.add_argument("--state_dir",       default=STATE_DIRECTORY)
    parser.add_argument("--deid_diff_conn",  default=DEID_DIFF_CONN,
                        help="SQLAlchemy connection string for the deidentified-diff DB")
    args = parser.parse_args()

    setup_logging(args.state_dir)

    import sqlalchemy as sa
    engine = sa.create_engine(args.deid_diff_conn)

    with open(args.tables_filepath, newline="") as f:
        tables = [row[0].strip() for row in csv.reader(f)]

    logging.info("Running deidentified-diff delete for %d tables", len(tables))

    state_manager = StateManager(args.state_dir)

    failed_tables = []
    for table in tables:
        try:
            process_table(table, state_manager, engine)
        except Exception:
            logging.exception("[%s] unexpected error during delete", table)
            failed_tables.append(table)

    if failed_tables:
        logging.error("Delete step failed for tables: %s", failed_tables)
        sys.exit(1)

    logging.info("Deidentified-diff delete complete")


if __name__ == "__main__":
    main()
