#!/usr/bin/env python
"""
DAG 3 — Task 2: Merge Deidentified Data into Main Deidentified Schema

For each table in the CSV:
  1. Skip if deidentification_status != 'success'   (not ready)
  2. Skip if deidentification_status = 'success' AND is_merged = True  (already done)
  3. Else run merge_engine with db_type='mysql'
  4. On success set is_merged = True in state
"""

import argparse
import concurrent.futures
import csv
import json
import logging
import os
import sys
import traceback
from datetime import datetime

# ============================================================
# ===================== CONFIG ===============================
# ============================================================

# db_type for merge_engine (must be 'mysql' for deidentified schema)
DB_TYPE = "mysql"

from db_config import (
    MYSQL_DEID_BASE_CONN, MYSQL_DEID_STAGING_CONN, MYSQL_DEID_DIFFMERGED_CONN,
    STATE_DIR, PK_CSV, HISTORICAL_DATA_DEFAULT_DATE,
)

# Connection to the DEIDENTIFIED main DB (destination for this merge)
DEID_MAIN_CONN = MYSQL_DEID_BASE_CONN

# Connection to the DEIDENTIFIED diff staging DB (source for this merge)
DEID_DIFF_STAGING_CONN = MYSQL_DEID_STAGING_CONN

# Connection to the DEIDENTIFIED diff merged DB
DEID_DIFF_MERGED_CONN = MYSQL_DEID_DIFFMERGED_CONN
# ============================================================
# ===================== LOGGING ==============================
# ============================================================

def setup_logging(state_dir):
    log_dir = os.path.join(state_dir, "deid_merge")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f"deid_merge_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
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
# ===================== PK LOADER ============================
# ============================================================

def load_pk_map(pk_csv):
    pk_map = {}
    with open(pk_csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pk_value = row["primary_key"]
            pk_map[row["table_name"]] = (
                [col.strip() for col in pk_value.split(",") if col.strip()]
                if pk_value
                else []
            )
    return pk_map


# ============================================================
# ===================== MERGE STEP ===========================
# ============================================================

def run_merge_step(table, pk_cols, merge_engine):
    try:
        result = merge_engine.run_table(table=table, pk_cols=pk_cols, default_date=HISTORICAL_DATA_DEFAULT_DATE)
        logging.info("[%s] merge result: %s", table, result)
        return result
    except Exception as e:
        return {
            "success": False,
            "table": table,
            "error": str(e),
            "traceback": traceback.format_exc(),
        }


# ============================================================
# ===================== CORE LOGIC ===========================
# ============================================================

def process_table(table_name, pk_cols, state_manager, merge_engine):
    state = state_manager.load(table_name)
    deid  = state.get("deidentification", {})

    deid_status = deid.get("deidentification_status")
    is_merged   = deid.get("is_merged", False)

    # ── Skip: not successfully deidentified ──────────────────
    if deid_status != "success":
        logging.info(
            "[%s] deidentification_status=%s (not success) → skip merge",
            table_name, deid_status,
        )
        return

    # ── Skip: already merged ──────────────────────────────────
    if is_merged:
        logging.info("[%s] is_merged=True → skip merge", table_name)
        return

    # ── Run merge ─────────────────────────────────────────────
    logging.info("[%s] starting deidentified merge", table_name)
    result = run_merge_step(table_name, pk_cols, merge_engine)

    if result.get("success"):
        logging.info(
            "[%s] merge succeeded → updated=%d new=%d diffMerged=%d → is_merged=True",
            table_name,
            result.get("updated_rows", 0),
            result.get("new_rows", 0),
            result.get("diffMerged_inserted_rows", 0),
        )
        deid["is_merged"] = True
        deid["message"] = result.get("message")
    else:
        logging.error("[%s] merge failed: %s", table_name, result.get("error"))
        deid["message"] = result.get("error")

    state["deidentification"] = deid
    state_manager.save(table_name, state)


# ============================================================
# =========================== MAIN ===========================
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables_filepath", required=True)
    parser.add_argument("--pk_csv",          default=PK_CSV)
    parser.add_argument("--max_workers",     type=int, default=8)
    args = parser.parse_args()

    setup_logging(STATE_DIR)

    pk_map = load_pk_map(args.pk_csv)

    with open(args.tables_filepath, newline="") as f:
        tables = [row[0].strip() for row in csv.reader(f)]

    logging.info("Running deidentified merge for %d tables with max_workers=%d",
                 len(tables), args.max_workers)

    state_manager = StateManager(STATE_DIR)

    from merge_engine import MergeEngine
    merge_engine = MergeEngine(
        db_type=DB_TYPE,
        main_conn=DEID_MAIN_CONN,
        diff_conn=DEID_DIFF_STAGING_CONN,
        diffmerged_conn=DEID_DIFF_MERGED_CONN,
        pool_size=args.max_workers,
    )

    failed_tables = []

    def _process(table):
        if table not in pk_map:
            logging.warning("[%s] no PK found, skipping", table)
            return
        process_table(table, pk_map[table], state_manager, merge_engine)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {executor.submit(_process, table): table for table in tables}
            for future in concurrent.futures.as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                except Exception:
                    logging.exception("[%s] unexpected error during merge", table)
                    failed_tables.append(table)
    finally:
        merge_engine.dispose()

    if failed_tables:
        logging.error("Merge failed for tables: %s", failed_tables)
        sys.exit(1)

    logging.info("Deidentified merge complete")


if __name__ == "__main__":
    main()
