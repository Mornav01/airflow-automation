#!/usr/bin/env python
"""
DAG 3 — Task 1: Deidentification Status Check

For each table in the CSV:
  1. Skip if is_merged = True
  2. Skip if deidentification_status = 'success'
  3. Check if all Django tasks are completed
       status=0  → not_started (still running)
       status=-1 → failed
  4. If all tasks done → compare original vs deidentified row counts (10% tolerance)
  5. Update deidentification_status = 'success' | 'failed' in state

Does NOT touch last_processed_date.
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

STATE_DIRECTORY = "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/state"

BASE_REPO_DIR = "/Users/ndaicnsarizona/Desktop/deidenmtification_polars/deidentification_v2/"
DJANGO_PROJECT_DIR = os.path.join(BASE_REPO_DIR, "deIdentification")

# Connection to the SOURCE table (diff-merged DB — input to deidentification)
DEID_SOURCE_CONN = "mssql+pymssql://sa:ndADMIN2025@localhost:1437/mobiledoc_base_2302"
#mobiledoc_diff_merged
# Connection to the DEIDENTIFIED output table
DEID_OUTPUT_CONN = "mysql+pymysql://ndadmin:ndADMIN%402025@localhost:3306/deidentified_mobiledoc_base"
#deidentified_mobiledoc_diff_staging
ROW_COUNT_TOLERANCE = 0.10   # 10 %

# ============================================================
# ===================== DJANGO SETUP =========================
# ============================================================

sys.path.insert(0, DJANGO_PROJECT_DIR)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

import django
django.setup()

from nd_api.models import DbDetailsModel, TableDetailsModel
from worker.models import Task

# ============================================================
# ===================== LOGGING ==============================
# ============================================================

def setup_logging(state_dir):
    log_dir = os.path.join(state_dir, "deid_status_check")
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
# ===================== HELPERS ==============================
# ============================================================

def get_row_count(conn_str, table_name):
    """Run SELECT COUNT(*) on the given table via SQLAlchemy.
    Uses dialect-aware identifier quoting ([...] for MSSQL, `...` for MySQL).
    """
    import sqlalchemy as sa
    engine = sa.create_engine(conn_str)
    quoted = engine.dialect.identifier_preparer.quote(table_name)
    with engine.connect() as conn:
        result = conn.execute(sa.text(f"SELECT COUNT(*) FROM {quoted}"))
        return result.scalar()


def check_task_status(table_obj):
    """
    Returns (all_done, has_failures, is_running).

    status=0  → task not started / pending
    status=-1 → task failed
    """
    failed_count  = Task.objects.filter(arguments__table_id=table_obj.id, status=-1).count()
    pending_count = Task.objects.filter(arguments__table_id=table_obj.id, status=0).count()

    has_failures = failed_count > 0
    is_running   = pending_count > 0 and not has_failures
    all_done     = not has_failures and not is_running

    logging.info(
        "Task status for table_id=%s → pending=%d failed=%d",
        table_obj.id, pending_count, failed_count,
    )
    return all_done, has_failures, is_running


# ============================================================
# ===================== CORE LOGIC ===========================
# ============================================================

def process_table(table_name, db, state_manager):
    state = state_manager.load(table_name)
    deid  = state.get("deidentification", {})

    # ── Skip conditions ──────────────────────────────────────
    if deid.get("is_merged"):
        logging.info("[%s] is_merged=True → skip", table_name)
        return

    if deid.get("deidentification_status") == "success":
        logging.info("[%s] deidentification_status=success → skip", table_name)
        return

    # ── Get Django table object ───────────────────────────────
    try:
        table_obj = TableDetailsModel.objects.get(db=db, table_name=table_name)
    except TableDetailsModel.DoesNotExist:
        logging.warning("[%s] not found in Django DB, skipping", table_name)
        return

    # ── Check task completion ─────────────────────────────────
    all_done, has_failures, is_running = check_task_status(table_obj)

    if has_failures:
        logging.warning("[%s] has failed tasks → deidentification_status=failed", table_name)
        deid["deidentification_status"] = "failed"
        state["deidentification"] = deid
        state_manager.save(table_name, state)
        return

    if is_running:
        logging.info("[%s] tasks still running → no change", table_name)
        return

    # ── All tasks done → check row counts ────────────────────
    logging.info("[%s] all tasks completed, checking row counts", table_name)

    try:
        original_count = get_row_count(DEID_SOURCE_CONN, table_name)
        deid_count     = get_row_count(DEID_OUTPUT_CONN, table_name)

        deid["rows_count"]        = original_count
        deid["row_count_original"] = original_count
        deid["row_count_deid"]    = deid_count

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
    parser.add_argument("--dbdetailsmodel_name", required=True)
    parser.add_argument("--tables_filepath",      required=True)
    parser.add_argument("--state_dir",            default=STATE_DIRECTORY)
    args = parser.parse_args()

    setup_logging(args.state_dir)

    try:
        db = DbDetailsModel.objects.get(db_name=args.dbdetailsmodel_name)
    except DbDetailsModel.DoesNotExist:
        logging.error("Database '%s' not found", args.dbdetailsmodel_name)
        sys.exit(1)

    with open(args.tables_filepath, newline="") as f:
        tables = [row[0].strip() for row in csv.reader(f)]

    logging.info("Checking deidentification status for %d tables", len(tables))

    state_manager = StateManager(args.state_dir)

    for table in tables:
        process_table(table, db, state_manager)

    logging.info("Status check complete")


if __name__ == "__main__":
    main()