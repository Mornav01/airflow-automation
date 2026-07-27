import os
import sys
import json
import csv
import argparse
import logging
import concurrent.futures
from datetime import date, datetime
import traceback

from db_config import (
    BASE_DB, PROD_DB, DIFF_STAGING_DB, DIFFMERGED_CONN,
    DB_TYPE, DIFF_SCHEMA,
    STATE_DIR, HISTORICAL_DATA_DEFAULT_DATE,
)

# ============================================================
# ===================== DEFAULT CONFIG =======================
# ============================================================

STATE_DIRECTORY = STATE_DIR

# ============================================================
# ===================== LOGGING SETUP ========================
# ============================================================

def setup_logging():
    os.makedirs(STATE_DIRECTORY, exist_ok=True)
    os.makedirs(os.path.join(STATE_DIRECTORY, "runner"), exist_ok=True)

    log_file = os.path.join(
        STATE_DIRECTORY, "runner",
        f"runner_{date.today().isoformat()}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
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
            return {"diff_merge": {}}
        with open(path, "r") as f:
            state = json.load(f)
        # Single STAGE-1 key. Preserve every other key already in the file
        # (e.g. 'deidentification' from STAGE 4, and any vestigial 'diff'/'merge').
        state.setdefault("diff_merge", {})
        return state

    def save(self, table, state):
        path = self._file_path(table)
        with open(path, "w") as f:
            json.dump(state, f, indent=4)

# ============================================================
# ===================== SKIP LOGIC ===========================
# ============================================================

def _now_iso():
    """Host-local ISO timestamp, second precision (same clock as STAGE-4)."""
    return datetime.now().isoformat(timespec="seconds")


def _date_of(dt_iso):
    """DATE part (YYYY-MM-DD) of an ISO datetime string, or None."""
    return str(dt_iso)[:10] if dt_iso else None


def should_run_init(dm, today):
    """Init resets the diff_merge key ONLY when the previous cycle is not mid-flight.

    RUN  : empty/new; OR extraction_status != 'success' (started/retry); OR
           (success AND is_merged AND date(extraction_datetime) < today)  → new cycle.
    SKIP : success AND not is_merged            → resume merge; OR
           success AND is_merged AND date == today → already done today.
    (Daily gate compares the DATE part of extraction_datetime, not the full datetime.)
    """
    status = dm.get("extraction_status")
    if not status:
        return True, "new/empty diff_merge → init"
    if status != "success":
        return True, "extraction_status != success → re-init (refresh)"
    if not dm.get("is_merged", False):
        return False, "success + not merged → skip init (resume merge)"
    if _date_of(dm.get("extraction_datetime")) == today:
        return False, "success + merged + today → skip init"
    return True, "success + merged + older day → new cycle init"


def should_run_diff(dm):
    return dm.get("extraction_status") == "started"


def should_run_merge(dm):
    return dm.get("extraction_status") == "success" and not dm.get("is_merged", False)

# ============================================================
# ===================== CORE EXECUTION =======================
# ============================================================

def run_diff_step(table):
    """
    Uses the Python diff script (diff_script_optimised)
    instead of the stored procedure.
    Schema is hardcoded as 'dbo'.
    """
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    from diff_script_optimised import run_diff

    try:
        run_diff(
            table_name=table,
            old_conn=BASE_DB,
            new_conn=PROD_DB,
            diff_conn=DIFF_STAGING_DB,
            schema=DIFF_SCHEMA,
        )
        return {
            "success": True,
            "table": table,
            "message": "Diff Calculated Successfully"
        }
    except Exception as e:
        tb = traceback.format_exc()
        logging.error(f"Diff failed for table '{table}': {e}\n{tb}")
        return {
            "success": False,
            "table": table,
            "error": str(e),
            "traceback": tb
        }


def run_merge_step(table, pk_cols, merge_engine):
    try:
        result = merge_engine.run_table(
            table=table,
            pk_cols=pk_cols,
            default_date=HISTORICAL_DATA_DEFAULT_DATE
        )

        logging.info(str(result))
        return result
    except Exception as e:
        return {
            "success": False,
            "table": table,
            "error": str(e),
            "traceback": traceback.format_exc()
        }

# ============================================================
# ========== SINGLE TABLE PIPELINE (DIFF + MERGE) ===========
# ============================================================

def run_table_pipeline(table, pk_cols, state_manager, merge_engine, today):
    """Folded per-table STAGE-1 flow on the single `diff_merge` key:
    init-gate → diff-gate → merge-gate."""
    logging.info(f"Processing table: {table}")

    state = state_manager.load(table)
    dm = state.get("diff_merge", {})

    # ------------------ INIT ------------------
    do_init, reason = should_run_init(dm, today)
    logging.info(f"[{table}] Init Decision: {reason}")
    if do_init:
        dm = {
            "table": table,
            "extraction_datetime": _now_iso(),
            "extraction_status": "started",
            "is_merged": False,
            "extraction_Message": None,
            "merge_Message": None,
        }
        state["diff_merge"] = dm
        state_manager.save(table, state)

    # ------------------ DIFF ------------------
    if should_run_diff(dm):
        logging.info(f"[{table}] Running diff (extraction_status=started)")
        diff_result = run_diff_step(table)
        if diff_result.get("success"):
            dm["extraction_datetime"] = _now_iso()
            dm["extraction_status"]   = "success"
            dm["extraction_Message"]  = diff_result.get("message")
        else:
            # keep extraction_status='started' so it retries next run
            dm["extraction_Message"] = diff_result.get("error")
        state["diff_merge"] = dm
        state_manager.save(table, state)
    else:
        logging.info(f"[{table}] Skipping diff (extraction_status={dm.get('extraction_status')})")

    # ------------------ MERGE ------------------
    if should_run_merge(dm):
        logging.info(f"[{table}] Running merge (success & not merged)")
        merge_result = run_merge_step(table, pk_cols, merge_engine)
        if merge_result.get("success"):
            dm["is_merged"]     = True
            dm["merge_Message"] = merge_result.get("message")
        else:
            dm["merge_Message"] = merge_result.get("error")
        state["diff_merge"] = dm
        state_manager.save(table, state)
    else:
        logging.info(
            f"[{table}] Skipping merge "
            f"(extraction_status={dm.get('extraction_status')}, is_merged={dm.get('is_merged')})"
        )

# ============================================================
# ===================== LOOP ALL TABLES ======================
# ============================================================

def run_all_tables(table_csv, pk_csv, max_workers=8):

    # Load table list
    with open(table_csv, newline="") as f:
        tables = [row[0] for row in csv.reader(f)]

    # Load PK mapping
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

    state_manager = StateManager(STATE_DIRECTORY)
    today         = date.today().isoformat()

    from merge_engine import MergeEngine
    merge_engine = MergeEngine(
        db_type=DB_TYPE,
        main_conn=BASE_DB,
        diff_conn=DIFF_STAGING_DB,
        diffmerged_conn=DIFFMERGED_CONN,
        pool_size=max_workers,
    )

    def process_table(table):
        if table not in pk_map:
            logging.warning(f"No PK found for {table}. Skipping.")
            return
        run_table_pipeline(table, pk_map[table], state_manager, merge_engine, today)

    logging.info(f"Processing {len(tables)} tables with max_workers={max_workers}")

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_table, table): table for table in tables}
            for future in concurrent.futures.as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Unhandled exception for table {table}: {e}")
    finally:
        merge_engine.dispose()


def generate_summary(table_csv, state_dir):

    today = date.today().isoformat()

    summary = {
        "date": today,
        "total_tables": 0,
        "diff_success_today": 0,
        "diff_failed_today": 0,
        "merge_success_today": 0,
        "merge_failed_today": 0,
        "skipped_today": 0,
        "decryption_success_today": 0,
        "decryption_failed_today": 0,
        "decryption_skipped": 0,
        "details": []
    }

    with open(table_csv, newline="") as f:
        tables = [row[0] for row in csv.reader(f)]

    state_manager = StateManager(state_dir)

    for table in tables:

        state = state_manager.load(table)

        dm = state.get("diff_merge", {})
        status    = dm.get("extraction_status")
        is_merged = dm.get("is_merged", False)
        ext_date  = _date_of(dm.get("extraction_datetime"))

        summary["total_tables"] += 1

        if ext_date == today:
            # Extraction bucket (no terminal 'failed' status → not-success = failed)
            if status == "success":
                summary["diff_success_today"] += 1
                # Merge bucket only meaningful once extraction succeeded
                if is_merged:
                    summary["merge_success_today"] += 1
                else:
                    summary["merge_failed_today"] += 1
            else:
                summary["diff_failed_today"] += 1
        else:
            summary["skipped_today"] += 1

        decryption = state.get("diff_decryption")
        if decryption is not None:
            dec_date   = decryption.get("last_processed_date")
            dec_status = decryption.get("status")
            if dec_date == today:
                if dec_status == "success":
                    summary["decryption_success_today"] += 1
                else:
                    summary["decryption_failed_today"] += 1
            else:
                summary["decryption_skipped"] += 1

    return summary

# ============================================================
# =========================== MAIN ===========================
# ============================================================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--mode", required=True, choices=["run", "summary"])
    parser.add_argument("--tables_csv", required=True)
    parser.add_argument("--pk_csv", required=False)
    parser.add_argument("--state_dir", required=True)
    parser.add_argument("--max_workers", type=int, default=8,
                        help="Parallel threads for table processing (default: 8)")

    args = parser.parse_args()

    setup_logging()

    if args.mode == "run":
        logging.info("Starting Diff-Merge Runner (Python diff script)")

        if not args.pk_csv:
            raise ValueError("--pk_csv is required in run mode")

        run_all_tables(args.tables_csv, args.pk_csv, max_workers=args.max_workers)

        logging.info("Completed")

    elif args.mode == "summary":
        summary = generate_summary(args.tables_csv, args.state_dir)

        # IMPORTANT: Only JSON output
        print(json.dumps(summary))

if __name__ == "__main__":
    main()
