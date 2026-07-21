import os
import json
import csv
import argparse
import logging
from datetime import date
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
            return {
                "diff": {},
                "merge": {}
            }
        with open(path, "r") as f:
            return json.load(f)

        state.setdefault("diff", {})
        state.setdefault("merge", {})

    def save(self, table, state):
        path = self._file_path(table)
        with open(path, "w") as f:
            json.dump(state, f, indent=4)

# ============================================================
# ===================== SKIP LOGIC ===========================
# ============================================================

class SkipLogic:
    """
    All skip rules are centralized here.
    Modify here in future if needed.
    """

    def __init__(self, today):
        self.today = today

    def should_run_diff(self, state):
        diff = state.get("diff", {})
        merge = state.get("merge", {})

        diff_date = diff.get("last_processed_date")
        diff_success = diff.get("success")

        merge_date = merge.get("last_processed_date")
        merge_success = merge.get("success")

        # RULE 1: If diff already successful today → skip
        if diff_date == self.today and diff_success:
            return False, "Diff already successful today"

        # RULE 2: If diff old success AND merge old failure → skip diff
        if (
            diff_success
            and diff_date != self.today
            and merge_date != self.today
            and merge_success is False
        ):
            return False, "Waiting for merge to succeed before re-running diff"

        # RULE 3: If both diff + merge old success → run diff
        if diff_success and merge_success:
            return True, "Re-running diff as both old runs were successful"

        return True, "Diff needs execution"

    def should_run_merge(self, state):
        diff = state.get("diff", {})
        merge = state.get("merge", {})

        merge_date = merge.get("last_processed_date")
        merge_success = merge.get("success")

        diff_success = diff.get("success")

        # RULE 1: Merge already successful today → skip
        if merge_date == self.today and merge_success:
            return False, "Merge already successful today"

        # RULE 2: Diff not successful → skip merge
        if not diff_success:
            return False, "Diff not successful — cannot run merge"

        return True, "Merge needs execution"

# ============================================================
# ===================== CORE EXECUTION =======================
# ============================================================

def run_diff_step(table):
    """
    Uses the Python diff script (diff_script_in_test_duplicate_pk_handled)
    instead of the stored procedure.
    Schema is hardcoded as 'dbo'.
    """
    import sys
    import os

    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    from diff_script_duplicate_pk_handled import run_diff

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


def run_merge_step(table, pk_cols):
    from merge_engine import MergeEngine

    engine = MergeEngine(
        db_type=DB_TYPE,
        main_conn=BASE_DB,
        diff_conn=DIFF_STAGING_DB,
        diffmerged_conn=DIFFMERGED_CONN
    )

    try:
        result = engine.run_table(
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

def run_table_pipeline(table, pk_cols, state_manager, skip_logic):

    logging.info(f"Processing table: {table}")

    state = state_manager.load(table)
    today = date.today().isoformat()

    # ------------------ DIFF ------------------
    run_diff, reason = skip_logic.should_run_diff(state)
    logging.info(f"Diff Decision: {reason}")

    if run_diff:
        diff_result = run_diff_step(table)
        state["diff"] = {
            "last_processed_date": today,
            **diff_result
        }
        state_manager.save(table, state)
    else:
        logging.info(f"Skipping diff for {table}")

    # Reload state after diff
    state = state_manager.load(table)

    # ------------------ MERGE ------------------
    run_merge, reason = skip_logic.should_run_merge(state)
    logging.info(f"Merge Decision: {reason}")

    if run_merge:
        merge_result = run_merge_step(table, pk_cols)
        state["merge"] = {
            "last_processed_date": today,
            **merge_result
        }
        state_manager.save(table, state)
    else:
        logging.info(f"Skipping merge for {table}")

# ============================================================
# ===================== LOOP ALL TABLES ======================
# ============================================================

def run_all_tables(table_csv, pk_csv):

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
    skip_logic = SkipLogic(date.today().isoformat())

    for table in tables:
        pk_cols = pk_map.get(table)
        if table not in pk_map:
            logging.warning(f"No PK found for {table}. Skipping.")
            continue

        run_table_pipeline(table, pk_cols, state_manager, skip_logic)


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

        diff = state.get("diff", {})
        merge = state.get("merge", {})

        diff_date = diff.get("last_processed_date")
        diff_success = diff.get("success")

        merge_date = merge.get("last_processed_date")
        merge_success = merge.get("success")

        summary["total_tables"] += 1

        if diff_date == today:
            if diff_success:
                summary["diff_success_today"] += 1
            else:
                summary["diff_failed_today"] += 1

        if merge_date == today:
            if merge_success:
                summary["merge_success_today"] += 1
            else:
                summary["merge_failed_today"] += 1

        if diff_date != today and merge_date != today:
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

    args = parser.parse_args()

    setup_logging()

    if args.mode == "run":
        logging.info("Starting Diff-Merge Runner (Python diff script)")

        if not args.pk_csv:
            raise ValueError("--pk_csv is required in run mode")

        run_all_tables(args.tables_csv, args.pk_csv)

        logging.info("Completed")

    elif args.mode == "summary":
        summary = generate_summary(args.tables_csv, args.state_dir)

        # IMPORTANT: Only JSON output
        print(json.dumps(summary))

if __name__ == "__main__":
    main()
