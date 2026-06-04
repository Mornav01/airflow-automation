import os
import sys
import json
import csv
import argparse
import logging
import traceback
from datetime import date

# ── Path setup ──────────────────────────────────────────────────
# scripts_dir  → lets us import db_config, merge_engine, mssql_mysql_sync
# project_root → lets mssql_mysql_sync do "from scripts.connection_config import …"
_scripts_dir  = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_scripts_dir)
for _p in (_scripts_dir, _project_root):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from db_config import (
    STATE_DIR,                   # shared state directory
    HISTORICAL_DATA_DEFAULT_DATE, # merge default date
    PROD_DB,                     # MSSQL source
    BASE_DB,                     # MySQL main  (centricityps_original_16oct)
    DIFF_STAGING_DB,             # MySQL diff staging → incremental landing zone
    DIFFMERGED_CONN,             # MySQL merged archive
)

# Schema names derived from the connection strings (last path component)
_INCREMENTAL_SCHEMA = DIFF_STAGING_DB.rsplit("/", 1)[-1]   # centricityps_diff_staging
_ORIGINAL_SCHEMA    = BASE_DB.rsplit("/", 1)[-1]            # centricityps_original_16oct
_TRACKING_SCHEMA    = "master"                              # sync timestamp tracking table

# ============================================================
# ===================== LOGGING SETUP ========================
# ============================================================

def setup_logging(state_dir):
    os.makedirs(state_dir, exist_ok=True)
    runner_dir = os.path.join(state_dir, "runner")
    os.makedirs(runner_dir, exist_ok=True)

    log_file = os.path.join(runner_dir, f"runner_{date.today().isoformat()}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )

# ============================================================
# ===================== ENGINE BUILDERS ======================
# ============================================================

def _make_mssql_engine():
    from sqlalchemy import create_engine
    return create_engine(PROD_DB, pool_pre_ping=True, pool_size=5, max_overflow=10)


def _make_mysql_engine(conn_str):
    from sqlalchemy import create_engine
    return create_engine(conn_str, pool_pre_ping=True, pool_recycle=1800)

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
            return {"sync": {}, "merge": {}}
        with open(path) as f:
            return json.load(f)

    def save(self, table, state):
        path = self._file_path(table)
        with open(path, "w") as f:
            json.dump(state, f, indent=4)

# ============================================================
# ===================== SKIP LOGIC ===========================
# ============================================================

class SkipLogic:

    def __init__(self, today):
        self.today = today

    def should_run_sync(self, state):
        sync  = state.get("sync",  {})
        merge = state.get("merge", {})

        d             = sync.get("last_processed_date")
        sync_success  = sync.get("success")
        merge_date    = merge.get("last_processed_date")
        merge_success = merge.get("success")

        # RULE 1: Sync already successful on or after run_date → skip
        if d and d >= self.today and sync_success:
            return False, "Sync already successful on or after run_date — skipping"

        # RULE 2: Sync old success + merge old failure → wait for merge to be fixed
        # Prevents overwriting diff_staging before the previous merge has been retried.
        # Operator should trigger with run_date=<past_date> to retry merge only.
        if (
            sync_success
            and (d is None or d < self.today)
            and (merge_date is None or merge_date < self.today)
            and merge_success is False
        ):
            return False, "Waiting for merge to succeed before re-running sync"

        return True, "Sync needs execution"

    def should_run_merge(self, state):
        sync  = state.get("sync",  {})
        merge = state.get("merge", {})

        merge_date    = merge.get("last_processed_date")
        merge_success = merge.get("success")

        # RULE 1: Merge already successful on or after run_date → skip
        if merge_date and merge_date >= self.today and merge_success:
            return False, "Merge already successful on or after run_date — skipping"

        # RULE 2: Sync has never succeeded → diff_staging is empty → skip
        # (sync must have put data into diff_staging at some point)
        if not sync.get("success"):
            return False, "Sync has not succeeded — no data to merge"

        return True, "Merge needs execution"

# ============================================================
# ===================== CORE STEPS ===========================
# ============================================================

def run_sync_step(table):
    from mssql_mysql_sync import (
        ensure_incremental_db,
        ensure_tracking_table,
        process_table,
        add_nd_auto_increment_id_step,
    )

    eng_mssql = _make_mssql_engine()
    eng_mysql  = _make_mysql_engine(DIFF_STAGING_DB)

    try:
        ensure_incremental_db(eng_mysql, incremental_schema=_INCREMENTAL_SCHEMA)
        ensure_tracking_table(eng_mysql, tracking_schema=_TRACKING_SCHEMA)

        rows = process_table(
            eng_mssql,
            eng_mysql,
            table,
            incremental_schema=_INCREMENTAL_SCHEMA,
            tracking_schema=_TRACKING_SCHEMA,
            log_fn=logging.info,
        )

        add_nd_auto_increment_id_step(
            eng_mysql,
            [table],
            original_schema=_ORIGINAL_SCHEMA,
            incremental_schema=_INCREMENTAL_SCHEMA,
            log_fn=logging.info,
        )

        return {"success": True, "table": table, "rows_synced": rows}

    except Exception as e:
        tb = traceback.format_exc()
        logging.error(f"Sync failed for table '{table}': {e}\n{tb}")
        return {"success": False, "table": table, "error": str(e), "traceback": tb}

    finally:
        eng_mssql.dispose()
        eng_mysql.dispose()


def run_merge_step(table, pk_cols):
    from merge_engine import MergeEngine

    engine = MergeEngine(
        db_type="mysql",
        main_conn=BASE_DB,
        diff_conn=DIFF_STAGING_DB,
        diffmerged_conn=DIFFMERGED_CONN,
    )

    try:
        result = engine.run_table(
            table=table,
            pk_cols=pk_cols,
            default_date=HISTORICAL_DATA_DEFAULT_DATE,
        )
        logging.info(str(result))
        return result

    except Exception as e:
        return {
            "success": False,
            "table": table,
            "error": str(e),
            "traceback": traceback.format_exc(),
        }

# ============================================================
# ========= SINGLE TABLE PIPELINE (SYNC or MERGE) ===========
# ============================================================

def run_sync_pipeline(table, state_manager, skip_logic, run_date):
    logging.info(f"Processing sync for table: {table}")

    state = state_manager.load(table)

    should_run, reason = skip_logic.should_run_sync(state)
    logging.info(f"Sync Decision: {reason}")

    if not should_run:
        logging.info(f"Skipping sync for {table}")
        return

    result = run_sync_step(table)
    state["sync"] = {"last_processed_date": run_date, **result}
    state_manager.save(table, state)


def run_merge_pipeline(table, pk_cols, state_manager, skip_logic, run_date):
    logging.info(f"Processing merge for table: {table}")

    state = state_manager.load(table)

    should_run, reason = skip_logic.should_run_merge(state)
    logging.info(f"Merge Decision: {reason}")

    if not should_run:
        logging.info(f"Skipping merge for {table}: {reason}")
        return

    result = run_merge_step(table, pk_cols)
    state["merge"] = {"last_processed_date": run_date, **result}
    state_manager.save(table, state)

# ============================================================
# ===================== LOOP ALL TABLES ======================
# ============================================================

def run_all_tables(mode, tables_csv, pk_csv, state_dir, run_date):
    with open(tables_csv, newline="") as f:
        tables = [row[0] for row in csv.reader(f) if row]

    pk_map = {}
    if pk_csv:
        with open(pk_csv, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pk_val = row.get("primary_key", "").strip()
                pk_map[row["table_name"]] = (
                    [c.strip() for c in pk_val.split(",") if c.strip()]
                    if pk_val else []
                )

    state_manager = StateManager(state_dir)
    skip_logic    = SkipLogic(run_date)

    logging.info(f"run_date: {run_date}")

    if mode == "run":
        for table in tables:
            run_sync_pipeline(table, state_manager, skip_logic, run_date)

    elif mode == "merge":
        for table in tables:
            if table not in pk_map:
                logging.warning(f"No PK found for {table}. Skipping merge.")
                continue
            run_merge_pipeline(table, pk_map[table], state_manager, skip_logic, run_date)


def generate_summary(tables_csv, state_dir, run_date):
    summary = {
        "date": run_date,
        "total_tables": 0,
        "sync_success_today":  0,
        "sync_failed_today":   0,
        "merge_success_today": 0,
        "merge_failed_today":  0,
        "skipped_today":       0,
        "details": [],
    }

    with open(tables_csv, newline="") as f:
        tables = [row[0] for row in csv.reader(f) if row]

    state_manager = StateManager(state_dir)

    for table in tables:
        state = state_manager.load(table)
        sync  = state.get("sync",  {})
        merge = state.get("merge", {})

        sync_date     = sync.get("last_processed_date")
        sync_success  = sync.get("success")
        merge_date    = merge.get("last_processed_date")
        merge_success = merge.get("success")

        summary["total_tables"] += 1
        ran_today = False

        if sync_date == run_date:
            ran_today = True
            if sync_success:
                summary["sync_success_today"] += 1
            else:
                summary["sync_failed_today"] += 1

        if merge_date == run_date:
            ran_today = True
            if merge_success:
                summary["merge_success_today"] += 1
            else:
                summary["merge_failed_today"] += 1

        if not ran_today:
            summary["skipped_today"] += 1

        summary["details"].append({
            "table":        table,
            "sync":         "success" if (sync_success  and sync_date  == run_date) else ("failed" if sync_date  == run_date else "skipped"),
            "sync_rows":    sync.get("rows_synced", 0),
            "merge":        "success" if (merge_success and merge_date == run_date) else ("failed" if merge_date == run_date else "skipped"),
            "updated_rows": merge.get("updated_rows", 0),
            "new_rows":     merge.get("new_rows", 0),
        })

    return summary

# ============================================================
# =========================== MAIN ===========================
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",       required=True, choices=["run", "merge", "summary"])
    parser.add_argument("--tables_csv", required=True)
    parser.add_argument("--pk_csv",     required=False)
    parser.add_argument("--state_dir",  required=True)
    parser.add_argument("--run_date",   default=None,
                        help="Date to process (YYYY-MM-DD). Defaults to today.")
    args = parser.parse_args()

    run_date = args.run_date or date.today().isoformat()

    if args.mode == "summary":
        summary = generate_summary(args.tables_csv, args.state_dir, run_date)
        # IMPORTANT: stdout must be pure JSON — DAG parses this
        print(json.dumps(summary))
        return

    setup_logging(args.state_dir)

    if args.mode == "run":
        logging.info(f"Starting Sync Runner (run_date={run_date})")
        run_all_tables("run", args.tables_csv, args.pk_csv, args.state_dir, run_date)
        logging.info("Sync complete")

    elif args.mode == "merge":
        if not args.pk_csv:
            raise ValueError("--pk_csv is required in merge mode")
        logging.info(f"Starting Merge Runner (run_date={run_date})")
        run_all_tables("merge", args.tables_csv, args.pk_csv, args.state_dir, run_date)
        logging.info("Merge complete")


if __name__ == "__main__":
    main()
