#!/usr/bin/env python
"""
Manual Merge Script

State is tracked in a CSV file


CSV columns:
    table_name, status, updated_rows, new_rows, diffmerged_rows, message, timestamp

Skips tables where status = 'success'.
Creates the CSV if it doesn't exist.

Usage:
    python manual_merge_script.py \
        --tables_filepath /path/to/tables.csv \
        --pk_csv          /path/to/primary_keys.csv \
        --status_csv      /path/to/merge_status.csv
"""

import argparse
import csv
import logging
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# ── Load manual.env (shared config for all manual scripts) ────────────────────
load_dotenv(Path(__file__).parent.parent / "manual.env")

# ============================================================
# ===================== CONFIG ===============================
# ============================================================

DB_TYPE = os.environ["MERGE_DB_TYPE"]

DEID_MAIN_CONN         = os.environ["MERGE_MAIN_CONN"]
DEID_DIFF_STAGING_CONN = os.environ["MERGE_DIFF_STAGING_CONN"]
DEID_DIFF_MERGED_CONN  = os.environ["MERGE_DIFF_MERGED_CONN"]

HISTORICAL_DATA_DEFAULT_DATE = os.environ["MERGE_DEFAULT_DATE"]

DEFAULT_PK_CSV     = os.environ["MERGE_PK_CSV"]
DEFAULT_STATUS_CSV = os.environ["MERGE_STATUS_CSV"]

STATUS_CSV_FIELDS = ["table_name", "status", "updated_rows", "new_rows", "diffmerged_rows", "message", "timestamp"]

# ============================================================
# ===================== LOGGING ==============================
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ============================================================
# ===================== CSV STATE ============================
# ============================================================

def load_status_csv(path: str) -> dict:
    """Load status CSV into a dict keyed by table_name. Creates file if missing."""
    if not os.path.exists(path):
        logging.info("Status CSV not found, will create: %s", path)
        return {}

    status = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            status[row["table_name"]] = row
    logging.info("Loaded status for %d tables from %s", len(status), path)
    return status


def save_status_csv(path: str, status: dict) -> None:
    """Write full status dict back to CSV."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=STATUS_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(status.values())


def update_status(status: dict, table: str, result: dict) -> None:
    """Update a single table's row in the status dict."""
    if result.get("success"):
        status[table] = {
            "table_name":    table,
            "status":        "success",
            "updated_rows":  result.get("updated_rows", 0),
            "new_rows":      result.get("new_rows", 0),
            "diffmerged_rows": result.get("diffMerged_inserted_rows", 0),
            "message":       result.get("message", ""),
            "timestamp":     datetime.now().isoformat(),
        }
    else:
        status[table] = {
            "table_name":    table,
            "status":        "failed",
            "updated_rows":  "",
            "new_rows":      "",
            "diffmerged_rows": "",
            "message":       result.get("error", ""),
            "timestamp":     datetime.now().isoformat(),
        }


# ============================================================
# ===================== PK LOADER ============================
# ============================================================

def load_pk_map(pk_csv: str) -> dict:
    pk_map = {}
    with open(pk_csv, newline="") as f:
        for row in csv.DictReader(f):
            pk_value = row["primary_key"]
            pk_map[row["table_name"]] = (
                [col.strip() for col in pk_value.split(",") if col.strip()]
                if pk_value else []
            )
    return pk_map


# ============================================================
# ===================== MERGE STEP ===========================
# ============================================================

def run_merge_step(table: str, pk_cols: list) -> dict:
    from merge_engine import MergeEngine

    engine = MergeEngine(
        db_type=DB_TYPE,
        main_conn=DEID_MAIN_CONN,
        diff_conn=DEID_DIFF_STAGING_CONN,
        diffmerged_conn=DEID_DIFF_MERGED_CONN,
    )

    try:
        result = engine.run_table(
            table=table,
            pk_cols=pk_cols,
            default_date=HISTORICAL_DATA_DEFAULT_DATE,
        )
        return result
    except Exception as e:
        return {
            "success":   False,
            "table":     table,
            "error":     str(e),
            "traceback": traceback.format_exc(),
        }


# ============================================================
# =========================== MAIN ===========================
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables_filepath", required=True)
    parser.add_argument("--pk_csv",          default=DEFAULT_PK_CSV)
    parser.add_argument("--status_csv",      default=DEFAULT_STATUS_CSV)
    args = parser.parse_args()

    pk_map = load_pk_map(args.pk_csv)
    status = load_status_csv(args.status_csv)

    with open(args.tables_filepath, newline="") as f:
        tables = [row[0].strip() for row in csv.reader(f)]

    logging.info("Processing %d tables", len(tables))

    for table in tables:
        existing = status.get(table, {})

        if existing.get("status") == "success":
            logging.info("[%s] already succeeded → skip", table)
            continue

        if table not in pk_map:
            logging.warning("[%s] no PK found, skipping", table)
            continue

        logging.info("[%s] starting merge", table)
        result = run_merge_step(table, pk_map[table])

        if result.get("success"):
            logging.info(
                "[%s] merge succeeded → updated=%s new=%s diffMerged=%s",
                table,
                result.get("updated_rows", 0),
                result.get("new_rows", 0),
                result.get("diffMerged_inserted_rows", 0),
            )
        else:
            logging.error("[%s] merge failed: %s", table, result.get("error"))

        update_status(status, table, result)
        save_status_csv(args.status_csv, status)   # save after each table

    logging.info("Done. Status saved to %s", args.status_csv)


if __name__ == "__main__":
    main()
