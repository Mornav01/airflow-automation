#!/usr/bin/env python
"""
Ignore-Rows Reconciliation — master runner.

For each table (given via --tables_csv), runs ignore_rows_fill.process_table()
then ignore_rows_merge.process_table() in sequence, across a thread pool —
same architecture as state_diff_merge_v3.py. One table's failure is logged and
skipped; it never blocks other tables.

No per-table JSON state file is used — IGNORE_ROWS_DB_NAME (with its
ndig_status tracking column) is itself the durable state for this feature.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import logging
import os
import sys
import traceback
from datetime import date

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from db_config import STATE_DIR  # noqa: E402
import ignore_rows_fill  # noqa: E402
import ignore_rows_merge  # noqa: E402


# ============================================================
# ===================== LOGGING SETUP =========================
# ============================================================

def setup_logging():
    log_dir = os.path.join(STATE_DIR, "ignore_rows")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"ignore_rows_{date.today().isoformat()}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )


# ============================================================
# ===================== SINGLE TABLE ==========================
# ============================================================

def process_table(table: str) -> None:
    logging.info("[%s] ignore-rows fill", table)
    ignore_rows_fill.process_table(table)
    logging.info("[%s] ignore-rows merge", table)
    ignore_rows_merge.process_table(table)


# ============================================================
# ===================== LOOP ALL TABLES =======================
# ============================================================

def _read_tables(path: str) -> list:
    with open(path, newline="") as f:
        return [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]


def run_all_tables(tables_csv: str, max_workers: int = 8) -> None:
    tables = _read_tables(tables_csv)
    logging.info("Processing %d table(s) with max_workers=%d", len(tables), max_workers)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_table, table): table for table in tables}
        for future in concurrent.futures.as_completed(futures):
            table = futures[future]
            try:
                future.result()
            except Exception:
                logging.error("[%s] unhandled exception:\n%s", table, traceback.format_exc())


# ============================================================
# =========================== MAIN ============================
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Ignore-rows reconciliation — master runner")
    parser.add_argument("--tables_csv", required=True)
    parser.add_argument("--max_workers", type=int, default=8)
    args = parser.parse_args()

    setup_logging()
    logging.info("Starting Ignore-Rows Reconciliation")
    run_all_tables(args.tables_csv, max_workers=args.max_workers)
    logging.info("Ignore-Rows Reconciliation complete")


if __name__ == "__main__":
    main()
