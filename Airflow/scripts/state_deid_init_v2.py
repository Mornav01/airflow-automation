#!/usr/bin/env python
"""
DAG 1 (v2) — Task 3: Deidentification State Init

Runs BEFORE `deid run` to initialize the JSON state files for each table.
Equivalent to the state-writing portion of state_deidentification_run_polar.py.

For each table in the CSV, init is CONDITIONAL (resumable per-table cycle):
  - Reset the deidentification section UNLESS the previous cycle is stuck
    between merge and delete, i.e. skip only when:
        deidentification_status == "success" AND is_merged AND NOT is_deleted
  - On reset:
      deidentification_status: "started"
      last_processed_datetime: now (full ISO timestamp)
      is_merged:               False
      is_deleted:              False
  - Otherwise (merged but not yet deleted) LEAVE the block untouched so the
    delete step can resume the in-progress cycle on the next run.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import logging
import os
import sys
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from db_config import STATE_DIR  # noqa: E402  # type: ignore[import-untyped]


# ============================================================
# ===================== LOGGING ==============================
# ============================================================

def setup_logging(state_dir: str):
    log_dir = os.path.join(state_dir, "deid_init_v2")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f"deid_init_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
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

    def __init__(self, state_dir: str):
        self.state_dir = state_dir
        os.makedirs(self.state_dir, exist_ok=True)

    def _file_path(self, table: str) -> str:
        return os.path.join(self.state_dir, f"{table}.json")

    def load(self, table: str) -> dict:
        path = self._file_path(table)
        if not os.path.exists(path):
            return {"deidentification": {}}
        with open(path) as f:
            state = json.load(f)
        # Own only the deidentification key; preserve everything else already in
        # the file (e.g. STAGE-1's 'diff_merge').
        state.setdefault("deidentification", {})
        return state

    def save(self, table: str, state: dict):
        path = self._file_path(table)
        with open(path, "w") as f:
            json.dump(state, f, indent=4)


# ============================================================
# ===================== CORE LOGIC ===========================
# ============================================================

def _should_init(deid: dict) -> bool:
    """Init unless the previous cycle is stuck between merge and delete.

    Run when: status != 'success'  (fresh / started / retry), OR
              status == 'success' AND NOT (is_merged AND NOT is_deleted).
    Skip only when: status == 'success' AND is_merged AND NOT is_deleted —
    merged but not yet deleted, so the delete step can resume it.
    """
    if deid.get("deidentification_status") != "success":
        return True
    is_merged  = bool(deid.get("is_merged"))
    is_deleted = bool(deid.get("is_deleted"))
    return not (is_merged and not is_deleted)


def init_table_state(table_name: str, state_manager: StateManager):
    state = state_manager.load(table_name)
    deid  = state.get("deidentification", {})

    if not _should_init(deid):
        logging.info(
            "[%s] mid-cycle (status=success, is_merged=%s, is_deleted=%s) → leaving state as-is",
            table_name, deid.get("is_merged"), deid.get("is_deleted"),
        )
        return

    state["deidentification"] = {
        "last_processed_datetime": datetime.now().isoformat(),
        "deidentification_status": "started",
        "is_merged": False,
        "is_deleted": False,
    }

    state_manager.save(table_name, state)
    logging.info("[%s] state initialized → deidentification_status=started", table_name)


# ============================================================
# =========================== MAIN ===========================
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables-filepath", required=True, help="Path to tables CSV")
    parser.add_argument("--max-workers",     type=int, default=16,
                        help="Parallel threads for state file writes (default: 16)")
    args = parser.parse_args()

    setup_logging(STATE_DIR)

    with open(args.tables_filepath, newline="") as f:
        tables = [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]

    logging.info("Initializing deid state for %d tables with max_workers=%d",
                 len(tables), args.max_workers)

    state_manager = StateManager(STATE_DIR)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(init_table_state, table, state_manager): table
                   for table in tables}
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception:
                table = futures[future]
                logging.exception("[%s] unexpected error during state init", table)

    logging.info("State init complete")


if __name__ == "__main__":
    main()
