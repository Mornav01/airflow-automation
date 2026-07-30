#!/usr/bin/env python
"""
Pipeline status report (READ-ONLY).

Rolls up per-table pipeline progress across the three stage keys stored in the
per-table JSON state files — WITHOUT running any pipeline step:

    diff_merge        {STATE_DIR}/{table}.json         (extraction + merge)
    deidentification  {STATE_DIR}/{table}.json         (deid → merge → delete)
    aws_transfer      {STATE_DIR}/{table}.json         (S3 upload → delete)

cnotes tables (greenway/mind) keep their own state under {STATE_DIR}/cnotes/ and
have only deidentification + aws_transfer (no diff_merge). ecw has no cnotes, so
cnotes rows are auto-skipped when CLINICALBIN_TABLES_CSV is unset / a placeholder /
points at a missing file — the report then shows only the main tables. One script
serves ecw / greenway / mind; it is driven purely by the .env scoped table CSVs.

Writes {STATE_DIR}/status/status_report.csv (+ a timestamped copy) and prints a
readable table + a totals banner to stdout (so an Airflow task log is enough).

Usage:
    python pipeline_status_report.py
    python pipeline_status_report.py --out /tmp/status.csv --no-cnotes
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from db_config import (  # noqa: E402
    STATE_DIR,
    DIFF_MERGE_TABLES_CSV,
    DEID_POST_PROCESS_TABLES_CSV,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


# ── CSV columns ──────────────────────────────────────────────────────────────
FIELDS = [
    "scope", "table",
    "diff_merge_status", "diff_merge_extracted_at", "diff_merge_is_merged",
    "deid_status", "deid_is_merged", "deid_is_deleted",
    "deid_rows_original", "deid_rows_deid", "deid_failed_count", "deid_last_processed",
    "aws_status", "aws_last_date", "aws_is_deleted",
    "overall",
]


# ── Helpers ──────────────────────────────────────────────────────────────────
def _read_tables(path: str) -> list[str]:
    """One table name per line (column 0); missing/empty path → [] (no error).

    Matches the canonical reader used by the pipeline scripts, so a placeholder or
    absent CLINICALBIN_TABLES_CSV (the ecw case) simply yields no cnotes rows."""
    if not path or not os.path.isfile(path):
        return []
    with open(path, newline="") as f:
        return [r[0].strip() for r in csv.reader(f) if r and r[0].strip()]


def _load_state(path: str) -> dict:
    """Load a per-table JSON state file; missing/broken → {} (never write)."""
    if not os.path.isfile(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _overall(dm: dict, deid: dict, aws: dict, has_state: bool) -> str:
    """Furthest stage reached, checked most-advanced first."""
    if aws.get("is_deleted"):
        return "aws_done"
    if aws.get("transfer_status") == "success":
        return "aws_uploaded"
    if deid.get("is_deleted"):
        return "deid_deleted"
    if deid.get("is_merged"):
        return "deid_merged"
    if deid.get("deidentification_status") == "success":
        return "deid_done"
    if dm.get("is_merged"):
        return "merged"
    if dm.get("extraction_status") == "success":
        return "extracted"
    return "started" if has_state else "not_started"


def _row(scope: str, table: str, state: dict) -> dict:
    dm   = state.get("diff_merge", {}) or {}
    deid = state.get("deidentification", {}) or {}
    aws  = state.get("aws_transfer", {}) or {}
    has_state = bool(dm or deid or aws)
    return {
        "scope": scope,
        "table": table,
        "diff_merge_status":      dm.get("extraction_status", ""),
        "diff_merge_extracted_at": dm.get("extraction_datetime", ""),
        "diff_merge_is_merged":   dm.get("is_merged", ""),
        "deid_status":            deid.get("deidentification_status", ""),
        "deid_is_merged":         deid.get("is_merged", ""),
        "deid_is_deleted":        deid.get("is_deleted", ""),
        "deid_rows_original":     deid.get("row_count_original", ""),
        "deid_rows_deid":         deid.get("row_count_deid", ""),
        "deid_failed_count":      deid.get("deid_failed_count", ""),
        "deid_last_processed":    deid.get("last_processed_datetime", ""),
        "aws_status":             aws.get("transfer_status", ""),
        "aws_last_date":          aws.get("last_processed_date", ""),
        "aws_is_deleted":         aws.get("is_deleted", ""),
        "overall":                _overall(dm, deid, aws, has_state),
    }


def _dedup(names: list[str]) -> list[str]:
    seen, out = set(), []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


# ── Report ───────────────────────────────────────────────────────────────────
def build_rows(diff_merge_csv: str, post_process_csv: str,
               clinicalbin_csv: str, include_cnotes: bool) -> list[dict]:
    main_tables = _dedup(_read_tables(diff_merge_csv) + _read_tables(post_process_csv))
    rows = [_row("main", t, _load_state(os.path.join(STATE_DIR, f"{t}.json")))
            for t in main_tables]

    if include_cnotes:
        cnotes_dir = os.path.join(STATE_DIR, "cnotes")
        cnotes_tables = _read_tables(clinicalbin_csv)   # [] when unset/placeholder → ecw
        if cnotes_tables:
            rows += [_row("cnotes", t, _load_state(os.path.join(cnotes_dir, f"{t}.json")))
                     for t in cnotes_tables]
    return rows


def write_csv(rows: list[dict], out_path: str) -> str:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    return out_path


def log_table(rows: list[dict]) -> None:
    """Print a compact fixed-width view + a totals banner to the log."""
    cols = [("scope", 6), ("table", 34), ("overall", 13),
            ("deid_status", 11), ("deid_is_merged", 9), ("deid_is_deleted", 9),
            ("aws_status", 10)]
    header = "  ".join(name[:w].ljust(w) for name, w in cols)
    logger.info(header)
    logger.info("-" * len(header))
    for r in rows:
        logger.info("  ".join(str(r.get(name, ""))[:w].ljust(w) for name, w in cols))

    totals: dict[str, int] = {}
    for r in rows:
        totals[r["overall"]] = totals.get(r["overall"], 0) + 1
    summary = ", ".join(f"{k}={v}" for k, v in sorted(totals.items()))
    logger.info("-" * len(header))
    logger.info("TOTAL tables: %d  |  %s", len(rows), summary or "(none)")


def main():
    p = argparse.ArgumentParser(description="Read-only per-table pipeline status report")
    p.add_argument("--out", default=os.path.join(STATE_DIR, "status", "status_report.csv"),
                   help="Output CSV path (default: {STATE_DIR}/status/status_report.csv)")
    p.add_argument("--diff-merge-tables", default=DIFF_MERGE_TABLES_CSV,
                   help="Tables CSV for the diff_merge scope (default: env DIFF_MERGE_TABLES_CSV)")
    p.add_argument("--post-process-tables", default=DEID_POST_PROCESS_TABLES_CSV,
                   help="Tables CSV for the deid/aws scope (default: env DEID_POST_PROCESS_TABLES_CSV)")
    p.add_argument("--clinicalbin-tables", default=os.environ.get("CLINICALBIN_TABLES_CSV", ""),
                   help="cnotes tables CSV (default: env CLINICALBIN_TABLES_CSV; absent → no cnotes)")
    p.add_argument("--no-cnotes", action="store_true", help="Skip the cnotes sub-flow entirely")
    args = p.parse_args()

    rows = build_rows(args.diff_merge_tables, args.post_process_tables,
                      args.clinicalbin_tables, include_cnotes=not args.no_cnotes)

    log_table(rows)

    canonical = write_csv(rows, args.out)
    # Timestamped copy alongside the canonical file (skip if --out is elsewhere).
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stamped = os.path.join(os.path.dirname(canonical), f"status_report_{stamp}.csv")
    write_csv(rows, stamped)

    logger.info("Status report written: %s", canonical)
    logger.info("Timestamped copy:      %s", stamped)


if __name__ == "__main__":
    main()
