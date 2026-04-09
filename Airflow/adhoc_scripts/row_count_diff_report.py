#!/usr/bin/env python
"""
Row Count Diff Report
---------------------
Reads arizona_row_count_stats CSV and produces a report comparing:
  - mssql vs mysql row count difference
  - ignored rows (base + incremental) stored in IgnoreRowsDeIdentificaiton
  - whether the diff matches the total ignored rows
"""

import csv
import logging
import os
import sys
from datetime import datetime

# ============================================================
# ===================== DJANGO SETUP =========================
# ============================================================

BASE_REPO_DIR = "/Users/ndaicnsarizona/Desktop/deidentification_polars_2/deidentification_v2/"
DJANGO_PROJECT_DIR = os.path.join(BASE_REPO_DIR, "deIdentification")
sys.path.insert(0, DJANGO_PROJECT_DIR)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

import django
django.setup()

from nd_api.models import DbDetailsModel, IgnoreRowsDeIdentificaiton

# ============================================================
# ===================== CONFIG ===============================
# ============================================================

INPUT_CSV = os.path.join(
    os.path.dirname(__file__),
    "row_count_stats_20260406_043114.csv",
)

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_CSV = os.path.join(
    os.path.dirname(__file__),
    f"row_count_diff_report_{TIMESTAMP}.csv",
)

BASE_DB_NAME        = "arizona_mobiledoc_base"
INCREMENTAL_DB_NAME = "arizona_mobiledoc_incremental"

# ============================================================
# ===================== LOGGING ==============================
# ============================================================

LOG_FILE = os.path.join(
    os.path.dirname(__file__),
    f"row_count_diff_report_{TIMESTAMP}.log",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def get_ignored_row_count(db_name: str, table_name: str) -> int:
    """Return the number of ignored rows stored for a given db/table."""
    logger.debug(f"  Querying IgnoreRowsDeIdentificaiton: db={db_name}, table={table_name}")
    return IgnoreRowsDeIdentificaiton.objects.filter(
        db_name=db_name, table_name=table_name
    ).count()


def main():
    logger.info("Starting row count diff report")
    logger.info(f"Input CSV  : {INPUT_CSV}")
    logger.info(f"Output CSV : {OUTPUT_CSV}")
    logger.info(f"Log file   : {LOG_FILE}")

    rows_out = []
    mismatches = []

    logger.info("Reading input CSV and querying ignored rows...")

    with open(INPUT_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            table      = row["table"]
            mssql_raw  = row["mssql_count"]
            mysql_raw  = row["mysql_count"]

            logger.info(f"[{table}] mssql={mssql_raw}, mysql={mysql_raw}")

            # Skip rows where either count is not a valid integer (e.g. ERROR)
            try:
                mssql_count = int(mssql_raw)
                mysql_count = int(mysql_raw)
            except ValueError:
                logger.warning(f"[{table}] Skipping — non-integer count (mssql={mssql_raw}, mysql={mysql_raw})")
                rows_out.append({
                    "table":              table,
                    "mssql_count":        mssql_raw,
                    "mysql_count":        mysql_raw,
                    "diff":               "N/A",
                    "ignored_base":       "N/A",
                    "ignored_incremental":"N/A",
                    "total_ignored":      "N/A",
                    "diff_matches_ignored": "N/A",
                })
                continue

            diff = mssql_count - mysql_count

            ignored_base        = get_ignored_row_count(BASE_DB_NAME, table)
            ignored_incremental = get_ignored_row_count(INCREMENTAL_DB_NAME, table)
            total_ignored       = ignored_base + ignored_incremental

            matches = diff == total_ignored
            if not matches and diff != 0:
                mismatches.append((table, diff, total_ignored))
                logger.warning(
                    f"[{table}] MISMATCH — diff={diff}, total_ignored={total_ignored}, gap={diff - total_ignored}"
                )
            else:
                logger.info(
                    f"[{table}] diff={diff}, ignored_base={ignored_base}, "
                    f"ignored_incremental={ignored_incremental}, total_ignored={total_ignored} -> {'OK' if matches else 'diff=0'}"
                )

            rows_out.append({
                "table":               table,
                "mssql_count":         mssql_count,
                "mysql_count":         mysql_count,
                "diff":                diff,
                "ignored_base":        ignored_base,
                "ignored_incremental": ignored_incremental,
                "total_ignored":       total_ignored,
                "diff_matches_ignored": "YES" if matches else "NO",
            })

    # Write output CSV
    logger.info(f"Writing {len(rows_out)} rows to output CSV...")
    fieldnames = [
        "table",
        "mssql_count",
        "mysql_count",
        "diff",
        "ignored_base",
        "ignored_incremental",
        "total_ignored",
        "diff_matches_ignored",
    ]

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    logger.info(f"Report written to: {OUTPUT_CSV}")

    # Summary
    total_tables    = len(rows_out)
    matched_tables  = sum(1 for r in rows_out if r["diff_matches_ignored"] == "YES")
    mismatch_tables = sum(1 for r in rows_out if r["diff_matches_ignored"] == "NO")
    skipped_tables  = sum(1 for r in rows_out if r["diff_matches_ignored"] == "N/A")

    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Total tables checked : {total_tables}")
    logger.info(f"  Diff matches ignored : {matched_tables}")
    logger.info(f"  Diff != ignored rows : {mismatch_tables}")
    logger.info(f"  Skipped (bad data)   : {skipped_tables}")

    if mismatches:
        logger.warning("Tables where diff does NOT match total ignored rows:")
        logger.warning(f"  {'Table':<35} {'Diff':>10} {'Total Ignored':>15} {'Gap':>10}")
        logger.warning(f"  {'-'*35} {'-'*10} {'-'*15} {'-'*10}")
        for table, diff, total_ignored in mismatches:
            gap = diff - total_ignored
            logger.warning(f"  {table:<35} {diff:>10} {total_ignored:>15} {gap:>10}")
    else:
        logger.info("All tables with non-zero diff are fully accounted for by ignored rows.")

    logger.info("=" * 60)
    logger.info("Done.")


if __name__ == "__main__":
    main()
