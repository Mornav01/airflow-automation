#!/usr/bin/env python
"""
Row Count + Ignored Rows Report
--------------------------------
For each table:
  - mssql_count   : COUNT(*) from MSSQL source
  - mysql_count   : COUNT(*) from MySQL deidentified
  - diff          : mssql - mysql
  - ignored_rows  : failed rows from failed_rows.db for this config_key
  - match         : YES if diff == ignored_rows, NO otherwise

config_key and failed_rows_db_path are read from the deid config yaml.
MSSQL/MySQL connections and tables CSV are loaded from Airflow/manual.env.

Usage:
    python row_count_ignored_report.py
    python row_count_ignored_report.py --tables /path/to/tables.csv
    python row_count_ignored_report.py --config /path/to/config_incremental.yaml
"""

import argparse
import csv
import logging
import os
import sqlite3
import sys

from datetime import datetime
from pathlib import Path

import sqlalchemy
import yaml
from dotenv import load_dotenv

# ── Load manual.env ───────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent.parent / "manual.env")

# ── Config from manual.env ────────────────────────────────────────────────────
MSSQL_CONN         = os.environ["REPORT_MSSQL_CONN"]
MYSQL_CONN         = os.environ["REPORT_MYSQL_CONN"]
DEFAULT_TABLES_CSV = os.environ["REPORT_TABLES_CSV"]
STATE_DIR          = os.environ["REPORT_STATE_DIR"]
DEFAULT_CONFIG     = os.environ["REPORT_DEID_CONFIG"]

TIMESTAMP   = datetime.now().strftime("%Y%m%d_%H%M%S")
SUMMARY_DIR = Path(STATE_DIR) / "summary"
SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV  = SUMMARY_DIR / f"row_count_ignored_report_{TIMESTAMP}.csv"
LOG_FILE    = SUMMARY_DIR / f"row_count_ignored_report_{TIMESTAMP}.log"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_tables(csv_path: str) -> list[str]:
    tables = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for i, row in enumerate(csv.reader(f)):
            if not row:
                continue
            val = row[0].strip()
            if not val:
                continue
            if i == 0 and val.lower() in ("table", "table_name", "tablename", "name"):
                continue
            tables.append(val)
    return tables


def get_mssql_count(engine, table: str) -> int | None:
    try:
        with engine.connect() as conn:
            return conn.execute(sqlalchemy.text(f"SELECT COUNT(*) FROM [{table}]")).scalar()
    except Exception:
        return None


def get_mysql_count(engine, table: str) -> int | None:
    try:
        with engine.connect() as conn:
            return conn.execute(sqlalchemy.text(f"SELECT COUNT(*) FROM `{table}`")).scalar()
    except Exception:
        return None


def _source_db_name(conn_str: str) -> str:
    """Extract the database name from a SQLAlchemy connection string."""
    # e.g. mssql+pyodbc://user:pw@host:port/mobiledoc_base_0601?driver=...
    path = conn_str.split("@")[-1]          # host:port/db?params
    db   = path.split("/")[-1]              # db?params
    return db.split("?")[0]                 # db


def load_ignored_rows(db_path: str, config_key: str, source_conn_str: str) -> dict[str, int]:
    """
    Query failed_rows_{source_db_name} in failed_rows.db and return a dict of
    {table_name: count} for the given config_key.
    """
    source_db  = _source_db_name(source_conn_str)
    failed_tbl = f"failed_rows_{source_db}"

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(f"""
            SELECT table_name, COUNT(*) AS cnt
            FROM [{failed_tbl}]
            WHERE config_key = ?
            GROUP BY table_name
        """, (config_key,)).fetchall()
        result = {table_name: cnt for table_name, cnt in rows}
    except sqlite3.OperationalError as e:
        logger.warning("[%s] could not query: %s", failed_tbl, e)
        result = {}
    finally:
        conn.close()

    logger.info("queried table: %s  (%d table entries with failures)", failed_tbl, len(result))
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", default=DEFAULT_TABLES_CSV)
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="Path to deid config yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    config_key   = cfg["config_key"]
    db_path      = cfg.get("failed_rows_db_path")

    if not db_path:
        logger.error("failed_rows_db_path not set in %s", args.config)
        sys.exit(1)

    tables = load_tables(args.tables)
    if not tables:
        logger.error("No tables found in %s", args.tables)
        sys.exit(1)

    logger.info("tables      : %d  (%s)", len(tables), args.tables)
    logger.info("mssql       : %s", MSSQL_CONN.split("@")[-1])
    logger.info("mysql       : %s", MYSQL_CONN.split("@")[-1])
    logger.info("config_key  : %s", config_key)
    logger.info("failed rows : %s", db_path)

    mssql_engine = sqlalchemy.create_engine(MSSQL_CONN)
    mysql_engine = sqlalchemy.create_engine(MYSQL_CONN)

    logger.info("loading ignored rows from failed_rows.db...")
    ignored_map = load_ignored_rows(db_path, config_key, MSSQL_CONN)
    logger.info("ignored rows loaded for %d tables", len(ignored_map))

    rows_out   = []
    mismatches = []

    for table in tables:
        mssql_count = get_mssql_count(mssql_engine, table)
        mysql_count = get_mysql_count(mysql_engine, table)
        ignored     = ignored_map.get(table, 0)

        if mssql_count is None or mysql_count is None:
            logger.warning("[%s] ERROR  mssql=%s  mysql=%s", table, mssql_count, mysql_count)
            rows_out.append({
                "table":         table,
                "mssql_count":   mssql_count if mssql_count is not None else "ERROR",
                "mysql_count":   mysql_count if mysql_count is not None else "ERROR",
                "diff":          "N/A",
                "ignored_rows":  ignored,
                "match":         "ERROR",
            })
            continue

        diff  = mssql_count - mysql_count
        match = diff == ignored

        if not match and diff != 0:
            mismatches.append((table, diff, ignored))
            logger.warning(
                "[%s] MISMATCH  mssql=%-8d  mysql=%-8d  diff=%-6d  ignored=%-6d  gap=%d",
                table, mssql_count, mysql_count, diff, ignored, diff - ignored,
            )
        else:
            logger.info(
                "[%s] %-6s  mssql=%-8d  mysql=%-8d  diff=%-6d  ignored=%d",
                table, "OK" if match else "diff=0", mssql_count, mysql_count, diff, ignored,
            )

        rows_out.append({
            "table":        table,
            "mssql_count":  mssql_count,
            "mysql_count":  mysql_count,
            "diff":         diff,
            "ignored_rows": ignored,
            "match":        "YES" if match else "NO",
        })

    # ── Write CSV ─────────────────────────────────────────────────────────────
    fieldnames = ["table", "mssql_count", "mysql_count", "diff", "ignored_rows", "match"]
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    # ── Summary ───────────────────────────────────────────────────────────────
    ok       = sum(1 for r in rows_out if r["match"] == "YES")
    no_match = sum(1 for r in rows_out if r["match"] == "NO")
    errors   = sum(1 for r in rows_out if r["match"] == "ERROR")

    logger.info("=" * 62)
    logger.info("SUMMARY")
    logger.info("=" * 62)
    logger.info("  total tables  : %d", len(tables))
    logger.info("  match (OK)    : %d", ok)
    logger.info("  mismatch      : %d", no_match)
    logger.info("  errors        : %d", errors)

    if mismatches:
        logger.warning("  %-35s %8s %10s %8s", "Table", "Diff", "Ignored", "Gap")
        logger.warning("  %s", "-" * 65)
        for table, diff, ignored in mismatches:
            logger.warning("  %-35s %8d %10d %8d", table, diff, ignored, diff - ignored)

    logger.info("  report        : %s", OUTPUT_CSV)
    logger.info("=" * 62)


if __name__ == "__main__":
    main()
