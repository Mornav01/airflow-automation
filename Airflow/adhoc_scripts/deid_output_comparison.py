#!/usr/bin/env python
"""
Deid Output Comparison Script

Compares old vs new deidentification output for a set of tables.
Only compares columns that have deidentification rules in config_rules.csv.
Rows are joined on nd_auto_increment_id.
Differences are written to MySQL tables for querying.

Usage:
    python deid_output_comparison.py \
        --old-conn "mysql+pymysql://user:pass@host/old_db" \
        --new-conn "mysql+pymysql://user:pass@host/new_db" \
        --diff-conn "mysql+pymysql://user:pass@host/diff_db" \
        --tables /path/to/tables.csv \
        --rules-csv /path/to/config_rules.csv
"""

import argparse
import csv
import logging
import os
import sys
from datetime import datetime

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

# ── Paths ─────────────────────────────────────────────────────────────────────

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

DEFAULT_TABLES = "/Users/ndaijwm/Desktop/airflow_incremental/airflow-automation/input/test2.csv"
DEFAULT_RULES_CSV = "/Users/ndaijwm/Desktop/airflow_incremental/airflow-automation/Airflow/scripts/configs/adhoc_config_rules.csv"
LOG_DIR = os.path.join(_SCRIPT_DIR, "..", "..", "deid_runtime", "logs")

# Warn if a table has more than this many rows (memory caution)
LARGE_TABLE_THRESHOLD = 500_000

# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(
        LOG_DIR,
        f"deid_output_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Log file: %s", log_file)


# ── Rules CSV ─────────────────────────────────────────────────────────────────

def load_phi_columns(
    rules_csv: str, exclude_rules: list[str] | None = None
) -> dict[str, list[tuple[str, str]]]:
    """Return {table_name: [(col, rule), ...]} for columns with non-empty rules.

    Args:
        rules_csv: Path to the rules CSV file.
        exclude_rules: List of rule names to skip (case-insensitive).
    """
    excluded = {r.upper() for r in (exclude_rules or [])}
    phi: dict[str, list[tuple[str, str]]] = {}
    with open(rules_csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rule = (row.get("rule") or "").strip()
            if not rule:
                continue
            if rule.upper() in excluded:
                continue
            table = row["table_name"].strip()
            col = row["column_name"].strip()
            phi.setdefault(table, []).append((col, rule))
    return phi


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_engine(conn_str: str) -> sa.Engine:
    return sa.create_engine(conn_str)


def get_row_count(engine: sa.Engine, table: str) -> int:
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM `{table}`"))
        return result.scalar()


def fetch_table(engine: sa.Engine, table: str, columns: list[str]) -> pd.DataFrame:
    cols = ", ".join(f"`{c}`" for c in ["nd_auto_increment_id"] + columns)
    query = f"SELECT {cols} FROM `{table}`"
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


# ── Diff storage ──────────────────────────────────────────────────────────────

DDL_DIFFS = """
CREATE TABLE IF NOT EXISTS deid_comparison_diffs (
    id                   BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id               VARCHAR(30),
    table_name           VARCHAR(100),
    nd_auto_increment_id BIGINT,
    column_name          VARCHAR(100),
    rule_name            VARCHAR(100),
    old_value            LONGTEXT,
    new_value            LONGTEXT,
    compared_at          DATETIME
)
"""

DDL_SUMMARY = """
CREATE TABLE IF NOT EXISTS deid_comparison_summary (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    run_id              VARCHAR(30),
    table_name          VARCHAR(100),
    total_rows_checked  INT,
    rows_with_diffs     INT,
    columns_compared    INT,
    compared_at         DATETIME
)
"""


def ensure_diff_tables(engine: sa.Engine):
    with engine.begin() as conn:
        conn.execute(text(DDL_DIFFS))
        conn.execute(text(DDL_SUMMARY))


def write_diffs(engine: sa.Engine, run_id: str, table: str, diffs: list[dict]):
    if not diffs:
        return
    now = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "table_name": table,
            "nd_auto_increment_id": d["nd_auto_increment_id"],
            "column_name": d["column_name"],
            "rule_name": d["rule_name"],
            "old_value": str(d["old_value"]) if d["old_value"] is not None else None,
            "new_value": str(d["new_value"]) if d["new_value"] is not None else None,
            "compared_at": now,
        }
        for d in diffs
    ]
    df = pd.DataFrame(rows)
    df.to_sql("deid_comparison_diffs", engine, if_exists="append", index=False)


def write_summary(engine: sa.Engine, run_id: str, table: str,
                  total_rows: int, rows_with_diffs: int, columns_compared: int):
    row = pd.DataFrame([{
        "run_id": run_id,
        "table_name": table,
        "total_rows_checked": total_rows,
        "rows_with_diffs": rows_with_diffs,
        "columns_compared": columns_compared,
        "compared_at": datetime.now(),
    }])
    row.to_sql("deid_comparison_summary", engine, if_exists="append", index=False)


# ── Core comparison ───────────────────────────────────────────────────────────

def compare_table(
    table: str,
    phi_col_rules: list[tuple[str, str]],
    old_engine: sa.Engine,
    new_engine: sa.Engine,
    diff_engine: sa.Engine,
    run_id: str,
) -> dict:
    """Compare one table. Returns summary dict.

    Args:
        phi_col_rules: List of (column_name, rule_name) tuples.
    """
    phi_cols = [col for col, _ in phi_col_rules]
    rule_map = {col: rule for col, rule in phi_col_rules}

    logging.info("[%s] Comparing %d PHI column(s): %s", table, len(phi_cols), phi_cols)

    # Check row counts before fetching
    try:
        old_count = get_row_count(old_engine, table)
        new_count = get_row_count(new_engine, table)
    except Exception as e:
        logging.error("[%s] Could not get row count: %s", table, e)
        return {"table": table, "status": "error", "error": str(e)}

    logging.info("[%s] Old DB rows: %d | New DB rows: %d", table, old_count, new_count)

    if old_count > LARGE_TABLE_THRESHOLD or new_count > LARGE_TABLE_THRESHOLD:
        logging.warning(
            "[%s] Table has >%s rows — fetching full table may use significant memory",
            table, f"{LARGE_TABLE_THRESHOLD:,}"
        )

    # Fetch data
    try:
        df_old = fetch_table(old_engine, table, phi_cols)
        df_new = fetch_table(new_engine, table, phi_cols)
    except Exception as e:
        logging.error("[%s] Failed to fetch data: %s", table, e)
        return {"table": table, "status": "error", "error": str(e)}

    # Rename columns to avoid collision on merge
    df_old = df_old.rename(columns={c: f"{c}__old" for c in phi_cols})
    df_new = df_new.rename(columns={c: f"{c}__new" for c in phi_cols})

    merged = pd.merge(df_old, df_new, on="nd_auto_increment_id", how="inner")
    total_rows = len(merged)
    logging.info("[%s] Matched rows (inner join): %d", table, total_rows)

    # Find diffs per column
    diffs = []
    differing_cols = set()

    for col in phi_cols:
        old_col = f"{col}__old"
        new_col = f"{col}__new"
        if old_col not in merged.columns or new_col not in merged.columns:
            logging.warning("[%s] Column '%s' missing from one or both DBs, skipping", table, col)
            continue

        # Compare as strings to handle type differences gracefully
        mask = merged[old_col].astype(str) != merged[new_col].astype(str)
        diff_rows = merged[mask][["nd_auto_increment_id", old_col, new_col]]

        if not diff_rows.empty:
            differing_cols.add(col)
            for _, row in diff_rows.iterrows():
                diffs.append({
                    "nd_auto_increment_id": row["nd_auto_increment_id"],
                    "column_name": col,
                    "rule_name": rule_map[col],
                    "old_value": row[old_col],
                    "new_value": row[new_col],
                })

    rows_with_diffs = len({d["nd_auto_increment_id"] for d in diffs})

    # Write to MySQL
    write_diffs(diff_engine, run_id, table, diffs)
    write_summary(diff_engine, run_id, table, total_rows, rows_with_diffs, len(phi_cols))

    if rows_with_diffs == 0:
        logging.info("[%s] No differences found", table)
    else:
        logging.warning(
            "[%s] %d row(s) differ across %d column(s): %s",
            table, rows_with_diffs, len(differing_cols), sorted(differing_cols)
        )

    return {
        "table": table,
        "status": "ok",
        "total_rows": total_rows,
        "rows_with_diffs": rows_with_diffs,
        "columns_compared": len(phi_cols),
        "differing_cols": sorted(differing_cols),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compare old vs new deid output")
    parser.add_argument("--old-conn",   default=os.environ.get("MYSQL_OLD_CONN", ""),  help="SQLAlchemy URL for old code output DB")
    parser.add_argument("--new-conn",   default=os.environ.get("MYSQL_NEW_CONN", ""),  help="SQLAlchemy URL for new code output DB")
    parser.add_argument("--diff-conn",  default=os.environ.get("MYSQL_DIFF_CONN", ""), help="SQLAlchemy URL for MySQL DB to store diff results")
    parser.add_argument("--tables",     default=DEFAULT_TABLES,    help="Path to tables CSV (one table per line)")
    parser.add_argument("--rules-csv",  default=DEFAULT_RULES_CSV, help="Path to config_rules.csv")
    parser.add_argument(
        "--exclude-rules",
        default=['MASK'],
        nargs="*",
        metavar="RULE",
        help="Rule names to exclude from comparison (e.g. --exclude-rules STATIC_OFFSET NOTES). "
             "Default: none excluded.",
    )
    args = parser.parse_args()

    setup_logging()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logging.info("Run ID: %s", run_id)

    # Load rules
    if not os.path.exists(args.rules_csv):
        logging.error("Rules CSV not found: %s", args.rules_csv)
        sys.exit(1)
    phi_columns = load_phi_columns(args.rules_csv, exclude_rules=args.exclude_rules)
    if args.exclude_rules:
        logging.info("Excluding rules: %s", args.exclude_rules)
    logging.info("Loaded PHI rules for %d table(s)", len(phi_columns))

    # Load table list
    if not os.path.exists(args.tables):
        logging.error("Tables file not found: %s", args.tables)
        sys.exit(1)
    with open(args.tables, newline="") as f:
        tables = [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]
    logging.info("Tables to compare: %d", len(tables))

    # Connect
    old_engine  = get_engine(args.old_conn)
    new_engine  = get_engine(args.new_conn)
    diff_engine = get_engine(args.diff_conn)

    ensure_diff_tables(diff_engine)

    # Run comparison
    results = []
    for table in tables:
        phi_col_rules = phi_columns.get(table, [])
        if not phi_col_rules:
            logging.info("[%s] SKIP — no PHI columns in rules CSV", table)
            results.append({"table": table, "status": "skip"})
            continue
        result = compare_table(table, phi_col_rules, old_engine, new_engine, diff_engine, run_id)
        results.append(result)

    # Print summary
    print("\n" + "=" * 75)
    print(f"{'Table':<30} | {'Rows Checked':>12} | {'Rows w/ Diffs':>13} | Differing Columns")
    print("-" * 75)
    for r in results:
        if r["status"] == "skip":
            print(f"{r['table']:<30} | {'SKIP (no PHI)':>12} | {'':>13} |")
        elif r["status"] == "error":
            print(f"{r['table']:<30} | {'ERROR':>12} | {'':>13} | {r.get('error', '')}")
        else:
            diff_cols_str = ", ".join(r["differing_cols"]) if r["differing_cols"] else "—"
            print(
                f"{r['table']:<30} | {r['total_rows']:>12,} | {r['rows_with_diffs']:>13,} | {diff_cols_str}"
            )
    print("=" * 75)
    logging.info("Done. Diff results written to 'deid_comparison_diffs' and 'deid_comparison_summary'.")


if __name__ == "__main__":
    main()
