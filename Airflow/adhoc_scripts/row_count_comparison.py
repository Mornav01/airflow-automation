"""
Row count comparison between two databases for a given set of tables.
Source and dest can each independently be MSSQL or MySQL (mssql-mssql,
mysql-mysql, or mssql-mysql in either direction) — the dialect is detected
from each connection's engine, so identifier quoting is picked automatically.

Usage:
    python row_count_comparison.py [path_to_csv] [--source-conn URL] [--dest-conn URL] [--output-csv PATH]

CSV format: one table name per row (header optional, first column used).

Config is read from Airflow/.env (see .env.example): path_to_csv defaults to
ROWCOUNT_TABLES_CSV, connection strings default to ROWCOUNT_SOURCE_CONN /
ROWCOUNT_DEST_CONN, and the output CSV path defaults to ROWCOUNT_OUTPUT_CSV
(or row_count_stats.csv next to this script).
"""

import argparse
import csv
import os
from pathlib import Path

import sqlalchemy
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).parent

load_dotenv(SCRIPT_DIR.parent / ".env")

# Dialects that quote identifiers with brackets instead of backticks.
BRACKET_DIALECTS = {"mssql"}


def get_row_count(engine, table_name: str) -> int | None:
    quote = "[{}]" if engine.dialect.name in BRACKET_DIALECTS else "`{}`"
    try:
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(f"SELECT COUNT(*) FROM {quote.format(table_name)}"))
            return result.scalar()
    except Exception as e:
        print(f"\n    [{engine.dialect.name}] error counting `{table_name}`: {e}")
        return None


def load_tables_from_csv(csv_path: str) -> list[str]:
    tables = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            val = row[0].strip()
            if not val:
                continue
            # Skip header row if it looks like a header label
            if i == 0 and val.lower() in ("table", "table_name", "tablename", "name"):
                continue
            tables.append(val)
    return tables


def make_engine(conn_str: str, label: str):
    if not conn_str:
        print(f"No connection string provided for {label} (set ROWCOUNT_{label.upper()}_CONN or pass --{label}-conn).")
        raise SystemExit(1)
    engine = sqlalchemy.create_engine(conn_str)
    print(f"Connecting to {label} ({engine.dialect.name})...")
    return engine


def main():
    parser = argparse.ArgumentParser(description="Compare row counts between two databases (any mix of MSSQL/MySQL)")
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=os.environ.get("ROWCOUNT_TABLES_CSV", ""),
        help="Path to CSV of table names",
    )
    parser.add_argument("--source-conn", default=os.environ.get("ROWCOUNT_SOURCE_CONN", ""), help="SQLAlchemy URL for source DB")
    parser.add_argument("--dest-conn", default=os.environ.get("ROWCOUNT_DEST_CONN", ""), help="SQLAlchemy URL for dest DB")
    parser.add_argument(
        "--output-csv",
        default=os.environ.get("ROWCOUNT_OUTPUT_CSV") or str(SCRIPT_DIR / "row_count_stats.csv"),
        help="Path to write the output CSV",
    )
    args = parser.parse_args()

    if not args.csv_path:
        print("No CSV path provided (set ROWCOUNT_TABLES_CSV in manual.env or pass it as an argument).")
        raise SystemExit(1)
    if not os.path.exists(args.csv_path):
        print(f"CSV file not found: {args.csv_path}")
        raise SystemExit(1)

    tables = load_tables_from_csv(args.csv_path)
    if not tables:
        print("No tables found in CSV.")
        raise SystemExit(1)

    print(f"Loaded {len(tables)} tables from {args.csv_path}")
    source_engine = make_engine(args.source_conn, "source")
    dest_engine = make_engine(args.dest_conn, "dest")

    rows = []
    for table in tables:
        print(f"  Counting: {table}", end=" ... ", flush=True)
        source_count = get_row_count(source_engine, table)
        dest_count = get_row_count(dest_engine, table)

        if source_count is None or dest_count is None:
            match = "ERROR"
        elif source_count == dest_count:
            match = "OK"
        else:
            match = "MISMATCH"

        print(f"source={source_count}  dest={dest_count}  [{match}]")
        rows.append({
            "table": table,
            "source_count": source_count if source_count is not None else "ERROR",
            "dest_count": dest_count if dest_count is not None else "ERROR",
            "match": match,
        })

    out_path = Path(args.output_csv)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["table", "source_count", "dest_count", "match"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nStats written to: {out_path}")

    mismatches = [r for r in rows if r["match"] == "MISMATCH"]
    errors = [r for r in rows if r["match"] == "ERROR"]
    print(f"Summary: {len(tables)} tables | {len(mismatches)} mismatches | {len(errors)} errors")


if __name__ == "__main__":
    main()
