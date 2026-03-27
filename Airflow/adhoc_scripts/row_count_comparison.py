"""
Row count comparison between MSSQL (source) and MySQL (deidentified) for a given set of tables.

Usage:
    python row_count_comparison.py <path_to_csv>

CSV format: one table name per row (header optional, first column used).

Output: row_count_stats_<timestamp>.csv written to the same folder as this script.
"""

import sys
import csv
import os
from datetime import datetime
from pathlib import Path

import sqlalchemy

MSSQL_CONN = "mssql+pyodbc://sa:ndADMIN2025@127.0.0.1:1437/mobiledoc_base_2302?driver=ODBC+Driver+17+for+SQL+Server"
MYSQL_CONN = "mysql+pymysql://ndadmin:ndADMIN%402025@localhost:3306/deidentified_mobiledoc_base"

SCRIPT_DIR = Path(__file__).parent


def get_row_count_mssql(engine, table_name: str) -> int | None:
    try:
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(f"SELECT COUNT(*) FROM [{table_name}]"))
            return result.scalar()
    except Exception:
        return None


def get_row_count_mysql(engine, table_name: str) -> int | None:
    try:
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(f"SELECT COUNT(*) FROM `{table_name}`"))
            return result.scalar()
    except Exception:
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


def main():
    if len(sys.argv) < 2:
        print("Usage: python row_count_comparison.py <path_to_csv>")
        sys.exit(1)

    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        print(f"CSV file not found: {csv_path}")
        sys.exit(1)

    tables = load_tables_from_csv(csv_path)
    if not tables:
        print("No tables found in CSV.")
        sys.exit(1)

    print(f"Loaded {len(tables)} tables from {csv_path}")
    print("Connecting to MSSQL...")
    mssql_engine = sqlalchemy.create_engine(MSSQL_CONN, fast_executemany=True)
    print("Connecting to MySQL...")
    mysql_engine = sqlalchemy.create_engine(MYSQL_CONN)

    rows = []
    for table in tables:
        print(f"  Counting: {table}", end=" ... ", flush=True)
        mssql_count = get_row_count_mssql(mssql_engine, table)
        mysql_count = get_row_count_mysql(mysql_engine, table)

        if mssql_count is None or mysql_count is None:
            match = "ERROR"
        elif mssql_count == mysql_count:
            match = "OK"
        else:
            match = "MISMATCH"

        print(f"MSSQL={mssql_count}  MySQL={mysql_count}  [{match}]")
        rows.append({
            "table": table,
            "mssql_count": mssql_count if mssql_count is not None else "ERROR",
            "mysql_count": mysql_count if mysql_count is not None else "ERROR",
            "match": match,
        })

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = SCRIPT_DIR / f"row_count_stats_{timestamp}.csv"

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["table", "mssql_count", "mysql_count", "match"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nStats written to: {out_path}")

    mismatches = [r for r in rows if r["match"] == "MISMATCH"]
    errors = [r for r in rows if r["match"] == "ERROR"]
    print(f"Summary: {len(tables)} tables | {len(mismatches)} mismatches | {len(errors)} errors")


if __name__ == "__main__":
    main()
