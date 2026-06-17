from __future__ import annotations

import csv
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import dotenv_values

local_tz = pendulum.timezone("Asia/Kolkata")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts")))

MANUAL_ENV_PATH = os.path.join(SCRIPT_DIR, "..", "manual.env")

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _extract_db(conn_str: str) -> str:
    return conn_str.split("/")[-1].split("?")[0]


def _detect_db_type(conn_str: str) -> str:
    if conn_str.startswith("mysql"):
        return "mysql"
    if conn_str.startswith("mssql"):
        return "mssql"
    raise ValueError(f"Cannot detect db_type from connection string: {conn_str[:40]}...")


def _reset_to_y(engine, db_type: str, db: str, table: str) -> int:
    from sqlalchemy import text

    if db_type == "mysql":
        sql = f"UPDATE `{db}`.`{table}` SET nd_ActiveFlag = 'Y'"
    else:
        sql = f"UPDATE [{table}] SET nd_ActiveFlag = 'Y'"

    with engine.begin() as conn:
        result = conn.execute(text(sql))
        return result.rowcount


def _normalize_table(engine, db_type: str, db: str, table: str, pk_cols: list) -> int:
    from sqlalchemy import text

    if db_type == "mysql":
        pk_expr = ", ".join(f"`{c}`" for c in pk_cols)
        sql = f"""
            UPDATE `{db}`.`{table}` t
            JOIN (
                SELECT nd_auto_increment_id FROM (
                    SELECT nd_auto_increment_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY {pk_expr}
                               ORDER BY nd_auto_increment_id DESC
                           ) rn
                    FROM `{db}`.`{table}`
                    WHERE nd_ActiveFlag = 'Y'
                ) x WHERE rn > 1
            ) d ON t.nd_auto_increment_id = d.nd_auto_increment_id
            SET t.nd_ActiveFlag = 'N'
        """
    else:
        pk_expr = ", ".join(f"[{c}]" for c in pk_cols)
        sql = f"""
            UPDATE t SET t.nd_ActiveFlag = 'N'
            FROM [{table}] t
            JOIN (
                SELECT nd_auto_increment_id FROM (
                    SELECT nd_auto_increment_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY {pk_expr}
                               ORDER BY nd_auto_increment_id DESC
                           ) rn
                    FROM [{table}]
                    WHERE nd_ActiveFlag = 'Y'
                ) x WHERE rn > 1
            ) d ON t.nd_auto_increment_id = d.nd_auto_increment_id
        """

    with engine.begin() as conn:
        result = conn.execute(text(sql))
        return result.rowcount


def _table_exists_db(engine, db_type: str, db: str, table: str) -> bool:
    from sqlalchemy import text

    if db_type == "mysql":
        sql = text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = :db AND table_name = :tbl LIMIT 1"
        )
    else:
        sql = text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_catalog = :db AND table_name = :tbl"
        )
    with engine.connect() as conn:
        row = conn.execute(sql, {"db": db, "tbl": table}).fetchone()
    return row is not None


# ──────────────────────────────────────────────
# Task callable
# ──────────────────────────────────────────────

def normalize(**context):
    from sqlalchemy import create_engine

    params     = context["params"]
    table_name = (params.get("table_name") or "").strip()

    # Load manual.env
    cfg = dotenv_values(MANUAL_ENV_PATH)

    conn_str   = (cfg.get("MANUAL_ACTIVE_CONN") or "").strip()
    tables_csv = (cfg.get("MANUAL_ACTIVE_TABLES_CSV") or "").strip()
    pk_csv     = (cfg.get("MANUAL_ACTIVE_PK_CSV") or "").strip()

    if not conn_str:
        raise RuntimeError("MANUAL_ACTIVE_CONN is blank in manual.env — set a SQLAlchemy connection string.")
    if not tables_csv or not Path(tables_csv).exists():
        raise RuntimeError(f"MANUAL_ACTIVE_TABLES_CSV not set or file missing: {tables_csv}")
    if not pk_csv or not Path(pk_csv).exists():
        raise RuntimeError(f"MANUAL_ACTIVE_PK_CSV not set or file missing: {pk_csv}")

    # Read table list
    with open(tables_csv, newline="") as f:
        all_tables = [r[0].strip() for r in csv.reader(f) if r and r[0].strip()]
    if table_name:
        if table_name not in all_tables:
            raise RuntimeError(f"Table '{table_name}' not found in {tables_csv}")
        all_tables = [table_name]
        print(f"Single-table mode: {table_name}")
    else:
        print(f"All-tables mode: {len(all_tables)} tables")

    # Build PK dict: {table_name: [col1, col2, ...]}
    # primary_key cell may contain a comma-separated list for composite PKs
    pk_map = defaultdict(list)
    with open(pk_csv, newline="") as f:
        for row in csv.DictReader(f):
            t = (row.get("table_name") or "").strip()
            c = (row.get("primary_key") or "").strip()
            if t and c:
                for col in c.split(","):
                    col = col.strip()
                    if col:
                        pk_map[t].append(col)

    db_type = _detect_db_type(conn_str)
    db      = _extract_db(conn_str)
    print(f"\n── {db_type.upper()} | {db} ──────────────────────────")

    engine    = create_engine(conn_str, pool_pre_ping=True, pool_size=5, max_overflow=10)
    total     = 0
    skipped   = 0
    processed = 0

    for table in all_tables:
        pk_cols = pk_map.get(table, [])
        if not pk_cols:
            print(f"  SKIP {table}: no PK found in {pk_csv}")
            skipped += 1
            continue

        if not _table_exists_db(engine, db_type, db, table):
            print(f"  SKIP {table}: table not found in {db}")
            skipped += 1
            continue

        try:
            reset    = _reset_to_y(engine, db_type, db, table)
            rows_set = _normalize_table(engine, db_type, db, table, pk_cols)
            print(f"  OK   {table:<45} pk={pk_cols}  reset_Y={reset:,}  rows_set_N={rows_set:,}")
            total     += rows_set
            processed += 1
        except Exception as e:
            print(f"  ERROR {table}: {e}")

    engine.dispose()
    print(f"\nDone. {processed} tables processed, {skipped} skipped, {total:,} total rows set to N")


# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="nd_activeflag_normalizer",
    description="Normalize nd_ActiveFlag (Y/N) on any MySQL or MSSQL DB using manual.env",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["activeflag", "normalize", "manual"],
    params={
        "table_name": "",   # leave blank to process all tables from MANUAL_ACTIVE_TABLES_CSV
    },
) as dag:

    PythonOperator(
        task_id="normalize",
        python_callable=normalize,
    )
