from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
import subprocess
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────

local_tz = pendulum.timezone("Asia/Kolkata")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts")))

from db_config import (  # noqa: E402
    STATE_DIR,
    S3_TRANSFER_MYSQL_HOST     as MYSQL_HOST,
    S3_TRANSFER_MYSQL_PORT     as MYSQL_PORT,
    S3_TRANSFER_MYSQL_USER     as MYSQL_USER,
    S3_TRANSFER_MYSQL_PASSWORD as MYSQL_PASSWORD,
    S3_TRANSFER_MYSQL_SCHEMA   as MYSQL_SCHEMA,
    S3_TRANSFER_MYSQLDUMP_PATH as MYSQLDUMP_PATH,
    S3_TRANSFER_MYSQL_PATH     as MYSQL_PATH,
    S3_TRANSFER_BUCKET         as S3_BUCKET,
    S3_TRANSFER_PREFIX         as S3_PREFIX,
    S3_TRANSFER_OUTPUT_BASE    as OUTPUT_BASE,
)

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ──────────────────────────────────────────────
# State helpers
# ──────────────────────────────────────────────

def _read_state(table: str) -> dict:
    path = os.path.join(STATE_DIR, f"{table}.json")
    if os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)
    return {}


def _write_state(table: str, state: dict) -> None:
    path = os.path.join(STATE_DIR, f"{table}.json")
    with open(path, "w") as fh:
        json.dump(state, fh, indent=4)


def _update_s3_transfer(table: str, patch: dict) -> None:
    state = _read_state(table)
    state["s3_transfer"] = {**state.get("s3_transfer", {}), **patch}
    _write_state(table, state)


# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="mysql_dump_s3_transfer",
    description="Dump all tables from MySQL schema, upload to S3, drop from schema, track state",
    schedule="0 19 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["mysql", "s3", "dump"],
    params={
        "date_folder": "",   # e.g. "04062026" — leave blank to use today
        "table_name": "",    # e.g. "my_table" — leave blank to process all tables
    },
) as dag:

    def resolve_date_folder(params: dict) -> str:
        raw = (params.get("date_folder") or "").strip()
        return raw if raw else datetime.now().strftime("%m%d%Y")

    # ──────────────────────────────────────────────
    # 1️⃣  Get table names from schema
    # ──────────────────────────────────────────────

    def get_tables(**context):
        params     = context["params"]
        table_name = (params.get("table_name") or "").strip()

        # Fetch all tables from schema
        query = (
            f"SELECT TABLE_NAME FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA = '{MYSQL_SCHEMA}' AND TABLE_TYPE = 'BASE TABLE';"
        )
        result = subprocess.run(
            [
                MYSQL_PATH,
                "-h", MYSQL_HOST,
                "-P", MYSQL_PORT,
                "-u", MYSQL_USER,
                f"-p{MYSQL_PASSWORD}",
                "--batch",
                "--skip-column-names",
                "-e", query,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        all_tables = [t.strip() for t in result.stdout.splitlines() if t.strip()]
        if not all_tables:
            raise RuntimeError(f"No tables found in schema '{MYSQL_SCHEMA}'")

        # If a specific table was requested, validate and use only that one
        if table_name:
            if table_name not in all_tables:
                raise RuntimeError(
                    f"Table '{table_name}' not found in schema '{MYSQL_SCHEMA}'. "
                    f"Available tables: {all_tables}"
                )
            candidate_tables = [table_name]
            print(f"Single-table mode: processing '{table_name}'")
        else:
            candidate_tables = all_tables
            print(f"All-tables mode: found {len(all_tables)} tables in schema")

        # Skip tables already fully processed today
        today  = datetime.now().strftime("%Y-%m-%d")
        tables = []
        for table in candidate_tables:
            s3_state = _read_state(table).get("s3_transfer", {})
            if s3_state.get("is_deleted") is True and s3_state.get("last_processed_date") == today:
                print(f"SKIP {table}: already transferred and dropped today")
                continue
            tables.append(table)

        if not tables:
            raise RuntimeError("All selected tables already processed today — nothing to do.")

        print(f"{len(tables)} table(s) to process: {tables}")
        return tables

    get_tables_task = PythonOperator(
        task_id="get_tables",
        python_callable=get_tables,
    )

    # ──────────────────────────────────────────────
    # 2️⃣  Dump tables to local .sql files
    # ──────────────────────────────────────────────

    def dump_tables(**context):
        params      = context["params"]
        date_folder = resolve_date_folder(params)
        tables      = context["ti"].xcom_pull(task_ids="get_tables")

        dump_dir = os.path.join(OUTPUT_BASE, date_folder, MYSQL_SCHEMA)
        os.makedirs(dump_dir, exist_ok=True)

        failed = []
        for table in tables:
            dump_file = os.path.join(dump_dir, f"{table}.sql")
            try:
                with open(dump_file, "w") as fh:
                    subprocess.run(
                        [
                            MYSQLDUMP_PATH,
                            "--default-character-set=utf8",
                            "-h", MYSQL_HOST,
                            "-P", MYSQL_PORT,
                            "-u", MYSQL_USER,
                            f"-p{MYSQL_PASSWORD}",
                            MYSQL_SCHEMA,
                            table,
                        ],
                        stdout=fh,
                        stderr=subprocess.PIPE,
                        check=True,
                    )
                print(f"Dumped: {table} → {dump_file}")
            except subprocess.CalledProcessError as e:
                print(f"ERROR dumping {table}: {e.stderr.decode()}")
                failed.append(table)

        if failed:
            raise RuntimeError(f"Dump failed for tables: {failed}")

        return {"date_folder": date_folder, "dump_dir": dump_dir, "tables": tables}

    dump_task = PythonOperator(
        task_id="dump_tables",
        python_callable=dump_tables,
    )

    # ──────────────────────────────────────────────
    # 3️⃣  Upload .sql files to S3 + write state
    # ──────────────────────────────────────────────

    def upload_to_s3(**context):
        info        = context["ti"].xcom_pull(task_ids="dump_tables")
        date_folder = info["date_folder"]
        dump_dir    = info["dump_dir"]
        tables      = info["tables"]
        today       = datetime.now().strftime("%Y-%m-%d")

        stats  = []
        failed = []

        for table in tables:
            file_path = os.path.join(dump_dir, f"{table}.sql")
            if not os.path.exists(file_path):
                print(f"SKIP {table}: dump file not found")
                failed.append(table)
                continue

            # Checksum + size
            h = hashlib.md5()
            with open(file_path, "rb") as fh:
                for chunk in iter(lambda: fh.read(4096), b""):
                    h.update(chunk)
            checksum  = h.hexdigest()
            file_size = os.path.getsize(file_path)

            # S3 destination
            rel_path = os.path.relpath(file_path, os.path.join(OUTPUT_BASE, date_folder))
            s3_uri   = f"s3://{S3_BUCKET}/{S3_PREFIX}/{date_folder}/{rel_path}".replace("\\", "/")

            try:
                subprocess.run(
                    ["aws", "s3", "cp", file_path, s3_uri],
                    check=True,
                    capture_output=True,
                )
                print(f"Uploaded → {s3_uri}")
                stats.append({
                    "table": table,
                    "file_path": os.path.relpath(file_path, OUTPUT_BASE),
                    "checksum": checksum,
                    "file_size_bytes": file_size,
                    "s3_uri": s3_uri,
                })

                # Write state: uploaded but not yet deleted
                _update_s3_transfer(table, {
                    "last_processed_date": today,
                    "local_dump_folder": date_folder,
                    "transferred_folder_in_s3": date_folder,
                    "is_deleted": False,
                })

            except subprocess.CalledProcessError as e:
                print(f"ERROR uploading {table}: {e.stderr.decode()}")
                failed.append(table)

        if failed:
            raise RuntimeError(f"S3 upload failed for: {failed}")

        # Stats CSV
        stats_local = os.path.join(OUTPUT_BASE, date_folder, "sql_dump_stats.csv")
        with open(stats_local, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["table", "file_path", "checksum", "file_size_bytes", "s3_uri"])
            writer.writeheader()
            writer.writerows(stats)

        stats_s3 = f"s3://{S3_BUCKET}/{S3_PREFIX}/{date_folder}/sql_dump_stats.csv"
        subprocess.run(["aws", "s3", "cp", stats_local, stats_s3], check=True, capture_output=True)
        print(f"Uploaded stats → {stats_s3}")
        print(f"Transfer complete: {len(stats)} tables uploaded for date_folder={date_folder}")

        return {"date_folder": date_folder, "uploaded_tables": [s["table"] for s in stats]}

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    # ──────────────────────────────────────────────
    # 4️⃣  Drop tables from schema + update state
    # ──────────────────────────────────────────────

    def drop_tables(**context):
        info        = context["ti"].xcom_pull(task_ids="upload_to_s3")
        tables      = info["uploaded_tables"]
        date_folder = info["date_folder"]
        today       = datetime.now().strftime("%Y-%m-%d")

        failed = []
        for table in tables:
            try:
                subprocess.run(
                    [
                        MYSQL_PATH,
                        "-h", MYSQL_HOST,
                        "-P", MYSQL_PORT,
                        "-u", MYSQL_USER,
                        f"-p{MYSQL_PASSWORD}",
                        MYSQL_SCHEMA,
                        "-e", f"DROP TABLE IF EXISTS {table};",
                    ],
                    check=True,
                    capture_output=True,
                )
                print(f"Dropped table: {table}")

                _update_s3_transfer(table, {
                    "last_processed_date": today,
                    "local_dump_folder": date_folder,
                    "transferred_folder_in_s3": date_folder,
                    "is_deleted": True,
                })

            except subprocess.CalledProcessError as e:
                print(f"ERROR dropping {table}: {e.stderr.decode()}")
                failed.append(table)
                # State remains is_deleted=False so next run will retry

        if failed:
            raise RuntimeError(f"Drop failed for tables: {failed}")

    drop_task = PythonOperator(
        task_id="drop_tables",
        python_callable=drop_tables,
    )

    # ──────────────────────────────────────────────
    # Dependencies
    # ──────────────────────────────────────────────

    get_tables_task >> dump_task >> upload_task >> drop_task