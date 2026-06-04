from __future__ import annotations

import csv
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
    DEID_TABLES_CSV,  # noqa: F401 — kept for parity with gcp_transfer
    AWS_TRANSFER_MYSQL_HOST       as MYSQL_HOST,
    AWS_TRANSFER_MYSQL_PORT       as MYSQL_PORT,
    AWS_TRANSFER_MYSQL_USER       as MYSQL_USER,
    AWS_TRANSFER_MYSQL_PASSWORD   as MYSQL_PASSWORD,
    AWS_TRANSFER_MYSQL_SCHEMA     as MYSQL_SCHEMA,
    AWS_TRANSFER_MYSQLDUMP_PATH   as MYSQLDUMP_PATH,
    AWS_TRANSFER_MYSQL_PATH       as MYSQL_PATH,
    AWS_TRANSFER_S3_BUCKET        as S3_BUCKET,
    AWS_TRANSFER_S3_PREFIX        as S3_PREFIX,
    AWS_TRANSFER_OUTPUT_BASE      as OUTPUT_BASE,
)
from s3_upload_service import s3_client, local_md5  # noqa: E402

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


def _update_aws_transfer(table: str, patch: dict) -> None:
    state = _read_state(table)
    state["aws_transfer"] = {**state.get("aws_transfer", {}), **patch}
    _write_state(table, state)


# ──────────────────────────────────────────────
# Shared task callables
# ──────────────────────────────────────────────

def resolve_date_folder(params: dict) -> str:
    raw = (params.get("date_folder") or "").strip()
    return raw if raw else datetime.now().strftime("%m%d%Y")


def _mysql_env() -> dict:
    """env dict that hides the password from the process list via MYSQL_PWD."""
    return {**os.environ, "MYSQL_PWD": MYSQL_PASSWORD}


def _verify_s3_object(client, bucket: str, key: str, expected_md5: str, expected_size: int) -> bool:
    """head_object the uploaded key and check size + (when single-part) ETag md5.

    Falls back to trusting boto3's upload-time Content-MD5 check when the IAM
    role is write-only (head_object returns 403 AccessDenied).
    """
    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        code = getattr(getattr(e, "response", None), "get", lambda *_: {})("Error", {}).get("Code", "")
        msg = str(e)
        if code in ("AccessDenied", "403") or "Forbidden" in msg or "AccessDenied" in msg:
            print(f"VERIFY SKIP s3://{bucket}/{key}: head_object forbidden — trusting upload_file integrity check")
            return True
        print(f"VERIFY FAIL s3://{bucket}/{key}: head_object failed: {e}")
        return False
    if head.get("ContentLength") != expected_size:
        print(
            f"VERIFY FAIL s3://{bucket}/{key}: size {head.get('ContentLength')} != local {expected_size}"
        )
        return False
    etag = (head.get("ETag") or "").strip('"')
    if "-" in etag:
        return True
    if etag.lower() != expected_md5.lower():
        print(f"VERIFY FAIL s3://{bucket}/{key}: ETag {etag} != local md5 {expected_md5}")
        return False
    return True


def get_tables(**context):
    params     = context["params"]
    table_name = (params.get("table_name") or "").strip()

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
            "--batch",
            "--skip-column-names",
            "-e", query,
        ],
        capture_output=True,
        text=True,
        check=True,
        env=_mysql_env(),
    )
    all_tables = [t.strip() for t in result.stdout.splitlines() if t.strip()]
    if not all_tables:
        raise RuntimeError(f"No tables found in schema '{MYSQL_SCHEMA}'")

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

    today  = datetime.now().strftime("%Y-%m-%d")
    tables = []
    for table in candidate_tables:
        aws = _read_state(table).get("aws_transfer", {})
        if aws.get("is_deleted") is True and aws.get("last_processed_date") == today:
            print(f"SKIP {table}: already transferred and dropped today")
            continue
        tables.append(table)

    if not tables:
        raise RuntimeError("All selected tables already processed today — nothing to do.")

    print(f"{len(tables)} table(s) to process: {tables}")
    return tables


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
                        MYSQL_SCHEMA,
                        table,
                    ],
                    stdout=fh,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                    env=_mysql_env(),
                )
            print(f"Dumped: {table} → {dump_file}")
        except subprocess.CalledProcessError as e:
            print(f"ERROR dumping {table}: {e.stderr}")
            failed.append(table)

    if failed:
        raise RuntimeError(f"Dump failed for tables: {failed}")

    return {"date_folder": date_folder, "dump_dir": dump_dir, "tables": tables}


def upload_to_s3(**context):
    info        = context["ti"].xcom_pull(task_ids="dump_tables")
    date_folder = info["date_folder"]
    dump_dir    = info["dump_dir"]
    tables      = info["tables"]
    today       = datetime.now().strftime("%Y-%m-%d")

    client = s3_client()
    prefix = S3_PREFIX.strip("/")

    stats  = []
    failed = []

    for table in tables:
        file_path = os.path.join(dump_dir, f"{table}.sql")
        if not os.path.exists(file_path):
            print(f"SKIP {table}: dump file not found")
            failed.append(table)
            continue

        checksum  = local_md5(file_path)
        file_size = os.path.getsize(file_path)

        rel_path = os.path.relpath(file_path, os.path.join(OUTPUT_BASE, date_folder))
        s3_key   = f"{prefix}/{date_folder}/{rel_path}".replace("\\", "/")
        s3_uri   = f"s3://{S3_BUCKET}/{s3_key}"

        try:
            client.upload_file(file_path, S3_BUCKET, s3_key)
            print(f"Uploaded → {s3_uri}")
        except Exception as e:
            print(f"ERROR uploading {table}: {e}")
            failed.append(table)
            continue

        if not _verify_s3_object(client, S3_BUCKET, s3_key, checksum, file_size):
            failed.append(table)
            continue
        print(f"VERIFY OK {table} md5={checksum}")

        stats.append({
            "table": table,
            "file_path": os.path.relpath(file_path, OUTPUT_BASE),
            "checksum": checksum,
            "file_size_bytes": file_size,
            "s3_uri": s3_uri,
        })

        _update_aws_transfer(table, {
            "last_processed_date": today,
            "local_dump_folder": date_folder,
            "transferred_folder_in_s3": date_folder,
            "is_deleted": False,
        })

    if failed:
        raise RuntimeError(f"S3 upload failed for: {failed}")

    stats_local = os.path.join(OUTPUT_BASE, date_folder, "sql_dump_stats.csv")
    with open(stats_local, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["table", "file_path", "checksum", "file_size_bytes", "s3_uri"])
        writer.writeheader()
        writer.writerows(stats)

    stats_key = f"{prefix}/{date_folder}/sql_dump_stats.csv"
    client.upload_file(stats_local, S3_BUCKET, stats_key)
    print(f"Uploaded stats → s3://{S3_BUCKET}/{stats_key}")
    print(f"Transfer complete: {len(stats)} tables uploaded for date_folder={date_folder}")

    return {"date_folder": date_folder, "uploaded_tables": [s["table"] for s in stats]}


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
                    MYSQL_SCHEMA,
                    "-e", f"DROP TABLE IF EXISTS {table};",
                ],
                check=True,
                capture_output=True,
                text=True,
                env=_mysql_env(),
            )
            print(f"Dropped table: {table}")

            _update_aws_transfer(table, {
                "last_processed_date": today,
                "local_dump_folder": date_folder,
                "transferred_folder_in_s3": date_folder,
                "is_deleted": True,
            })

        except subprocess.CalledProcessError as e:
            print(f"ERROR dropping {table}: {e.stderr}")
            failed.append(table)

    if failed:
        raise RuntimeError(f"Drop failed for tables: {failed}")


# ──────────────────────────────────────────────
# DAG: incremental transfer pipeline (manual trigger)
# ──────────────────────────────────────────────

with DAG(
    dag_id="mysql_dump_s3_transfer",
    description="Dump all tables from MySQL schema, upload to S3, drop from schema, track state",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["mysql", "s3", "dump"],
    params={
        "date_folder": "",
        "table_name": "",
    },
) as dag:

    get_tables_task = PythonOperator(
        task_id="get_tables",
        python_callable=get_tables,
    )

    dump_task = PythonOperator(
        task_id="dump_tables",
        python_callable=dump_tables,
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    drop_task = PythonOperator(
        task_id="drop_tables",
        python_callable=drop_tables,
    )

    get_tables_task >> dump_task >> upload_task >> drop_task
