#!/usr/bin/env python
"""
S3 Transfer Script

Dumps MySQL tables to SQL files and uploads them to AWS S3.
Only non-empty tables (COUNT(*) >= 1) are included. Dump and upload run in parallel.

Usage:
    python s3_transfer.py --schema "deidentified_staging" --run_date "2026-05-07"
"""

import os
import sys
import argparse
import logging
import hashlib
import subprocess
import pandas as pd
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import unquote
from sqlalchemy import create_engine, inspect, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Load DB / AWS credentials from .env (same directory as this script)
# ---------------------------------------------------------------------------

def _load_env_file(path: str) -> None:
    """Parse KEY="VALUE" or KEY=VALUE .env file into os.environ."""
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)
    except FileNotFoundError:
        pass


_load_env_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "3306")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")

AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID") or None
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY") or None
AWS_REGION            = os.environ.get("AWS_REGION") or None
S3_BUCKET             = os.environ.get("S3_BUCKET") or None
# Base S3 folder (per-run date folder appended by the caller). Maintained
# centrally in .env; differs per EHR (e.g. "EHR", "tables").
S3_DESTINATION_PREFIX = os.environ.get("S3_DESTINATION_PREFIX", "EHR").strip("/")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _s3_client():
    """Return a boto3 S3 client. Uses .env creds if set, otherwise falls back to the default AWS credential chain."""
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def _md5(file_path: str) -> str:
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Table discovery — parallel COUNT(*) to filter empty tables
# ---------------------------------------------------------------------------

def _count_table_rows(engine, schema: str, table: str) -> tuple[str, int]:
    """Return (table, row_count). Returns -1 on query error."""
    try:
        with engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM `{schema}`.`{table}`")).scalar()
            return table, count
    except Exception as e:
        logger.warning(f"Could not count rows in `{table}`: {e}")
        return table, -1


def discover_tables_in_schema(
    schema: str,
    host: str = DB_HOST,
    port: str = DB_PORT,
    username: str = DB_USER,
    password: str = DB_PASS,
    max_workers: int = 8,
) -> list[str]:
    """
    Return all tables in schema that have at least 1 row.
    COUNT(*) queries run in parallel for speed.
    """
    try:
        # @ in password must be percent-encoded in the SQLAlchemy URL
        connection_str = (
            f"mysql+pymysql://{username}:{password.replace('@', '%40')}@{host}:{port}/{schema}"
        )
        engine = create_engine(
            connection_str,
            pool_recycle=3600,
            pool_pre_ping=True,
            pool_size=max_workers,
            max_overflow=0,
        )

        all_tables = inspect(engine).get_table_names()
        logger.info(
            f"Found {len(all_tables)} total tables in '{schema}', "
            f"checking row counts (max_workers={max_workers})..."
        )

        non_empty: list[str] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_count_table_rows, engine, schema, t): t
                for t in all_tables
            }
            for future in as_completed(futures):
                table, count = future.result()
                if count > 0:
                    non_empty.append(table)
                elif count == 0:
                    logger.info(f"Skipping empty table: `{table}`")

        non_empty.sort()
        logger.info(
            f"✅ {len(non_empty)} non-empty tables discovered "
            f"({len(all_tables) - len(non_empty)} empty skipped)"
        )
        return non_empty

    except Exception as e:
        logger.error(f"❌ Failed to discover tables in schema '{schema}': {e}")
        raise


# ---------------------------------------------------------------------------
# Dump — parallel mysqldump
# ---------------------------------------------------------------------------

def _dump_one_table(
    schema: str, table: str, schema_dir: str,
    host: str, port: str, username: str, password: str,
) -> tuple[str, str | None]:
    """Dump one table to {schema_dir}/{table}.sql. Returns (table, error_or_None)."""
    os.makedirs(schema_dir, exist_ok=True)
    dump_file = os.path.join(schema_dir, f"{table}.sql")
    command = [
        "mysqldump",
        "--default-character-set=utf8",
        "-h", host,
        "-P", port,
        "-u", username,
        f"-p{unquote(password)}",   # decode %40 → @ for mysqldump (not a URL context)
        schema,
        table,
    ]
    try:
        with open(dump_file, "w", encoding="utf-8") as f:
            result = subprocess.run(command, stdout=f, stderr=subprocess.PIPE, check=False, text=True)
        if result.returncode != 0:
            err = result.stderr.strip()
            logger.error(f"❌ Error dumping `{table}`: {err}")
            return table, err
        logger.info(f"✅ Dumped: `{table}` → {dump_file}")
        return table, None
    except Exception as e:
        logger.error(f"❌ Failed to dump `{table}`: {e}")
        return table, str(e)


def dump_tables_from_schema(
    schema: str,
    date_folder: str,
    base_path: str = None,
    host: str = DB_HOST,
    port: str = DB_PORT,
    username: str = DB_USER,
    password: str = DB_PASS,
    max_workers: int = 4,
) -> dict:
    """Discover non-empty tables and dump them in parallel."""
    stats = {"total_tables": 0, "successful": 0, "failed": 0, "failed_tables": []}

    try:
        tables = discover_tables_in_schema(
            schema=schema, host=host, port=port,
            username=username, password=password,
            max_workers=max_workers,
        )
        stats["total_tables"] = len(tables)

        if not tables:
            logger.warning("⚠️ No non-empty tables found in schema")
            return stats

        schema_dir = (
            os.path.join(base_path, date_folder, schema)
            if base_path
            else os.path.join(date_folder, schema)
        )
        os.makedirs(schema_dir, exist_ok=True)

        logger.info(f"Dumping {len(tables)} table(s) in parallel (max_workers={max_workers})...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _dump_one_table, schema, table, schema_dir,
                    host, port, username, password,
                ): table
                for table in tables
            }
            for future in as_completed(futures):
                table, err = future.result()
                if err:
                    stats["failed"] += 1
                    stats["failed_tables"].append(table)
                else:
                    stats["successful"] += 1

        logger.info(
            f"✅ Dump completed: total={stats['total_tables']}, "
            f"successful={stats['successful']}, failed={stats['failed']}"
        )
        if stats["failed_tables"]:
            logger.warning(f"⚠️ Failed tables: {stats['failed_tables']}")

        return stats

    except Exception as e:
        logger.error(f"❌ Failed to dump tables from schema: {e}")
        raise


# ---------------------------------------------------------------------------
# S3 upload — parallel boto3
# ---------------------------------------------------------------------------

def _upload_one_file(
    file_path: str,
    full_source_folder: str,
    anchor: str,
    bucket_name: str,
    destination_prefix: str,
    s3_client,
) -> dict | None:
    """Checksum, stat, and upload one .sql file to S3. Returns file_data row or None on failure."""
    try:
        checksum = _md5(file_path)
        file_size_bytes = os.path.getsize(file_path)
        relative_file_path = os.path.relpath(file_path, anchor)
        s3_key = (
            f"{destination_prefix}/"
            f"{os.path.relpath(file_path, full_source_folder).replace(chr(92), '/')}"
        )
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logger.info(f"✅ Uploaded → s3://{bucket_name}/{s3_key}")
        return {
            "file_path": relative_file_path,
            "checksum": checksum,
            "file_size_bytes": file_size_bytes,
        }
    except Exception as e:
        logger.error(f"❌ Failed to upload {file_path}: {e}")
        return None


def upload_to_s3(
    bucket_name: str,
    source_folder: str,
    destination_prefix: str,
    date_folder: str,
    base_path: str = None,
    max_workers: int = 8,
) -> dict:
    """
    Upload .sql files to S3 in parallel, then upload sql_dump_stats.csv last.
    The stats CSV presence signals the restore pipeline that transfer is complete.
    """
    stats = {"total_files": 0, "successful": 0, "failed": 0, "failed_files": []}

    try:
        full_source_folder = (
            os.path.join(base_path, source_folder) if base_path else source_folder
        )
        anchor = base_path if base_path else "."
        client = _s3_client()

        sql_files = sorted([
            os.path.join(root, file_name)
            for root, _, files in os.walk(full_source_folder)
            for file_name in files
            if file_name.endswith(".sql")
        ])

        stats["total_files"] = len(sql_files)
        logger.info(
            f"Uploading {len(sql_files)} file(s) to "
            f"s3://{bucket_name}/{destination_prefix}/ (max_workers={max_workers})..."
        )

        file_data: list[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _upload_one_file, fp, full_source_folder, anchor,
                    bucket_name, destination_prefix, client,
                ): fp
                for fp in sql_files
            }
            for future in as_completed(futures):
                row = future.result()
                if row is not None:
                    file_data.append(row)
                    stats["successful"] += 1
                else:
                    stats["failed"] += 1
                    stats["failed_files"].append(futures[future])

        # Sort for a deterministic stats CSV regardless of completion order
        file_data.sort(key=lambda r: r["file_path"])

        # Stats CSV — uploaded after all .sql files so its presence marks transfer complete
        if file_data:
            stats_local = (
                os.path.join(base_path, date_folder, "sql_dump_stats.csv")
                if base_path
                else os.path.join(date_folder, "sql_dump_stats.csv")
            )
            pd.DataFrame(file_data).to_csv(stats_local, index=False)

            stats_s3_key = f"{destination_prefix}/sql_dump_stats.csv"
            try:
                client.upload_file(stats_local, bucket_name, stats_s3_key)
                logger.info(f"✅ Uploaded stats → s3://{bucket_name}/{stats_s3_key}")
            except Exception as e:
                logger.error(f"❌ Failed to upload stats CSV: {e}")

        logger.info(
            f"✅ S3 upload completed: total={stats['total_files']}, "
            f"successful={stats['successful']}, failed={stats['failed']}"
        )
        if stats["failed_files"]:
            logger.warning(f"⚠️ Failed files: {stats['failed_files']}")

        return stats

    except Exception as e:
        logger.error(f"❌ Failed to upload to S3: {e}")
        raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Dump MySQL tables and upload to AWS S3")

    parser.add_argument("--schema", required=True, help="Database schema name")
    parser.add_argument("--run_date", required=True, help="Run date in YYYY-MM-DD format")
    parser.add_argument(
        "--bucket",
        default=None,
        help="S3 bucket name. Overrides S3_BUCKET in .env.",
    )
    parser.add_argument(
        "--base_path",
        default=os.environ.get("DATA_TRANSFER_BASE_PATH", "/Volumes/NDAIVol/S3 Transfer"),
        help="Base directory path for storing local dumps",
    )
    parser.add_argument(
        "--destination_prefix",
        help="S3 key prefix (default: 'tables/{date_folder}')",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=4,
        help=(
            "Parallel workers for COUNT(*) discovery, mysqldump, and S3 upload (default: 4). "
            "Each dump worker spawns a separate mysqldump process — increase carefully."
        ),
    )

    parser.add_argument("--mysql_host", default=DB_HOST)
    parser.add_argument("--mysql_port", default=DB_PORT)
    parser.add_argument("--mysql_user", default=DB_USER)
    parser.add_argument("--mysql_pass", default=DB_PASS)

    args = parser.parse_args()

    bucket = args.bucket or S3_BUCKET
    if not bucket:
        parser.error("S3 bucket name must be set via --bucket or S3_BUCKET in .env")

    try:
        try:
            date_folder = datetime.strptime(args.run_date, "%Y-%m-%d").strftime("%m%d%Y")
        except ValueError:
            logger.error(f"❌ Invalid run_date format: {args.run_date}. Expected YYYY-MM-DD")
            sys.exit(1)

        os.makedirs(args.base_path, exist_ok=True)
        destination_prefix = args.destination_prefix or f"{S3_DESTINATION_PREFIX}/{date_folder}"

        logger.info(
            f"Starting S3 transfer: schema={args.schema}, date={args.run_date}, "
            f"bucket={bucket}, max_workers={args.max_workers}"
        )

        dump_stats = dump_tables_from_schema(
            schema=args.schema,
            date_folder=date_folder,
            base_path=args.base_path,
            host=args.mysql_host,
            port=args.mysql_port,
            username=args.mysql_user,
            password=args.mysql_pass,
            max_workers=args.max_workers,
        )

        if dump_stats["failed"] > 0:
            logger.warning("⚠️ Some table dumps failed, continuing with upload...")

        upload_stats = upload_to_s3(
            bucket_name=bucket,
            source_folder=date_folder,
            destination_prefix=destination_prefix,
            date_folder=date_folder,
            base_path=args.base_path,
            max_workers=args.max_workers,
        )

        if dump_stats["failed"] > 0 or upload_stats["failed"] > 0:
            logger.warning("⚠️ Completed with some failures")
            sys.exit(1)

        logger.info("✅ S3 transfer completed successfully")

    except Exception as e:
        logger.error(f"❌ S3 transfer failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
