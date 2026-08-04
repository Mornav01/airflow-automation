#!/usr/bin/env python
"""
STAGE 5 — aws_transfer: state-gated S3 transfer for the deid MySQL schema.

Three modes (one DAG task each), all scoped to the tables in --tables-filepath
and tracked per table under the `aws_transfer` key of {STATE_DIR}/{table}.json:

  --mode upload : mysqldump + upload each scoped table to S3 (skip if already
                  done today, or if a prior upload is pending delete → let
                  delete run first). After every scoped .sql file has landed,
                  writes and uploads sql_dump_stats.csv — its presence signals
                  the restore pipeline that the transfer is complete.
  --mode count  : refresh a cumulative count CSV — query ONLY the scoped tables,
                  carry over all other rows from the previous CSV (no DB query),
                  scoped wins on clash — then upload the CSV to S3.
  --mode delete : DROP each scoped table from the MySQL schema once its upload
                  succeeded (idempotent / resumable).

S3 layout — both prefixes come straight from .env, dated by --run_date:

  {S3_INCREMENTAL_DUMP_PATH_PREFIX}/{MMDDYYYY}/{schema}/{table}.sql
  {S3_INCREMENTAL_DUMP_PATH_PREFIX}/{MMDDYYYY}/sql_dump_stats.csv
  {S3_COUNT_REPORT_PATH_PREFIX}/{MMDDYYYY}/count_report.csv

i.e. with S3_INCREMENTAL_DUMP_PATH_PREFIX=EHR/nd_incremental and
     S3_COUNT_REPORT_PATH_PREFIX=EHR/merge_stats:

  EHR/nd_incremental/08042026/deidentified_staging/enc.sql
  EHR/nd_incremental/08042026/sql_dump_stats.csv
  EHR/merge_stats/08042026/count_report.csv

Every date in this script derives from --run_date. The host clock is read in
exactly one place: the --run_date default, for manual invocations.

Per-table `aws_transfer` state:
  transfer_status      : "started" | "success"   (upload result)
  last_processed_date  : "YYYY-MM-DD"            (run_date of last successful upload — daily gate)
  is_deleted           : bool                    (MySQL-side DROP done)
  upload_Message / delete_Message : str | None
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from urllib.parse import unquote

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from sqlalchemy import create_engine, inspect, text  # noqa: E402

from db_config import STATE_DIR  # noqa: E402
from db_url import make_conn_url  # noqa: E402

# Reuse the proven dump / S3 helpers (they take explicit args / use AWS-only globals).
from s3_transfer import _s3_client, _dump_one_table, _md5  # noqa: E402

# ── Credentials (unquote DB_PASS — tolerate a legacy %40-encoded value) ─────────
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "3306")
DB_USER = os.environ.get("DB_USER", "")
DB_PASS = unquote(os.environ.get("DB_PASS", ""))

S3_BUCKET = os.environ.get("S3_BUCKET") or None

# Full S3 key prefixes, maintained centrally in .env (each already includes the
# top-level EHR folder). A dated MMDDYYYY folder is appended per run.
#   S3_INCREMENTAL_DUMP_PATH_PREFIX → the .sql payload + sql_dump_stats.csv
#   S3_COUNT_REPORT_PATH_PREFIX     → count_report.csv
S3_INCREMENTAL_PREFIX = os.environ.get(
    "S3_INCREMENTAL_DUMP_PATH_PREFIX", "EHR/nd_incremental").strip("/")
S3_COUNT_PREFIX = os.environ.get(
    "S3_COUNT_REPORT_PATH_PREFIX", "EHR/merge_stats").strip("/")

BASE_PATH = os.environ.get("DATA_TRANSFER_BASE_PATH", "")

# Row counts for the report come from the deid BASE schema (the full accumulator),
# NOT the diff-merged upload schema — the uploaded data is further merged into the
# base on the AWS side, so row comparison (local vs AWS) is against the base on both.
COUNT_SCHEMA = os.environ.get("DEID_BASE_NAME")

# Cumulative count CSV persisted across runs. Uploaded to
#     {S3_COUNT_REPORT_PATH_PREFIX}/{MMDDYYYY of run_date}/count_report.csv
COUNT_DIR = os.path.join(STATE_DIR, "aws_transfer")
COUNT_CSV = os.path.join(COUNT_DIR, "count_report.csv")
COUNT_FIELDS = ["table_name", "row_count_total", "row_count_active", "snapshot_date", "created_at"]

# Manifest of the .sql files uploaded in THIS run (scoped tables only — skipped
# and no-op tables are omitted). Schema matches s3_transfer.py's stats CSV so the
# restore side needs no changes. Sits in the same dated folder as the payload it
# describes:
#     {S3_INCREMENTAL_DUMP_PATH_PREFIX}/{MMDDYYYY of run_date}/sql_dump_stats.csv
STATS_FILENAME = "sql_dump_stats.csv"
STATS_FIELDS = ["file_path", "checksum", "file_size_bytes"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


# ── State manager (owns only the aws_transfer key) ──────────────────────────────
class StateManager:
    def __init__(self, state_dir: str):
        self.state_dir = state_dir
        os.makedirs(self.state_dir, exist_ok=True)

    def _path(self, table: str) -> str:
        return os.path.join(self.state_dir, f"{table}.json")

    def load(self, table: str) -> dict:
        path = self._path(table)
        if not os.path.exists(path):
            return {"aws_transfer": {}}
        with open(path) as f:
            state = json.load(f)
        state.setdefault("aws_transfer", {})  # preserve every other key (diff_merge, deidentification)
        return state

    def save(self, table: str, state: dict) -> None:
        with open(self._path(table), "w") as f:
            json.dump(state, f, indent=4)


# ── Gates ───────────────────────────────────────────────────────────────────────
def should_upload(aws: dict, run_date: str) -> bool:
    status = aws.get("transfer_status")
    if status != "success":
        return True                              # never uploaded / retry
    if not aws.get("is_deleted", False):
        return False                             # pending delete → let delete run first
    if aws.get("last_processed_date") == run_date:
        return False                             # already fully done for this run_date
    return True                                  # deleted on an older day → new cycle


def should_delete(aws: dict) -> bool:
    return aws.get("transfer_status") == "success" and not aws.get("is_deleted", False)


# ── Small helpers ────────────────────────────────────────────────────────────────
def _today() -> str:
    """Only used as the --run_date fallback for manual invocations."""
    return date.today().isoformat()


def _read_tables(path: str) -> list[str]:
    with open(path, newline="") as f:
        return [r[0].strip() for r in csv.reader(f) if r and r[0].strip()]


def _engine(schema: str):
    url = make_conn_url("mysql+pymysql", username=DB_USER, password=DB_PASS,
                        host=DB_HOST, port=DB_PORT, database=schema)
    return create_engine(url, pool_pre_ping=True)


def _date_folder(run_date: str) -> str:
    return datetime.strptime(run_date, "%Y-%m-%d").strftime("%m%d%Y")


def _default_data_prefix(run_date: str) -> str:
    """EHR/nd_incremental/MMDDYYYY — holds {schema}/{table}.sql."""
    return f"{S3_INCREMENTAL_PREFIX}/{_date_folder(run_date)}"


def _default_stats_prefix(run_date: str) -> str:
    """Same folder as the .sql payload — holds sql_dump_stats.csv."""
    return f"{S3_INCREMENTAL_PREFIX}/{_date_folder(run_date)}"


def _default_count_prefix(run_date: str) -> str:
    """EHR/merge_stats/MMDDYYYY — holds count_report.csv."""
    return f"{S3_COUNT_PREFIX}/{_date_folder(run_date)}"


# ── Upload manifest (sql_dump_stats.csv) ────────────────────────────────────────
def _write_upload_manifest(rows: list[dict], run_date: str, stats_prefix: str, client) -> None:
    """
    Write + upload the stats CSV for the tables uploaded in THIS run.

    Called only after every scoped .sql file has uploaded successfully, so the
    object's presence under {stats_prefix}/ is a reliable "transfer complete"
    marker for the restore pipeline.

    stats_prefix defaults to {S3_INCREMENTAL_DUMP_PATH_PREFIX}/{MMDDYYYY of
    run_date} — the same folder as the .sql payload it describes. Every date in
    this script comes from --run_date; the host clock is never consulted, so a
    UTC-vs-local skew can't split the marker from the data.
    """
    if not rows:
        logger.info("No tables uploaded this run — skipping %s", STATS_FILENAME)
        return

    rows.sort(key=lambda r: r["file_path"])  # deterministic regardless of completion order

    # Local copy sits in the run's dated folder, next to count_report.csv.
    local_dir = os.path.join(BASE_PATH, _date_folder(run_date))
    os.makedirs(local_dir, exist_ok=True)
    local = os.path.join(local_dir, STATS_FILENAME)

    with open(local, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=STATS_FIELDS)
        w.writeheader()
        w.writerows(rows)

    s3_key = f"{stats_prefix.strip('/')}/{STATS_FILENAME}"
    client.upload_file(local, S3_BUCKET, s3_key)
    logger.info("Manifest: %d file(s) → s3://%s/%s", len(rows), S3_BUCKET, s3_key)


# ── Mode: upload ─────────────────────────────────────────────────────────────────
def run_upload(schema, tables, run_date, destination_prefix, max_workers, stats_prefix=None):
    sm = StateManager(STATE_DIR)
    date_folder = _date_folder(run_date)
    schema_dir = os.path.join(BASE_PATH, date_folder, schema)
    os.makedirs(schema_dir, exist_ok=True)

    engine = _engine(schema)
    existing = set(inspect(engine).get_table_names())
    client = _s3_client()

    def _one(table):
        state = sm.load(table)
        aws = state.get("aws_transfer", {})

        if not should_upload(aws, run_date):
            logger.info("[%s] skip upload (status=%s, is_deleted=%s, date=%s)",
                        table, aws.get("transfer_status"), aws.get("is_deleted"),
                        aws.get("last_processed_date"))
            return table, "skipped", None, None

        if table not in existing:
            # A zero-row source is never materialised by deid, so the table is
            # absent here. Nothing to upload → mark success (no-op) so the
            # lifecycle completes (delete becomes a no-op DROP IF EXISTS).
            # No .sql file exists, so it contributes no manifest row.
            logger.warning("[%s] not present in schema %s → no-op success (nothing to upload)",
                           table, schema)
            aws.update({
                "table": table,
                "transfer_status": "success",
                "last_processed_date": run_date,
                "is_deleted": False,
                "upload_Message": "table not present — nothing to upload",
            })
            state["aws_transfer"] = aws
            sm.save(table, state)
            return table, "success", None, None

        aws["transfer_status"] = "started"
        aws["table"] = table
        state["aws_transfer"] = aws
        sm.save(table, state)

        _, err = _dump_one_table(schema, table, schema_dir, DB_HOST, DB_PORT, DB_USER, DB_PASS)
        if err:
            aws["upload_Message"] = f"dump failed: {err}"
            state["aws_transfer"] = aws
            sm.save(table, state)
            return table, "failed", err, None

        try:
            local = os.path.join(schema_dir, f"{table}.sql")
            s3_key = f"{destination_prefix}/{schema}/{table}.sql"
            client.upload_file(local, S3_BUCKET, s3_key)

            # Checksum/size for the run manifest — computed in the worker thread
            # so the hashing parallelises with the other uploads.
            manifest_row = {
                "file_path": os.path.relpath(local, BASE_PATH or "."),
                "checksum": _md5(local),
                "file_size_bytes": os.path.getsize(local),
            }

            aws.update({
                "transfer_status": "success",
                "last_processed_date": run_date,
                "is_deleted": False,
                "upload_Message": f"uploaded → s3://{S3_BUCKET}/{s3_key}",
            })
            state["aws_transfer"] = aws
            sm.save(table, state)
            logger.info("[%s] uploaded → s3://%s/%s", table, S3_BUCKET, s3_key)
            return table, "success", None, manifest_row
        except Exception as e:
            # keep transfer_status='started' → retry next run
            aws["upload_Message"] = f"upload failed: {e}"
            state["aws_transfer"] = aws
            sm.save(table, state)
            logger.error("[%s] upload failed: %s", table, e)
            return table, "failed", str(e), None

    failed, manifest = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for fut in as_completed({ex.submit(_one, t): t for t in tables}):
            table, outcome, err, row = fut.result()
            if outcome == "failed":
                failed.append(table)
            elif row is not None:
                manifest.append(row)

    # Raise BEFORE writing the manifest: a partial transfer must never publish a
    # "complete" marker.
    if failed:
        raise RuntimeError(f"S3 upload failed for: {failed}")

    _write_upload_manifest(manifest, run_date,
                           stats_prefix or _default_stats_prefix(run_date), client)
    logger.info("Upload complete for %d scoped table(s)", len(tables))


# ── Mode: count (cumulative CSV; only scoped tables queried) ─────────────────────
def _read_prev_counts() -> dict:
    if not os.path.exists(COUNT_CSV):
        return {}
    with open(COUNT_CSV, newline="") as f:
        return {row["table_name"]: row for row in csv.DictReader(f) if row.get("table_name")}


def run_count(schema, tables, run_date, destination_prefix):
    os.makedirs(COUNT_DIR, exist_ok=True)
    # snapshot_date is the run's date; created_at stays wall-clock, since it
    # records when the row was actually written.
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    merged = _read_prev_counts()                 # carry over ALL previous rows (no DB query)

    count_schema = COUNT_SCHEMA                  # count from the deid BASE, not the upload schema
    engine = _engine(count_schema)
    existing = set(inspect(engine).get_table_names())

    queried = 0
    with engine.connect() as conn:
        for table in tables:                     # query ONLY scoped tables; scoped wins on clash
            if table not in existing:
                # Not present in base = zero rows (a zero-row source is never materialised).
                total, active = 0, 0
                logger.warning("[%s] not in base schema %s → recording 0", table, count_schema)
            else:
                total = conn.execute(text(f"SELECT COUNT(*) FROM `{count_schema}`.`{table}`")).scalar() or 0
                try:
                    active = conn.execute(
                        text(f"SELECT COUNT(*) FROM `{count_schema}`.`{table}` WHERE nd_ActiveFlag = 'Y'")
                    ).scalar() or 0
                except Exception:
                    active = ""                  # table has no nd_ActiveFlag column

            merged[table] = {
                "table_name": table,
                "row_count_total": total,
                "row_count_active": active,
                "snapshot_date": run_date,
                "created_at": now,
            }
            queried += 1

    rows = [merged[k] for k in sorted(merged)]

    # canonical (persistent) copy
    with open(COUNT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COUNT_FIELDS)
        w.writeheader(); w.writerows(rows)

    # dated copy + upload
    date_folder = _date_folder(run_date)
    dated_dir = os.path.join(BASE_PATH, date_folder)
    os.makedirs(dated_dir, exist_ok=True)
    dated_csv = os.path.join(dated_dir, "count_report.csv")
    with open(dated_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COUNT_FIELDS)
        w.writeheader(); w.writerows(rows)

    if S3_BUCKET:
        _s3_client().upload_file(dated_csv, S3_BUCKET, f"{destination_prefix}/count_report.csv")

    logger.info("Count report: %d scoped queried, %d total rows → %s", queried, len(rows), COUNT_CSV)


# ── Mode: delete (DROP MySQL deid tables once uploaded) ──────────────────────────
def run_delete(schema, tables, max_workers):
    sm = StateManager(STATE_DIR)
    engine = _engine(schema)

    def _one(table):
        state = sm.load(table)
        aws = state.get("aws_transfer", {})
        if not should_delete(aws):
            logger.info("[%s] skip delete (status=%s, is_deleted=%s)",
                        table, aws.get("transfer_status"), aws.get("is_deleted"))
            return table, None
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS `{schema}`.`{table}`"))
            aws["is_deleted"] = True
            aws["delete_Message"] = f"dropped {schema}.{table}"
            state["aws_transfer"] = aws
            sm.save(table, state)
            logger.info("[%s] dropped from %s", table, schema)
            return table, None
        except Exception as e:
            aws["delete_Message"] = f"delete failed: {e}"
            state["aws_transfer"] = aws
            sm.save(table, state)
            logger.error("[%s] delete failed: %s", table, e)
            return table, str(e)

    failed = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for fut in as_completed({ex.submit(_one, t): t for t in tables}):
            table, err = fut.result()
            if err:
                failed.append(table)

    if failed:
        raise RuntimeError(f"MySQL delete failed for: {failed}")
    logger.info("Delete complete for %d scoped table(s)", len(tables))


# ── Main ─────────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="STAGE 5 aws_transfer (upload/count/delete)")
    p.add_argument("--mode", required=True, choices=["upload", "count", "delete"])
    p.add_argument("--schema", required=True)
    p.add_argument("--tables-filepath", required=True)
    p.add_argument("--run_date", default=_today(), help="YYYY-MM-DD (default: today)")
    p.add_argument("--destination_prefix", default=None,
                   help="S3 prefix for the .sql files "
                        "(default: {S3_INCREMENTAL_DUMP_PATH_PREFIX}/{MMDDYYYY of run_date})")
    p.add_argument("--stats_prefix", default=None,
                   help="S3 prefix for sql_dump_stats.csv "
                        "(default: {S3_INCREMENTAL_DUMP_PATH_PREFIX}/{MMDDYYYY of run_date})")
    p.add_argument("--count_prefix", default=None,
                   help="S3 prefix for count_report.csv "
                        "(default: {S3_COUNT_REPORT_PATH_PREFIX}/{MMDDYYYY of run_date})")
    p.add_argument("--max-workers", type=int, default=4)
    args = p.parse_args()

    tables = _read_tables(args.tables_filepath)
    if not tables:
        logger.info("No scoped tables in %s — nothing to do.", args.tables_filepath)
        return

    if args.mode == "upload":
        if not S3_BUCKET:
            raise RuntimeError("S3_BUCKET must be set in .env")
        run_upload(args.schema, tables, args.run_date,
                   args.destination_prefix or _default_data_prefix(args.run_date),
                   args.max_workers,
                   stats_prefix=args.stats_prefix)
    elif args.mode == "count":
        run_count(args.schema, tables, args.run_date,
                  args.count_prefix or _default_count_prefix(args.run_date))
    elif args.mode == "delete":
        run_delete(args.schema, tables, args.max_workers)


if __name__ == "__main__":
    main()
