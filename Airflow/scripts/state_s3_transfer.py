#!/usr/bin/env python
"""
STAGE 5 — aws_transfer: state-gated S3 transfer for the deid MySQL schema.

Three modes (one DAG task each), all scoped to the tables in --tables-filepath
and tracked per table under the `aws_transfer` key of {STATE_DIR}/{table}.json:

  --mode upload : mysqldump + upload each scoped table to S3 (skip if already
                  sealed and pending delete → let delete run first). Each call
                  picks the first date-suffixed S3 folder (MMDDYYYY, _1, _2, ...)
                  with zero existing objects — a same-day retry never resumes an
                  old folder, it always lands in a fresh one. After every table
                  that needed upload this call has succeeded (or failed), writes
                  and uploads sql_dump_stats.csv for whichever tables DID
                  succeed — a partial failure no longer blocks publishing the
                  manifest for the rest; the failed tables just stay pending for
                  a later call, which will publish its own manifest in its own
                  fresh folder. One logical day's batch can end up described by
                  more than one manifest across more than one folder.
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
  transfer_status              : "started" | "success"   (upload result, bookkeeping only)
  last_processed_date          : "YYYY-MM-DD"            (run_date of last successful upload)
  is_deleted                   : bool                    (MySQL-side DROP done)
  is_sql_dump_stats_csv_uploaded : bool  (sealed — either a no-op, or this table's row made it
                                          into a successfully-published sql_dump_stats.csv)
  upload_folder                : str    (which S3 date folder this table's current cycle used,
                                          e.g. "08072026" or "08072026_1")
  upload_Message / delete_Message : str | None

should_upload/should_delete gate ONLY on is_deleted / is_sql_dump_stats_csv_uploaded — not on
transfer_status or last_processed_date, which are kept for display/debugging only.
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

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from sqlalchemy import create_engine, inspect, text  # noqa: E402

from db_config import STATE_DIR  # noqa: E402
from db_url import make_conn_url  # noqa: E402

# Reuse the proven dump / S3 helpers (they take explicit args / use AWS-only globals).
from s3_transfer import _s3_client, _dump_one_table, _md5  # noqa: E402

# ── Credentials ──────────────────────────────────────────────────────────────
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "3306")
DB_USER = os.environ.get("DB_USER", "")
DB_PASS = os.environ.get("DB_PASS", "")

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
    """run_date is accepted for call-site compatibility but not inspected — a
    same-day re-run is disambiguated by which S3 folder gets picked (see
    _select_upload_folder), not by comparing dates."""
    if aws.get("is_deleted", False):
        return True                                    # fully done & deleted -> fresh/normal run
    if aws.get("is_sql_dump_stats_csv_uploaded", False):
        return False                                   # sealed, pending delete only
    return True                                        # not sealed, not deleted -> (re)upload


def should_delete(aws: dict) -> bool:
    return aws.get("is_sql_dump_stats_csv_uploaded", False) and not aws.get("is_deleted", False)


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


def _table_has_rows(engine, schema: str, table: str) -> bool:
    """Cheap existence check — short-circuits on the first row, unlike COUNT(*).

    Uses the dialect's identifier preparer (not raw f-string interpolation) to
    quote schema/table into the query, so an unusual identifier can't break out
    of the intended identifier position."""
    prep = engine.dialect.identifier_preparer
    qualified = f"{prep.quote(schema)}.{prep.quote(table)}"
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT 1 FROM {qualified} LIMIT 1"))
        return result.first() is not None


def _select_upload_folder(client, run_date: str) -> str:
    """Probe S3 for the first date-folder (plain, then _1, _2, ...) with zero
    existing objects under it, and return that folder name (e.g. "08072026" or
    "08072026_1"). Any existing content at all disqualifies a folder from
    reuse — a same-day retry always lands in a fresh folder rather than
    resuming one that might already hold a partial, unsealed batch."""
    base = _date_folder(run_date)
    n = 0
    while True:
        candidate = base if n == 0 else f"{base}_{n}"
        prefix = f"{S3_INCREMENTAL_PREFIX}/{candidate}/"
        resp = client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1)
        if resp.get("KeyCount", 0) == 0:
            return candidate
        n += 1


# ── Upload manifest (sql_dump_stats.csv) ────────────────────────────────────────
def _write_upload_manifest(rows: list[dict], upload_folder: str, stats_prefix: str, client) -> None:
    """
    Write + upload the stats CSV for whichever tables succeeded a real upload
    in THIS call.

    Only ever contains rows for tables that succeeded in THIS call — a call
    with some failures still writes this for whichever tables DID succeed (the
    failure is surfaced separately, via run_upload raising after this
    completes). A later retry of the still-failed tables publishes its OWN
    manifest in a fresh folder (see _select_upload_folder) — one logical day's
    batch can end up described by more than one manifest.

    upload_folder is the same folder this call's .sql payload landed in (see
    run_upload) — stats_prefix defaults to the S3 path for that same folder.
    """
    if not rows:
        logger.info("No tables uploaded this run — skipping %s", STATS_FILENAME)
        return

    rows.sort(key=lambda r: r["file_path"])  # deterministic regardless of completion order

    # Local copy sits in the run's dated folder, next to count_report.csv.
    local_dir = os.path.join(BASE_PATH, upload_folder)
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
def run_upload(schema, tables, run_date, max_workers, destination_prefix=None, stats_prefix=None):
    sm = StateManager(STATE_DIR)
    client = _s3_client()

    engine = _engine(schema)
    existing = set(inspect(engine).get_table_names())

    # Skip the S3 probe (and creating a local folder) entirely when nothing in
    # scope actually needs uploading — e.g. every scoped table is already
    # sealed and pending-delete only.
    pending = [t for t in tables if should_upload(sm.load(t).get("aws_transfer", {}), run_date)]
    if not pending:
        logger.info("Nothing to upload — all %d scoped table(s) already sealed/pending-delete.",
                    len(tables))
        return

    # Only probe S3 for a fresh folder when the caller hasn't explicitly pinned
    # one (an explicit --destination_prefix/--stats_prefix override bypasses
    # this and behaves like before). Otherwise: find the first date-suffixed
    # folder with zero existing objects (see _select_upload_folder) — this is
    # what makes a same-day retry land in a fresh folder instead of silently
    # overwriting whatever the previous attempt already wrote.
    if destination_prefix is None and stats_prefix is None:
        upload_folder = _select_upload_folder(client, run_date)
        destination_prefix = f"{S3_INCREMENTAL_PREFIX}/{upload_folder}"
        stats_prefix = destination_prefix
    else:
        destination_prefix = destination_prefix or _default_data_prefix(run_date)
        stats_prefix = stats_prefix or _default_stats_prefix(run_date)
        upload_folder = _date_folder(run_date)

    schema_dir = os.path.join(BASE_PATH, upload_folder, schema)
    os.makedirs(schema_dir, exist_ok=True)

    def _one(table):
        # Thin catch-all wrapper around _process: anything unexpected (a
        # transient DB error in _table_has_rows, a disk error in sm.load/save,
        # etc.) must be isolated to THIS table, not propagate out of the
        # ThreadPoolExecutor future and abort the whole call — that would skip
        # the manifest-write/seal step entirely, losing sealing for every OTHER
        # table that already succeeded in this same call.
        try:
            return _process(table)
        except Exception as e:
            logger.error("[%s] unexpected error during upload processing: %s", table, e)
            try:
                state = sm.load(table)
                aws = state.get("aws_transfer", {})
                aws["upload_Message"] = f"unexpected error: {e}"
                state["aws_transfer"] = aws
                sm.save(table, state)
            except Exception:
                pass  # best-effort — don't let a second failure mask the first
            return table, "failed", str(e), None

    def _process(table):
        state = sm.load(table)
        aws = state.get("aws_transfer", {})

        if not should_upload(aws, run_date):
            logger.info("[%s] skip upload (is_deleted=%s, is_sql_dump_stats_csv_uploaded=%s)",
                        table, aws.get("is_deleted"), aws.get("is_sql_dump_stats_csv_uploaded"))
            return table, "skipped", None, None

        if table not in existing:
            # A zero-row source is never materialised by deid, so the table is
            # absent here. Nothing to upload → mark success (no-op), sealed
            # immediately (nothing was ever transferred, so nothing needs a
            # manifest to confirm it). No .sql file, so no manifest row.
            logger.warning("[%s] not present in schema %s → no-op success (nothing to upload)",
                           table, schema)
            aws.update({
                "table": table,
                "transfer_status": "success",
                "last_processed_date": run_date,
                "is_deleted": False,
                "is_sql_dump_stats_csv_uploaded": True,
                "upload_folder": upload_folder,
                "upload_Message": "table not present — nothing to upload",
            })
            state["aws_transfer"] = aws
            sm.save(table, state)
            return table, "success", None, None

        if not _table_has_rows(engine, schema, table):
            logger.warning("[%s] present but 0 rows in schema %s → no-op success", table, schema)
            aws.update({
                "table": table,
                "transfer_status": "success",
                "last_processed_date": run_date,
                "is_deleted": False,
                "is_sql_dump_stats_csv_uploaded": True,
                "upload_folder": upload_folder,
                "upload_Message": "table present, 0 rows — no-op",
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

            # is_sql_dump_stats_csv_uploaded stays False here — a real upload
            # only gets sealed once the manifest covering it is actually
            # published (below), unlike a no-op which has nothing to confirm.
            aws.update({
                "transfer_status": "success",
                "last_processed_date": run_date,
                "is_deleted": False,
                "upload_folder": upload_folder,
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

    failed, manifest, real_upload_successes = [], [], []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for fut in as_completed({ex.submit(_one, t): t for t in tables}):
            table, outcome, err, row = fut.result()
            if outcome == "failed":
                failed.append(table)
            elif row is not None:
                manifest.append(row)
                real_upload_successes.append(table)

    # Write (and seal) the manifest for whoever succeeded a real upload this
    # call, REGARDLESS of whether anyone else failed — a partial failure no
    # longer blocks publishing the manifest for the rest. The failure itself
    # still surfaces below, after this.
    if manifest:
        _write_upload_manifest(manifest, upload_folder, stats_prefix, client)
        for table in real_upload_successes:
            state = sm.load(table)
            aws = state.get("aws_transfer", {})
            aws["is_sql_dump_stats_csv_uploaded"] = True
            state["aws_transfer"] = aws
            sm.save(table, state)
        logger.info("Sealed %d table(s) in folder %s", len(real_upload_successes), upload_folder)

    if failed:
        raise RuntimeError(f"S3 upload failed for: {failed}")

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

    # argparse's required=True only enforces the flag is PRESENT, not non-empty —
    # a caller passing --schema "" (e.g. an unset env var upstream) satisfies it
    # trivially. An empty schema makes get_table_names() return nothing, which
    # run_upload/run_count/run_delete then treat as "table not present" for
    # every table — a no-op that reports success having done nothing. Fail here
    # instead, at the one place every mode passes through.
    if not args.schema:
        raise RuntimeError("--schema is empty — check the env var it was sourced from.")

    tables = _read_tables(args.tables_filepath)
    if not tables:
        logger.info("No scoped tables in %s — nothing to do.", args.tables_filepath)
        return

    if args.mode == "upload":
        if not S3_BUCKET:
            raise RuntimeError("S3_BUCKET must be set in .env")
        run_upload(args.schema, tables, args.run_date, args.max_workers,
                   destination_prefix=args.destination_prefix,
                   stats_prefix=args.stats_prefix)
    elif args.mode == "count":
        if not COUNT_SCHEMA:
            raise RuntimeError("DEID_BASE_NAME must be set in .env (count mode reads "
                              "row counts from the deid base schema).")
        run_count(args.schema, tables, args.run_date,
                  args.count_prefix or _default_count_prefix(args.run_date))
    elif args.mode == "delete":
        run_delete(args.schema, tables, args.max_workers)


if __name__ == "__main__":
    main()
