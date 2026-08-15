"""
Copy tables PROD_DB → BASE_DB — CROSS-INSTANCE version (v2).

Unlike copy_prod_to_base.py (v1), which requires PROD and BASE to live on the
SAME SQL Server instance (it uses server-side three-part names), this v2 streams
rows THROUGH Python, so PROD and BASE may be on different / remote instances.
Both sides must be MSSQL (mssql+pyodbc).

Reads chunks from PROD → writes chunks to BASE. PROD is only ever READ.

For each table in COPY_PROD_TABLES_FILE:
  • If the table exists in BASE_DB → TRUNCATE it.
  • Else → CREATE it from PROD's column metadata (name/type/nullability only —
    no PK/index/identity/defaults; "SELECT INTO level" structure, minus identity
    by design so BASE tables carry no identity constraint).
  • Stream PROD rows into BASE with fast_executemany, committing per chunk.
    BASE may carry 3 extra columns not present in PROD (nd_auto_increment_id,
    nd_extracted_date, nd_ActiveFlag); those are back-filled during insert
    (same values as add_nd_columns.py).
  • If a pre-existing BASE table has an IDENTITY column, original values are
    preserved via SET IDENTITY_INSERT (no stored-proc dependency).

Because rows flow through Python in fetchmany chunks, every table reports live
progress — no keyed/keyless distinction, no monitor thread.

Outputs (all resumable — updated after EVERY table):
  • COPY_PROD_LOG_FILE       — per-table outcome CSV (status, counts, error).
                               Tables logged 'success' are skipped on re-run.
  • COPY_PROD_COMPLETED_CSV  — flat list of completed (success) table names.
  • COPY_PROD_DETAIL_LOG     — detailed, timestamped run log (every step/chunk).

Usage:
    python copy_prod_to_base_v2.py
"""

import csv
import logging
import os
import sys
import time
import sqlalchemy
from pathlib import Path
from sqlalchemy import text
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "manual.env")

# ── Configuration (identical env vars to v1) ──────────────────────────────────
SOURCE_CONN  = os.environ["COPY_PROD_SOURCE_CONN"]   # PROD_DB (may be remote)
DEST_CONN    = os.environ["COPY_PROD_DEST_CONN"]     # BASE_DB (local)
TABLES_FILE  = os.environ["COPY_PROD_TABLES_FILE"]
LOG_FILE     = os.environ["COPY_PROD_LOG_FILE"]
DEFAULT_DATE = os.environ["COPY_PROD_DEFAULT_DATE"]

BATCH_SIZE        = int(os.environ.get("COPY_PROD_BATCH_SIZE", "50000"))
PROGRESS_INTERVAL = int(os.environ.get("COPY_PROD_PROGRESS_INTERVAL", "15"))

_LOG_DIR = os.path.dirname(LOG_FILE)
_RUN_TS  = datetime.now().strftime("%Y%m%d_%H%M%S")

COMPLETED_CSV = os.environ.get(
    "COPY_PROD_COMPLETED_CSV",
    os.path.join(_LOG_DIR, "copy_prod_to_base_completed.csv"),
)
DETAIL_LOG = os.environ.get(
    "COPY_PROD_DETAIL_LOG",
    os.path.join(_LOG_DIR, f"copy_prod_to_base_v2_{_RUN_TS}.log"),
)
# ──────────────────────────────────────────────────────────────────────────────

ND_AUTO_INCREMENT_ID = "nd_auto_increment_id"
ND_EXTRACTED_DATE    = "nd_extracted_date"
ND_ACTIVE_FLAG       = "nd_ActiveFlag"
ND_COLUMNS           = {ND_AUTO_INCREMENT_ID, ND_EXTRACTED_DATE, ND_ACTIVE_FLAG}

LOG_FIELDNAMES = [
    "table_name", "action", "strategy", "status",
    "prod_count", "base_count", "count_match",
    "timestamp", "error",
]
COMPLETED_FIELDNAMES = ["table_name", "action", "strategy", "prod_count", "base_count", "timestamp"]

os.makedirs(_LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DETAIL_LOG),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("copy_prod_to_base_v2")


def _extract_db(conn_str: str) -> str:
    return conn_str.split("/")[-1].split("?")[0]


def _fmt_eta(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "?"
    return f"{seconds:,.0f}s"


def _build_col_ddl(col: dict) -> str:
    """Column DDL from sys.columns metadata.

    Adapted from diff_script_optimised_v3.build_col_ddl; extended for
    datetime2/time/datetimeoffset (fractional-seconds scale) and float
    (precision). Emits name/type/nullability only — no identity, PK, default,
    or computed definition.
    """
    name = f"[{col['name']}]"
    typ  = col["type_name"].lower()

    if typ in ("varchar", "char", "nvarchar", "nchar"):
        ml = col["max_length"]
        if ml == -1:
            size = "MAX"
        elif typ in ("nvarchar", "nchar"):
            size = str(ml // 2)
        else:
            size = str(ml)
        type_str = f"{typ}({size})"
    elif typ in ("varbinary", "binary"):
        ml = col["max_length"]
        size = "MAX" if ml == -1 else str(ml)
        type_str = f"{typ}({size})"
    elif typ in ("decimal", "numeric"):
        type_str = f"{typ}({col['precision']},{col['scale']})"
    elif typ in ("datetime2", "time", "datetimeoffset"):
        type_str = f"{typ}({col['scale']})"
    elif typ == "float":
        # precision 53 is the default; emit explicitly to be safe
        type_str = f"float({col['precision']})" if col.get("precision") else "float"
    else:
        # int, bigint, bit, datetime, date, money, uniqueidentifier, text, etc.
        type_str = typ

    nullability = "NULL" if col["is_nullable"] else "NOT NULL"
    return f"{name} {type_str} {nullability}"


class CopyProdToBaseV2:

    SRC_DB = _extract_db(SOURCE_CONN)
    DST_DB = _extract_db(DEST_CONN)

    # ── log helpers (same scaffold as v1) ─────────────────────────────────────
    @staticmethod
    def _load_tables() -> list[str]:
        with open(TABLES_FILE) as f:
            return [line.strip() for line in f if line.strip()]

    @staticmethod
    def _load_log() -> dict:
        log = {}
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, newline="") as f:
                for row in csv.DictReader(f):
                    log[row["table_name"]] = row
        return log

    @staticmethod
    def _write_log(log: dict):
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=LOG_FIELDNAMES)
            writer.writeheader()
            for entry in log.values():
                writer.writerow({k: entry.get(k, "") for k in LOG_FIELDNAMES})

    @staticmethod
    def _write_completed(log: dict):
        os.makedirs(os.path.dirname(COMPLETED_CSV), exist_ok=True)
        with open(COMPLETED_CSV, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=COMPLETED_FIELDNAMES)
            writer.writeheader()
            for entry in log.values():
                if entry.get("status") == "success":
                    writer.writerow({k: entry.get(k, "") for k in COMPLETED_FIELDNAMES})

    # ── metadata helpers (each queries its OWN database, two-part names) ──────
    @staticmethod
    def _table_exists(conn, table: str) -> bool:
        q = text("""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = :tbl
        """)
        return conn.execute(q, {"tbl": table}).scalar() > 0

    @staticmethod
    def _columns(conn, table: str) -> list[str]:
        q = text("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = :tbl
            ORDER BY ORDINAL_POSITION
        """)
        return [r[0] for r in conn.execute(q, {"tbl": table})]

    @staticmethod
    def _ddl_info(conn, table: str) -> list[dict]:
        q = text("""
            SELECT c.name,
                   t.name            AS type_name,
                   c.max_length,
                   c.precision,
                   c.scale,
                   c.is_nullable,
                   c.column_id
            FROM   sys.columns c
            JOIN   sys.tables  tb ON c.object_id    = tb.object_id
            JOIN   sys.schemas sc ON tb.schema_id   = sc.schema_id
            JOIN   sys.types   t  ON c.user_type_id = t.user_type_id
            WHERE  tb.name = :table AND sc.name = 'dbo'
            ORDER  BY c.column_id
        """)
        return [dict(r._mapping) for r in conn.execute(q, {"table": table})]

    @staticmethod
    def _identity_column(conn, table: str) -> str | None:
        q = text("""
            SELECT c.name FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(:name) AND c.is_identity = 1
        """)
        return conn.execute(q, {"name": f"dbo.{table}"}).scalar()

    # ── nd fill value for a base-only column, per row ─────────────────────────
    @classmethod
    def _nd_value(cls, col: str, row_num: int):
        if col == ND_AUTO_INCREMENT_ID:
            return row_num
        if col == ND_EXTRACTED_DATE:
            return DEFAULT_DATE
        if col == ND_ACTIVE_FLAG:
            return "Y"
        return None

    # ── Per-table runner ──────────────────────────────────────────────────────
    @classmethod
    def run_table(cls, src_engine, dest_engine, table, log, idx, total_tables):
        if log.get(table, {}).get("status") == "success":
            logger.info(f"[SKIP] ({idx}/{total_tables}) {table} — already succeeded.")
            return

        logger.info(f"[RUN]  ({idx}/{total_tables}) {table}")
        action = ""
        strategy = "stream"
        status = "failed"
        error_msg = ""
        prod_count = base_count = None
        count_match = ""

        try:
            # 1. PROD table exists?
            with src_engine.connect() as sconn:
                if not cls._table_exists(sconn, table):
                    raise RuntimeError(f"table not found in PROD ({cls.SRC_DB}.dbo.{table})")
                prod_cols = cls._columns(sconn, table)
                prod_count = sconn.execute(
                    text(f"SELECT COUNT_BIG(*) FROM dbo.[{table}]")
                ).scalar()

            # 2. Ensure BASE table exists (truncate or create).
            with dest_engine.begin() as dconn:
                if cls._table_exists(dconn, table):
                    dconn.execute(text(f"TRUNCATE TABLE [{table}]"))
                    action = "truncated"
                    logger.info(f"       [{table}] existed in BASE → TRUNCATED.")
                else:
                    with src_engine.connect() as sconn:
                        col_meta = cls._ddl_info(sconn, table)
                    if not col_meta:
                        raise RuntimeError("could not read PROD column metadata for CREATE")
                    col_defs = ",\n    ".join(_build_col_ddl(c) for c in col_meta)
                    dconn.execute(text(f"CREATE TABLE [dbo].[{table}] (\n    {col_defs}\n)"))
                    action = "created"
                    logger.info(f"       [{table}] not in BASE → CREATED from PROD metadata "
                                f"({len(col_meta)} cols, no identity).")

            # 3. Column diff + identity detection on BASE.
            with dest_engine.connect() as dconn:
                base_cols = cls._columns(dconn, table)
                id_col = cls._identity_column(dconn, table)
            prod_set, base_set = set(prod_cols), set(base_cols)
            common_cols = [c for c in prod_cols if c in base_set]
            nd_extra    = [c for c in base_cols if c not in prod_set]

            prod_only = [c for c in prod_cols if c not in base_set]
            if prod_only:
                logger.warning(f"       [{table}] PROD-only columns DROPPED: {prod_only}")
            unexpected = [c for c in nd_extra if c not in ND_COLUMNS]
            if unexpected:
                logger.warning(f"       [{table}] BASE-only non-nd columns left NULL: {unexpected}")

            need_identity_insert = bool(id_col and id_col in common_cols)
            logger.info(
                f"       [{table}] prod_rows={prod_count:,} cols(prod/base/common)="
                f"{len(prod_cols)}/{len(base_cols)}/{len(common_cols)} nd_extra={nd_extra} "
                f"identity={id_col or 'none'}{' (IDENTITY_INSERT)' if need_identity_insert else ''}"
            )

            # 4. Stream copy.
            copied = cls._stream_copy(
                src_engine, dest_engine, table, table,
                common_cols, nd_extra, need_identity_insert, prod_count,
            )

            # 5. Final counts.
            with dest_engine.connect() as dconn:
                base_count = dconn.execute(text(f"SELECT COUNT_BIG(*) FROM [{table}]")).scalar()
            count_match = "OK" if prod_count == base_count else "MISMATCH"

            status = "success"
            log_fn = logger.info if count_match == "OK" else logger.warning
            log_fn(f"[OK]   ({idx}/{total_tables}) {table} — {action}/{strategy}, "
                   f"PROD={prod_count:,} BASE={base_count:,} [{count_match}]")
        except Exception as e:
            error_msg = str(e)
            logger.exception(f"[FAIL] ({idx}/{total_tables}) {table} — {error_msg}")

        log[table] = {
            "table_name":  table,
            "action":      action,
            "strategy":    strategy,
            "status":      status,
            "prod_count":  "" if prod_count is None else prod_count,
            "base_count":  "" if base_count is None else base_count,
            "count_match": count_match,
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "error":       error_msg,
        }
        cls._write_log(log)
        cls._write_completed(log)

    @classmethod
    def _stream_copy(cls, src_engine, dest_engine, src_table, dst_table,
                     common_cols, nd_extra, need_identity_insert, prod_count) -> int:
        """Read PROD in chunks, append nd values, fast_executemany into BASE.

        src_table / dst_table are the same for a real run; they differ only in
        tests (read a real table, write to a throwaway dest).
        """
        table = dst_table   # for log messages
        insert_cols = common_cols + nd_extra
        col_sql = ", ".join(f"[{c}]" for c in insert_cols)
        placeholders = ", ".join("?" for _ in insert_cols)
        insert_sql = f"INSERT INTO [{dst_table}] ({col_sql}) VALUES ({placeholders})"
        read_sql = f"SELECT {', '.join(f'[{c}]' for c in common_cols)} FROM dbo.[{src_table}]"

        src_raw = src_engine.raw_connection()
        dest_raw = dest_engine.raw_connection()
        copied = 0
        start = time.monotonic()
        last_log = start
        try:
            scur = src_raw.cursor()
            dcur = dest_raw.cursor()
            dcur.fast_executemany = True
            if need_identity_insert:
                dcur.execute(f"SET IDENTITY_INSERT [{table}] ON")

            scur.execute(read_sql)
            while True:
                chunk = scur.fetchmany(BATCH_SIZE)
                if not chunk:
                    break
                rows = []
                for src_row in chunk:
                    copied += 1
                    rows.append(tuple(src_row) + tuple(cls._nd_value(c, copied) for c in nd_extra))
                dcur.executemany(insert_sql, rows)
                dest_raw.commit()

                now = time.monotonic()
                if now - last_log >= PROGRESS_INTERVAL:
                    elapsed = max(now - start, 0.001)
                    rate = copied / elapsed
                    pct = (100.0 * copied / prod_count) if prod_count else 100.0
                    eta = ((prod_count - copied) / rate) if (rate > 0 and prod_count and prod_count > copied) else 0
                    logger.info(
                        f"       [{table}] streamed {copied:,}/{prod_count:,} ({pct:.1f}%) "
                        f"rate={rate:,.0f}/s eta≈{_fmt_eta(eta)}"
                    )
                    last_log = now

            if need_identity_insert:
                dcur.execute(f"SET IDENTITY_INSERT [{table}] OFF")
                dest_raw.commit()
            scur.close()
            dcur.close()
        except Exception:
            dest_raw.rollback()
            raise
        finally:
            src_raw.close()
            dest_raw.close()

        elapsed = max(time.monotonic() - start, 0.001)
        logger.info(f"       [{table}] streamed {copied:,} rows in {elapsed:,.0f}s ({copied / elapsed:,.0f}/s)")
        return copied

    # ── Entry point ─────────────────────────────────────────────────────────
    @classmethod
    def run_all(cls):
        tables = cls._load_tables()
        log = cls._load_log()
        src_engine = sqlalchemy.create_engine(SOURCE_CONN)
        dest_engine = sqlalchemy.create_engine(DEST_CONN)

        logger.info("=" * 70)
        logger.info(f"copy_prod_to_base_v2 (cross-instance stream) — {cls.SRC_DB} → {cls.DST_DB}")
        logger.info(f"  tables file : {TABLES_FILE} ({len(tables)} tables)")
        logger.info(f"  log csv     : {LOG_FILE}")
        logger.info(f"  completed   : {COMPLETED_CSV}")
        logger.info(f"  detail log  : {DETAIL_LOG}")
        logger.info(f"  batch size  : {BATCH_SIZE:,}   progress interval: {PROGRESS_INTERVAL}s")
        logger.info(f"  nd date     : {DEFAULT_DATE}")
        already = sum(1 for t in tables if log.get(t, {}).get("status") == "success")
        if already:
            logger.info(f"  resuming — {already} table(s) already succeeded, will skip.")
        logger.info("=" * 70)

        for i, table in enumerate(tables, start=1):
            cls.run_table(src_engine, dest_engine, table, log, i, len(tables))

        succeeded = sum(1 for t in tables if log.get(t, {}).get("status") == "success")
        mismatches = [t for t in tables if log.get(t, {}).get("count_match") == "MISMATCH"]
        failed = [t for t in tables if log.get(t, {}).get("status") == "failed"]
        logger.info("=" * 70)
        logger.info(f"Summary: {len(tables)} tables | {succeeded} success | "
                    f"{len(failed)} failed | {len(mismatches)} mismatch")
        if failed:
            logger.warning(f"  Failed: {failed}")
        if mismatches:
            logger.warning(f"  Mismatch: {mismatches}")
        logger.info(f"Completed-tables CSV: {COMPLETED_CSV}")
        logger.info(f"Detailed log: {DETAIL_LOG}")


if __name__ == "__main__":
    CopyProdToBaseV2.run_all()
