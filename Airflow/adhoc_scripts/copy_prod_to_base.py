"""
Copy tables PROD_DB (mobiledoc_latest) → BASE_DB (mobiledoc_base_0601).

Both databases live on the same MSSQL instance, so every operation runs
server-side through three-part names ([db].dbo.[table]) — no data is streamed
through Python. PROD_DB is only ever READ from; all writes go to BASE_DB.

For each table in COPY_PROD_TABLES_FILE:
  • If the table exists in BASE_DB → TRUNCATE it.
  • Else → create it with the exact same column structure as PROD_DB
    (SELECT TOP 0 * INTO …).
  • Remove any IDENTITY constraint on the (now empty) BASE table via the
    sp_remove_identity_columns_final_updated stored proc — original id values
    are still copied as plain-column data. (This runs on BASE only.)
  • Copy PROD rows into BASE with INSERT … SELECT WITH (TABLOCK) (minimal-logging
    speed). BASE may carry 3 extra columns not present in PROD
    (nd_auto_increment_id, nd_extracted_date, nd_ActiveFlag); those are
    back-filled during the insert (same logic as add_nd_columns.py).

Copy strategy is chosen per table by key availability (for progress + bounded
transactions on large tables):
  • single-column PK/unique key → KEYSET batching (fast index-seek pages).
  • multi-column PK/unique key  → OFFSET/FETCH batching over the unique key.
  • no unique key               → single atomic load + a background progress
                                   monitor (rows landed via NOLOCK + the SQL
                                   request's wait state), since it can't be
                                   paginated safely.

Outputs (all resumable — updated after EVERY table):
  • COPY_PROD_LOG_FILE       — per-table outcome CSV (status, counts, error).
                               Tables logged 'success' are skipped on re-run.
  • COPY_PROD_COMPLETED_CSV  — flat list of completed (success) table names.
  • COPY_PROD_DETAIL_LOG     — detailed, timestamped run log (every step/batch).

Usage:
    python copy_prod_to_base.py
"""

import csv
import logging
import os
import sys
import threading
import time
import sqlalchemy
from pathlib import Path
from sqlalchemy import text
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "manual.env")

# ── Configuration ─────────────────────────────────────────────────────────────
SOURCE_CONN  = os.environ["COPY_PROD_SOURCE_CONN"]   # PROD_DB  (mobiledoc_latest)
DEST_CONN    = os.environ["COPY_PROD_DEST_CONN"]     # BASE_DB  (mobiledoc_base_0601)
TABLES_FILE  = os.environ["COPY_PROD_TABLES_FILE"]
LOG_FILE     = os.environ["COPY_PROD_LOG_FILE"]
DEFAULT_DATE = os.environ["COPY_PROD_DEFAULT_DATE"]

BATCH_SIZE        = int(os.environ.get("COPY_PROD_BATCH_SIZE", "200000"))
PROGRESS_INTERVAL = int(os.environ.get("COPY_PROD_PROGRESS_INTERVAL", "15"))

_LOG_DIR = os.path.dirname(LOG_FILE)
_RUN_TS  = datetime.now().strftime("%Y%m%d_%H%M%S")

# Completed-tables CSV and detailed log — optional env overrides, else derived.
COMPLETED_CSV = os.environ.get(
    "COPY_PROD_COMPLETED_CSV",
    os.path.join(_LOG_DIR, "copy_prod_to_base_completed.csv"),
)
DETAIL_LOG = os.environ.get(
    "COPY_PROD_DETAIL_LOG",
    os.path.join(_LOG_DIR, f"copy_prod_to_base_{_RUN_TS}.log"),
)
# ──────────────────────────────────────────────────────────────────────────────

# The 3 nd columns that may exist only on the BASE side (not in PROD).
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

# ── Logging setup (detailed file log + console) ──────────────────────────────
os.makedirs(_LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DETAIL_LOG),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("copy_prod_to_base")


def _extract_db(conn_str: str) -> str:
    """Bare database name from a SQLAlchemy connection string.

    Mirrors merge_engine._extract_db — takes the last path segment and strips
    any ?driver=… query string.
    """
    return conn_str.split("/")[-1].split("?")[0]


def _fmt_eta(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "?"
    return f"{seconds:,.0f}s"


class CopyProdToBase:

    SRC_DB = _extract_db(SOURCE_CONN)   # e.g. mobiledoc_latest

    @staticmethod
    def _load_tables() -> list[str]:
        """Read table names from the input file — one per line, no header."""
        with open(TABLES_FILE) as f:
            return [line.strip() for line in f if line.strip()]

    @staticmethod
    def _load_log() -> dict:
        """Load existing log CSV into a dict keyed by table_name."""
        log = {}
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, newline="") as f:
                for row in csv.DictReader(f):
                    log[row["table_name"]] = row
        return log

    @staticmethod
    def _write_log(log: dict):
        """Rewrite the whole log CSV so progress survives a crash."""
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=LOG_FIELDNAMES)
            writer.writeheader()
            for entry in log.values():
                writer.writerow({k: entry.get(k, "") for k in LOG_FIELDNAMES})

    @staticmethod
    def _write_completed(log: dict):
        """Rewrite the completed-tables CSV from every 'success' log entry.

        Rebuilt (not appended) after each table so it stays deduped and
        consistent across resumed runs.
        """
        os.makedirs(os.path.dirname(COMPLETED_CSV), exist_ok=True)
        with open(COMPLETED_CSV, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=COMPLETED_FIELDNAMES)
            writer.writeheader()
            for entry in log.values():
                if entry.get("status") == "success":
                    writer.writerow({k: entry.get(k, "") for k in COMPLETED_FIELDNAMES})

    # ── Schema / metadata helpers ────────────────────────────────────────────

    @classmethod
    def _prod_table_exists(cls, conn, table: str) -> bool:
        q = text(f"""
            SELECT COUNT(*)
            FROM {cls.SRC_DB}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = :tbl
        """)
        return conn.execute(q, {"tbl": table}).scalar() > 0

    @staticmethod
    def _base_table_exists(conn, table: str) -> bool:
        # OBJECT_ID resolves against the current (BASE) database.
        return conn.execute(
            text("SELECT OBJECT_ID(:name, 'U')"),
            {"name": f"dbo.{table}"},
        ).scalar() is not None

    @classmethod
    def _prod_columns(cls, conn, table: str) -> list[str]:
        q = text(f"""
            SELECT COLUMN_NAME
            FROM {cls.SRC_DB}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = :tbl
            ORDER BY ORDINAL_POSITION
        """)
        return [r[0] for r in conn.execute(q, {"tbl": table})]

    @staticmethod
    def _base_columns(conn, table: str) -> list[str]:
        q = text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = :tbl
            ORDER BY ORDINAL_POSITION
        """)
        return [r[0] for r in conn.execute(q, {"tbl": table})]

    @staticmethod
    def _identity_column(conn, table: str) -> str | None:
        """Name of the BASE table's identity column, or None."""
        q = text("""
            SELECT c.name
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(:name) AND c.is_identity = 1
        """)
        return conn.execute(q, {"name": f"dbo.{table}"}).scalar()

    @classmethod
    def _best_unique_key(cls, conn, table: str) -> list[str] | None:
        """Ordered column list of PROD's best unique key for pagination.

        Picks the PK/unique index with the fewest key columns (preferring the
        primary key, then a clustered index). Returns None if the table has no
        PK/unique index.
        """
        q = text(f"""
            SELECT i.index_id, i.is_primary_key, i.type, ic.key_ordinal, col.name
            FROM {cls.SRC_DB}.sys.indexes i
            JOIN {cls.SRC_DB}.sys.index_columns ic
                 ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN {cls.SRC_DB}.sys.columns col
                 ON col.object_id = ic.object_id AND col.column_id = ic.column_id
            WHERE i.object_id = OBJECT_ID(:name)
              AND (i.is_primary_key = 1 OR i.is_unique = 1)
              AND i.type IN (1, 2)                 -- clustered / nonclustered
              AND ic.is_included_column = 0
            ORDER BY i.index_id, ic.key_ordinal
        """)
        indexes: dict = {}
        for index_id, is_pk, itype, _ordinal, colname in conn.execute(q, {"name": f"{cls.SRC_DB}.dbo.{table}"}):
            entry = indexes.setdefault(index_id, {"is_pk": bool(is_pk), "type": itype, "cols": []})
            entry["cols"].append(colname)
        if not indexes:
            return None
        best = min(
            indexes.values(),
            key=lambda d: (0 if d["is_pk"] else 1, 0 if d["type"] == 1 else 1, len(d["cols"])),
        )
        return best["cols"]

    # ── nd fill expressions ───────────────────────────────────────────────────

    @staticmethod
    def _nd_select_expr(col: str) -> str:
        """nd fill for the atomic (single-statement, global ROW_NUMBER) path."""
        if col == ND_AUTO_INCREMENT_ID:
            return "ROW_NUMBER() OVER (ORDER BY (SELECT 1))"
        if col == ND_EXTRACTED_DATE:
            return ":nd_default_date"
        if col == ND_ACTIVE_FLAG:
            return "'Y'"
        return "NULL"

    @staticmethod
    def _nd_batch_expr(col: str, order_by: str) -> str:
        """nd fill for a batched path — nd_auto_increment_id is
        :id_offset + ROW_NUMBER() OVER (ORDER BY <key>) so ids stay contiguous
        across batches."""
        if col == ND_AUTO_INCREMENT_ID:
            return f":id_offset + ROW_NUMBER() OVER (ORDER BY {order_by})"
        if col == ND_EXTRACTED_DATE:
            return ":nd_default_date"
        if col == ND_ACTIVE_FLAG:
            return "'Y'"
        return "NULL"

    # ── Copy strategies ────────────────────────────────────────────────────────

    @classmethod
    def _log_batch_progress(cls, table, strategy, batch_no, n, total, prod_count, start):
        elapsed = max(time.monotonic() - start, 0.001)
        rate = total / elapsed
        pct = (100.0 * total / prod_count) if prod_count else 100.0
        eta = ((prod_count - total) / rate) if (rate > 0 and prod_count and prod_count > total) else 0
        logger.info(
            f"       [{table}] {strategy} batch {batch_no}: +{n:,} → "
            f"{total:,}/{prod_count:,} ({pct:.1f}%) rate={rate:,.0f}/s eta≈{_fmt_eta(eta)}"
        )

    @classmethod
    def _copy_keyset(cls, engine, table, key_col, common_cols, nd_extra, prod_count) -> int:
        """Path A — single-column key: keyset pagination (WHERE key > last)."""
        insert_cols = common_cols + nd_extra
        col_list = ", ".join(f"[{c}]" for c in insert_cols)
        order_by = f"src.[{key_col}]"
        select_common = ", ".join(f"src.[{c}]" for c in common_cols)
        nd_sel = [cls._nd_batch_expr(c, order_by) for c in nd_extra]
        select_list = ", ".join(([select_common] if common_cols else []) + nd_sel)

        last = None
        id_offset = 0
        total = 0
        batch_no = 0
        start = time.monotonic()
        while True:
            batch_no += 1
            where = "" if last is None else f"WHERE [{key_col}] > :last"
            max_sql = text(
                f"SELECT MAX([{key_col}]) FROM "
                f"(SELECT TOP (:batch) [{key_col}] FROM {cls.SRC_DB}.dbo.[{table}] "
                f"{where} ORDER BY [{key_col}]) x"
            )
            insert_sql = text(
                f"INSERT INTO [{table}] WITH (TABLOCK) ({col_list}) "
                f"SELECT {select_list} FROM "
                f"(SELECT TOP (:batch) * FROM {cls.SRC_DB}.dbo.[{table}] "
                f"{where} ORDER BY [{key_col}]) src"
            )
            params = {"batch": BATCH_SIZE, "id_offset": id_offset}
            if last is not None:
                params["last"] = last
            if ND_EXTRACTED_DATE in nd_extra:
                params["nd_default_date"] = DEFAULT_DATE

            with engine.begin() as conn:
                new_last = conn.execute(max_sql, params).scalar()
                if new_last is None:      # no more rows
                    break
                n = conn.execute(insert_sql, params).rowcount

            total += n
            id_offset += n
            last = new_last
            cls._log_batch_progress(table, "keyset", batch_no, n, total, prod_count, start)
            if n < BATCH_SIZE:
                break
        return total

    @classmethod
    def _copy_offset(cls, engine, table, key_cols, common_cols, nd_extra, prod_count) -> int:
        """Path B — multi-column key: OFFSET/FETCH over the unique key order."""
        insert_cols = common_cols + nd_extra
        col_list = ", ".join(f"[{c}]" for c in insert_cols)
        order_by = ", ".join(f"src.[{c}]" for c in key_cols)
        src_order = ", ".join(f"[{c}]" for c in key_cols)
        select_common = ", ".join(f"src.[{c}]" for c in common_cols)
        nd_sel = [cls._nd_batch_expr(c, order_by) for c in nd_extra]
        select_list = ", ".join(([select_common] if common_cols else []) + nd_sel)

        offset = 0
        id_offset = 0
        total = 0
        batch_no = 0
        start = time.monotonic()
        while True:
            batch_no += 1
            insert_sql = text(
                f"INSERT INTO [{table}] WITH (TABLOCK) ({col_list}) "
                f"SELECT {select_list} FROM "
                f"(SELECT * FROM {cls.SRC_DB}.dbo.[{table}] "
                f"ORDER BY {src_order} OFFSET :off ROWS FETCH NEXT :batch ROWS ONLY) src"
            )
            params = {"off": offset, "batch": BATCH_SIZE, "id_offset": id_offset}
            if ND_EXTRACTED_DATE in nd_extra:
                params["nd_default_date"] = DEFAULT_DATE

            with engine.begin() as conn:
                n = conn.execute(insert_sql, params).rowcount

            total += n
            offset += n
            id_offset += n
            cls._log_batch_progress(table, "offset", batch_no, n, total, prod_count, start)
            if n < BATCH_SIZE:
                break
        return total

    @classmethod
    def _monitor(cls, engine, table, spid, target, stop_event, interval):
        """Background progress poller for the atomic (keyless) load. Never raises."""
        start = time.monotonic()
        while not stop_event.wait(interval):
            try:
                with engine.connect() as c:
                    cnt = c.execute(
                        text(f"SELECT COUNT_BIG(*) FROM [{table}] WITH (NOLOCK)")
                    ).scalar() or 0
                    req = c.execute(text("""
                        SELECT status, command, wait_type, blocking_session_id,
                               wait_time, logical_reads
                        FROM sys.dm_exec_requests WHERE session_id = :spid
                    """), {"spid": spid}).first()
                elapsed = max(time.monotonic() - start, 0.001)
                rate = cnt / elapsed
                pct = (100.0 * cnt / target) if target else 0.0
                eta = ((target - cnt) / rate) if (rate > 0 and target and target > cnt) else None
                if req:
                    status, _command, wait, blk, wtime, reads = req
                    detail = (f"status={status} wait={wait or '-'} "
                              f"blocked_by={blk or 0} wait_ms={wtime} reads={reads}")
                else:
                    detail = "no active request (committing/finishing)"
                logger.info(
                    f"       [{table}] progress: ~{cnt:,}/{target:,} ({pct:.1f}%) "
                    f"rate={rate:,.0f}/s eta≈{_fmt_eta(eta)} | {detail}"
                )
            except Exception as e:
                logger.warning(f"       [{table}] progress poll failed: {e}")

    @classmethod
    def _copy_atomic_monitored(cls, engine, table, common_cols, nd_extra, prod_count) -> int:
        """Path C — no key: one atomic INSERT … SELECT WITH (TABLOCK), watched by
        a background monitor thread."""
        insert_cols = common_cols + nd_extra
        col_list = ", ".join(f"[{c}]" for c in insert_cols)
        select_list = ", ".join(
            [f"[{c}]" for c in common_cols] + [cls._nd_select_expr(c) for c in nd_extra]
        )
        insert_sql = text(
            f"INSERT INTO [{table}] WITH (TABLOCK) ({col_list}) "
            f"SELECT {select_list} FROM {cls.SRC_DB}.dbo.[{table}]"
        )
        params = {"nd_default_date": DEFAULT_DATE} if ND_EXTRACTED_DATE in nd_extra else {}

        stop_event = threading.Event()
        start = time.monotonic()
        with engine.begin() as conn:
            spid = conn.execute(text("SELECT @@SPID")).scalar()
            monitor = threading.Thread(
                target=cls._monitor,
                args=(engine, table, spid, prod_count, stop_event, PROGRESS_INTERVAL),
                daemon=True,
            )
            logger.info(f"       [{table}] atomic load (spid={spid}) — monitoring every {PROGRESS_INTERVAL}s …")
            monitor.start()
            try:
                n = conn.execute(insert_sql, params).rowcount
            finally:
                stop_event.set()
                monitor.join(timeout=PROGRESS_INTERVAL + 5)
        elapsed = max(time.monotonic() - start, 0.001)
        logger.info(f"       [{table}] atomic load done: {n:,} rows in {elapsed:,.0f}s "
                    f"({n / elapsed:,.0f}/s)")
        return n

    # ── Per-table runner ──────────────────────────────────────────────────────

    @classmethod
    def run_table(cls, engine, table: str, log: dict, idx: int, total_tables: int):
        if log.get(table, {}).get("status") == "success":
            logger.info(f"[SKIP] ({idx}/{total_tables}) {table} — already succeeded.")
            return

        logger.info(f"[RUN]  ({idx}/{total_tables}) {table}")
        action = strategy = ""
        status = "failed"
        error_msg = ""
        prod_count = base_count = None
        count_match = ""

        try:
            # ── Tx-prep: ensure an empty, non-identity BASE table + column diff ──
            with engine.begin() as conn:
                if not cls._prod_table_exists(conn, table):
                    raise RuntimeError(f"table not found in PROD ({cls.SRC_DB}.dbo.{table})")

                if cls._base_table_exists(conn, table):
                    conn.execute(text(f"TRUNCATE TABLE [{table}]"))
                    action = "truncated"
                    logger.info(f"       [{table}] existed in BASE → TRUNCATED.")
                else:
                    conn.execute(text(
                        f"SELECT TOP 0 * INTO [{table}] FROM {cls.SRC_DB}.dbo.[{table}]"
                    ))
                    action = "created"
                    logger.info(f"       [{table}] not in BASE → CREATED empty clone of PROD.")

                id_col = cls._identity_column(conn, table)
                if id_col:
                    conn.execute(
                        text("""EXEC sp_remove_identity_columns_final_updated
                                    @SchemaName = :schema, @TableName = :tbl"""),
                        {"schema": "dbo", "tbl": table},
                    )
                    logger.info(f"       [{table}] IDENTITY removed from '{id_col}' (values preserved).")

                base_cols = cls._base_columns(conn, table)
                prod_cols = cls._prod_columns(conn, table)
                prod_set, base_set = set(prod_cols), set(base_cols)
                common_cols = [c for c in prod_cols if c in base_set]
                nd_extra    = [c for c in base_cols if c not in prod_set]

                prod_only = [c for c in prod_cols if c not in base_set]
                if prod_only:
                    logger.warning(f"       [{table}] PROD-only columns DROPPED: {prod_only}")
                unexpected = [c for c in nd_extra if c not in ND_COLUMNS]
                if unexpected:
                    logger.warning(f"       [{table}] BASE-only non-nd columns left NULL: {unexpected}")

                key_cols = cls._best_unique_key(conn, table)

            # ── Target row count (PROD) ──
            with engine.connect() as conn:
                prod_count = conn.execute(text(
                    f"SELECT COUNT_BIG(*) FROM {cls.SRC_DB}.dbo.[{table}]"
                )).scalar()

            logger.info(
                f"       [{table}] prod_rows={prod_count:,} cols(prod/base/common)="
                f"{len(prod_cols)}/{len(base_cols)}/{len(common_cols)} nd_extra={nd_extra} "
                f"key={key_cols or 'NONE'}"
            )

            # ── Dispatch by key availability ──
            if key_cols and len(key_cols) == 1:
                strategy = "keyset"
                cls._copy_keyset(engine, table, key_cols[0], common_cols, nd_extra, prod_count)
            elif key_cols and len(key_cols) > 1:
                strategy = "offset"
                cls._copy_offset(engine, table, key_cols, common_cols, nd_extra, prod_count)
            else:
                strategy = "atomic+monitor"
                cls._copy_atomic_monitored(engine, table, common_cols, nd_extra, prod_count)

            # ── Final counts ──
            with engine.connect() as conn:
                base_count = conn.execute(text(f"SELECT COUNT_BIG(*) FROM [{table}]")).scalar()
            count_match = "OK" if prod_count == base_count else "MISMATCH"

            status = "success"
            log_fn = logger.info if count_match == "OK" else logger.warning
            log_fn(
                f"[OK]   ({idx}/{total_tables}) {table} — {action}/{strategy}, "
                f"PROD={prod_count:,} BASE={base_count:,} [{count_match}]"
            )
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

    # ── Entry point ─────────────────────────────────────────────────────────

    @classmethod
    def run_all(cls):
        tables = cls._load_tables()
        log = cls._load_log()
        engine = sqlalchemy.create_engine(DEST_CONN)  # connects to BASE_DB

        recovery = "?"
        try:
            with engine.connect() as conn:
                recovery = conn.execute(text(
                    "SELECT recovery_model_desc FROM sys.databases WHERE name = DB_NAME()"
                )).scalar()
        except Exception as e:
            logger.warning(f"could not read BASE recovery model: {e}")

        logger.info("=" * 70)
        logger.info(f"copy_prod_to_base — {cls.SRC_DB} → BASE")
        logger.info(f"  tables file : {TABLES_FILE} ({len(tables)} tables)")
        logger.info(f"  log csv     : {LOG_FILE}")
        logger.info(f"  completed   : {COMPLETED_CSV}")
        logger.info(f"  detail log  : {DETAIL_LOG}")
        logger.info(f"  batch size  : {BATCH_SIZE:,}   progress interval: {PROGRESS_INTERVAL}s")
        logger.info(f"  nd date     : {DEFAULT_DATE}")
        logger.info(f"  BASE recovery model: {recovery} "
                    f"(TABLOCK minimal-logging speedup is largest under SIMPLE/BULK_LOGGED)")
        already = sum(1 for t in tables if log.get(t, {}).get("status") == "success")
        if already:
            logger.info(f"  resuming — {already} table(s) already succeeded, will skip.")
        logger.info("=" * 70)

        for i, table in enumerate(tables, start=1):
            cls.run_table(engine, table, log, i, len(tables))

        succeeded = sum(1 for t in tables if log.get(t, {}).get("status") == "success")
        mismatches = [t for t in tables if log.get(t, {}).get("count_match") == "MISMATCH"]
        failed = [t for t in tables if log.get(t, {}).get("status") == "failed"]
        logger.info("=" * 70)
        logger.info(
            f"Summary: {len(tables)} tables | {succeeded} success | "
            f"{len(failed)} failed | {len(mismatches)} mismatch"
        )
        if failed:
            logger.warning(f"  Failed: {failed}")
        if mismatches:
            logger.warning(f"  Mismatch: {mismatches}")
        logger.info(f"Completed-tables CSV: {COMPLETED_CSV}")
        logger.info(f"Detailed log: {DETAIL_LOG}")


if __name__ == "__main__":
    CopyProdToBase.run_all()
