import csv
import os
import threading
import concurrent.futures
import sqlalchemy
from pathlib import Path
from sqlalchemy import text
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "manual.env")

# ── Configuration ─────────────────────────────────────────────────────────────
CONNECTION_STRING = os.environ["ADD_ND_CONN"]
TABLES_INPUT_FILE = os.environ["ADD_ND_TABLES_FILE"]
LOG_FILE          = os.environ["ADD_ND_LOG_FILE"]
DEFAULT_DATE      = os.environ["ADD_ND_DEFAULT_DATE"]
# Fewer concurrent workers → fewer simultaneous whole-table UPDATEs → less
# tx-log/connection pressure on large tables. Override via ADD_ND_MAX_WORKERS.
MAX_WORKERS       = int(os.environ.get("ADD_ND_MAX_WORKERS", "4"))
# Populate is done in bounded, per-batch-committed chunks so no single UPDATE
# holds a multi-million-row transaction (which overwhelmed the server and reset
# the connection). Override via ADD_ND_BATCH_SIZE.
BATCH_SIZE        = int(os.environ.get("ADD_ND_BATCH_SIZE", "100000"))
# ──────────────────────────────────────────────────────────────────────────────

LOG_FIELDNAMES = ["table_name", "status", "timestamp", "error"]


class AddNdColumns:

    @staticmethod
    def _load_tables() -> list[str]:
        """Read table names from the input file — one table name per line, no header."""
        with open(TABLES_INPUT_FILE) as f:
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
        """Write the log dict back to the CSV file, creating dirs if needed."""
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=LOG_FIELDNAMES)
            writer.writeheader()
            for entry in log.values():
                writer.writerow(entry)

    # ── Column-existence check ─────────────────────────────────────────────────

    @staticmethod
    def _column_exists(conn, table: str, column: str) -> bool:
        q = text("""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME  = :tbl
            AND   COLUMN_NAME = :col
        """)
        return conn.execute(q, {"tbl": table, "col": column}).scalar() > 0

    # ── Helpers ────────────────────────────────────────────────────────────────

    @classmethod
    def _add_column_if_missing(cls, engine, table: str, column: str, ddl_type: str) -> None:
        """ALTER ADD the column in its own short transaction, only if absent."""
        with engine.begin() as conn:
            if cls._column_exists(conn, table, column):
                return
            conn.execute(text(f"ALTER TABLE {table} ADD {column} {ddl_type}"))

    # ── The three operations ───────────────────────────────────────────────────
    #
    # Each op ADDs the column (idempotent) then POPULATEs the still-NULL rows in
    # bounded, per-batch-committed chunks. This keeps every transaction small so
    # a multi-million-row table no longer overwhelms the server (previously the
    # single whole-table UPDATE reset the connection / timed out logins). It is
    # also resume-safe: a re-run fills only the rows left NULL by a prior failure.

    @classmethod
    def _add_nd_auto_increment_id(cls, engine, table: str):
        """
        Add nd_auto_increment_id BIGINT if missing, then populate NULL rows with
        sequential row numbers, batched. Numbering stays contiguous/unique across
        batches via an offset that continues from the current MAX (same net result
        as merge_engine._ensure_nd_auto_increment_main).
        """
        cls._add_column_if_missing(engine, table, "nd_auto_increment_id", "BIGINT")

        with engine.begin() as conn:
            offset = conn.execute(
                text(f"SELECT ISNULL(MAX(nd_auto_increment_id), 0) FROM {table}")
            ).scalar() or 0

        total = 0
        while True:
            with engine.begin() as conn:
                n = conn.execute(text(f"""
                    WITH cte AS (
                        SELECT TOP (:batch) nd_auto_increment_id,
                               ROW_NUMBER() OVER (ORDER BY (SELECT 1)) rn
                        FROM {table}
                        WHERE nd_auto_increment_id IS NULL
                    )
                    UPDATE cte SET nd_auto_increment_id = :offset + rn
                """), {"batch": BATCH_SIZE, "offset": int(offset)}).rowcount
            offset += n
            total += n
            if n < BATCH_SIZE:
                break
        print(f"         nd_auto_increment_id populated ({total:,} rows, batch={BATCH_SIZE:,}).")

    @classmethod
    def _add_nd_extracted_date(cls, engine, table: str):
        """
        Add nd_extracted_date DATETIME if missing, then set DEFAULT_DATE for all
        NULL rows, batched (same net result as merge_engine._ensure_nd_extracted_date).
        """
        cls._add_column_if_missing(engine, table, "nd_extracted_date", "DATETIME")

        total = 0
        while True:
            with engine.begin() as conn:
                n = conn.execute(text(f"""
                    UPDATE TOP (:batch) {table}
                    SET nd_extracted_date = :d
                    WHERE nd_extracted_date IS NULL
                """), {"batch": BATCH_SIZE, "d": DEFAULT_DATE}).rowcount
            total += n
            if n < BATCH_SIZE:
                break
        print(f"         nd_extracted_date populated with '{DEFAULT_DATE}' ({total:,} rows).")

    @classmethod
    def _add_nd_active_flag(cls, engine, table: str):
        """
        Add nd_ActiveFlag CHAR(1) if missing, then set 'Y' for all NULL rows,
        batched (same net result as merge_engine._ensure_active_flag).
        """
        cls._add_column_if_missing(engine, table, "nd_ActiveFlag", "CHAR(1)")

        total = 0
        while True:
            with engine.begin() as conn:
                n = conn.execute(text(f"""
                    UPDATE TOP (:batch) {table}
                    SET nd_ActiveFlag = 'Y'
                    WHERE nd_ActiveFlag IS NULL
                """), {"batch": BATCH_SIZE}).rowcount
            total += n
            if n < BATCH_SIZE:
                break
        print(f"         nd_ActiveFlag set to 'Y' ({total:,} rows).")

    # ── Per-table runner ───────────────────────────────────────────────────────

    @classmethod
    def run_table(cls, table_name: str, log: dict, lock: threading.Lock,
                  engine: sqlalchemy.Engine):
        """
        Runs all 3 column operations for the given table. Each op ADDs its column
        and populates it in per-batch-committed chunks (no single giant
        transaction), so it survives large tables and is resume-safe. Skips the
        table if it already has a 'success' entry in the log CSV. Logs the result
        (success / failure) after each table.
        """
        with lock:
            if log.get(table_name, {}).get("status") == "success":
                print(f"[SKIP] {table_name} — already succeeded, skipping.")
                return

        print(f"[RUN]  {table_name} — adding nd columns...")
        status    = "failure"
        error_msg = ""

        try:
            cls._add_nd_auto_increment_id(engine, table_name)
            cls._add_nd_extracted_date(engine, table_name)
            cls._add_nd_active_flag(engine, table_name)

            status = "success"
            print(f"[OK]   {table_name} — all 3 columns applied successfully.")
        except Exception as e:
            error_msg = str(e)
            print(f"[FAIL] {table_name} — {error_msg}")

        with lock:
            log[table_name] = {
                "table_name": table_name,
                "status":     status,
                "timestamp":  datetime.now(timezone.utc).isoformat(),
                "error":      error_msg,
            }
            cls._write_log(log)

    # ── Entry point ────────────────────────────────────────────────────────────

    @classmethod
    def run_all(cls, max_workers: int = 8):
        tables = cls._load_tables()
        log    = cls._load_log()
        lock   = threading.Lock()

        # Shared engine — pool_size matches thread count; no per-table connection setup.
        engine = sqlalchemy.create_engine(
            CONNECTION_STRING,
            pool_size=max_workers,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=1800,
        )

        print(f"Processing {len(tables)} table(s) from '{TABLES_INPUT_FILE}' "
              f"with max_workers={max_workers}...\n")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(cls.run_table, table, log, lock, engine): table
                for table in tables
            }
            for future in concurrent.futures.as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"[ERROR] {table} — unhandled exception: {e}")

        engine.dispose()


if __name__ == "__main__":
    AddNdColumns.run_all(max_workers=MAX_WORKERS)
