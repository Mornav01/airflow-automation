import csv
import os
import re
import threading
import concurrent.futures
import sqlalchemy
from pathlib import Path
from sqlalchemy import text
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "manual.env")

# ── Configuration ─────────────────────────────────────────────────────────────
CONNECTION_STRING = os.environ["CONSTRAINT_CONN"]
TABLES_INPUT_FILE = os.environ["CONSTRAINT_TABLES_FILE"]
LOG_FILE          = os.environ["CONSTRAINT_LOG_FILE"]
# ──────────────────────────────────────────────────────────────────────────────

LOG_FIELDNAMES = ["table_name", "status", "timestamp", "error"]


class ConstraintsRemoval:

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

    @classmethod
    def _ensure_stored_procedure(cls, engine: sqlalchemy.Engine):
        """Read remove_constraints.sql, split on GO, and execute each batch."""
        sql_path = Path(__file__).parent / "sql" / "remove_constraints.sql"
        sql_text = sql_path.read_text(encoding="utf-8-sig")  # strips BOM if present
        batches = [
            b.strip()
            for b in re.split(r"^\s*GO\s*$", sql_text, flags=re.MULTILINE)
            if b.strip()
        ]
        print(f"  Loaded {len(batches)} SQL batch(es) from {sql_path.name}")
        raw_conn = engine.raw_connection()
        cur = raw_conn.cursor()
        try:
            for i, batch in enumerate(batches, 1):
                print(f"  Executing batch {i}/{len(batches)}: {repr(batch[:80])}")
                cur.execute(batch)
            raw_conn.commit()
            cur.close()
        except Exception as exc:
            try:
                raw_conn.rollback()
            except Exception:
                pass
            raise RuntimeError(
                f"Batch {i}/{len(batches)} failed: {exc}\n"
                f"First 80 chars repr: {repr(batch[:80])}"
            ) from exc
        finally:
            raw_conn.close()
        print("Stored procedure sp_remove_identity_columns_final_updated created/updated.")

    @classmethod
    def run_sp(cls, table_name: str, log: dict, lock: threading.Lock,
               engine: sqlalchemy.Engine):
        """
        Calls sp_remove_identity_columns_final_updated for the given table.
        Skips the table if it already has a 'success' entry in the log CSV.
        Reuses the shared engine — no per-table connection setup overhead.
        """
        with lock:
            if log.get(table_name, {}).get("status") == "success":
                print(f"[SKIP] {table_name} — already succeeded, skipping.")
                return

        print(f"[RUN]  {table_name} — executing stored procedure...")
        status = "failure"
        error_msg = ""

        try:
            sql = text("""
                EXEC sp_remove_identity_columns_final_updated
                    @SchemaName = :SchemaName,
                    @TableName  = :TableName
            """)
            with engine.begin() as conn:
                conn.execute(sql, {"TableName": table_name, "SchemaName": "dbo"})
            status = "success"
            print(f"[OK]   {table_name} — completed successfully.")
        except Exception as e:
            error_msg = str(e)
            print(f"[FAIL] {table_name} — {error_msg}")

        with lock:
            log[table_name] = {
                "table_name": table_name,
                "status": status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": error_msg,
            }
            cls._write_log(log)

    @classmethod
    def run_all(cls, max_workers: int = 8):
        tables = cls._load_tables()
        log    = cls._load_log()
        lock   = threading.Lock()

        # Single shared engine — pool_size matches thread count so each thread
        # holds exactly one connection; no per-table TCP setup through Docker networking.
        engine = sqlalchemy.create_engine(
            CONNECTION_STRING,
            pool_size=max_workers,
            max_overflow=0,
            pool_pre_ping=True,
            pool_recycle=1800,
        )

        cls._ensure_stored_procedure(engine)

        print(f"Processing {len(tables)} table(s) from '{TABLES_INPUT_FILE}' "
              f"with max_workers={max_workers}...\n")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(cls.run_sp, table, log, lock, engine): table
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
    ConstraintsRemoval.run_all()
