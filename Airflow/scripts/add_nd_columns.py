import csv
import os
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

    # ── The three operations ───────────────────────────────────────────────────

    @classmethod
    def _add_nd_auto_increment_id(cls, conn, table: str):
        """
        Add nd_auto_increment_id BIGINT if missing, then populate with
        sequential row numbers (same logic as merge_engine._ensure_nd_auto_increment_main).
        """
        if cls._column_exists(conn, table, "nd_auto_increment_id"):
            print(f"         nd_auto_increment_id already exists — skipping ADD.")
            return

        conn.execute(text(f"ALTER TABLE {table} ADD nd_auto_increment_id BIGINT"))

        conn.execute(text(f"""
            WITH cte AS (
                SELECT nd_auto_increment_id,
                       ROW_NUMBER() OVER (ORDER BY (SELECT 1)) rn
                FROM {table}
            )
            UPDATE cte SET nd_auto_increment_id = rn
        """))
        print(f"         nd_auto_increment_id added and populated.")

    @classmethod
    def _add_nd_extracted_date(cls, conn, table: str):
        """
        Add nd_extracted_date DATETIME if missing, then set DEFAULT_DATE
        for all NULL rows (same logic as merge_engine._ensure_nd_extracted_date).
        """
        if cls._column_exists(conn, table, "nd_extracted_date"):
            print(f"         nd_extracted_date already exists — skipping ADD.")
            return

        conn.execute(text(f"ALTER TABLE {table} ADD nd_extracted_date DATETIME"))

        conn.execute(text(f"""
            UPDATE {table}
            SET nd_extracted_date = :d
            WHERE nd_extracted_date IS NULL
        """), {"d": DEFAULT_DATE})
        print(f"         nd_extracted_date added and populated with '{DEFAULT_DATE}'.")

    @classmethod
    def _add_nd_active_flag(cls, conn, table: str):
        """
        Add nd_ActiveFlag CHAR(1) if missing, then set 'Y' for all rows
        (same logic as merge_engine._ensure_active_flag).
        """
        if cls._column_exists(conn, table, "nd_ActiveFlag"):
            print(f"         nd_ActiveFlag already exists — skipping ADD.")
            return

        conn.execute(text(f"ALTER TABLE {table} ADD nd_ActiveFlag CHAR(1)"))
        conn.execute(text(f"UPDATE {table} SET nd_ActiveFlag = 'Y'"))
        print(f"         nd_ActiveFlag added and set to 'Y'.")

    # ── Per-table runner ───────────────────────────────────────────────────────

    @classmethod
    def run_table(cls, table_name: str, log: dict):
        """
        Runs all 3 column operations for the given table inside a single
        transaction.  Skips the table if it already has a 'success' entry in
        the log CSV.  Logs the result (success / failure) after each table.
        """
        if log.get(table_name, {}).get("status") == "success":
            print(f"[SKIP] {table_name} — already succeeded, skipping.")
            return

        print(f"[RUN]  {table_name} — adding nd columns...")
        status    = "failure"
        error_msg = ""

        try:
            engine = sqlalchemy.create_engine(CONNECTION_STRING)
            with engine.begin() as conn:
                cls._add_nd_auto_increment_id(conn, table_name)
                cls._add_nd_extracted_date(conn, table_name)
                cls._add_nd_active_flag(conn, table_name)

            status = "success"
            print(f"[OK]   {table_name} — all 3 columns applied successfully.")
        except Exception as e:
            error_msg = str(e)
            print(f"[FAIL] {table_name} — {error_msg}")

        log[table_name] = {
            "table_name": table_name,
            "status":     status,
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "error":      error_msg,
        }
        cls._write_log(log)

    # ── Entry point ────────────────────────────────────────────────────────────

    @classmethod
    def run_all(cls):
        tables = cls._load_tables()
        log    = cls._load_log()
        print(f"Processing {len(tables)} table(s) from '{TABLES_INPUT_FILE}'...\n")
        for table in tables:
            cls.run_table(table, log)


if __name__ == "__main__":
    AddNdColumns.run_all()
