import csv
import os
import sqlalchemy
from sqlalchemy import text
from datetime import datetime, timezone


# ── Configuration ─────────────────────────────────────────────────────────────
CONNECTION_STRING = "mssql+pymssql://sa:ndADMIN2025@localhost:1437/mobiledoc_base_2302"
TABLES_INPUT_FILE = "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/input/arizona_priority_tables.csv"   # CSV with a 'table_name' column
LOG_FILE          = "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/state/adhoc/constraint_removal_log_1437_new2.csv"
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
    def run_sp(cls, table_name: str, log: dict):
        """
        Calls sp_remove_identity_columns_final for the given table.
        Skips the table if it already has a 'success' entry in the log CSV.
        Logs the result (success/failure) after each run.
        """
        if log.get(table_name, {}).get("status") == "success":
            print(f"[SKIP] {table_name} — already succeeded, skipping.")
            return

        print(f"[RUN]  {table_name} — executing stored procedure...")
        status = "failure"
        error_msg = ""

        try:
            engine = sqlalchemy.create_engine(CONNECTION_STRING)
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

        log[table_name] = {
            "table_name": table_name,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": error_msg,
        }
        cls._write_log(log)

    @classmethod
    def run_all(cls):
        tables = cls._load_tables()
        log = cls._load_log()
        print(f"Processing {len(tables)} table(s) from '{TABLES_INPUT_FILE}'...\n")
        for table in tables:
            cls.run_sp(table, log)


if __name__ == "__main__":
    ConstraintsRemoval.run_all()
