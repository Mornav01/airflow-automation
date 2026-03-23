import os
import json
import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from db_config import STATE_DIR

MSSQL_CONN_STR = "mssql+pymssql://sa:ndADMIN2025@localhost:1437/mobiledoc_latest"
MYSQL_CONN_STR = "mysql+pymysql://root:ndADMIN%402025@localhost:3306/master_sep"

STATE_DIRECTORY = os.path.join(STATE_DIR, "mapping_master")


# ============================================================
# ===================== LOGGING SETUP ========================
# ============================================================

def setup_logging(state_dir):
    os.makedirs(state_dir, exist_ok=True)

    runner_dir = os.path.join(state_dir, "master_update_runner")
    os.makedirs(runner_dir, exist_ok=True)

    log_file = os.path.join(
        runner_dir,
        f"data_transfer_{date.today().isoformat()}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )


# ============================================================
# ===================== STATE MANAGER ========================
# ============================================================

class StateManager:

    def __init__(self, state_dir, mode):
        self.state_file = os.path.join(state_dir, f"{mode}_state.json")

    def load(self):
        if not os.path.exists(self.state_file):
            return {}
        with open(self.state_file, "r") as f:
            return json.load(f)

    def save(self, state):
        with open(self.state_file, "w") as f:
            json.dump(state, f, indent=4)


# ============================================================
# ===================== TABLE ROTATION =======================
# ============================================================

def rotate_table(mysql_engine, table_name):

    logging.info(f"Rotating table: {table_name}")

    with mysql_engine.connect() as conn:

        # Drop old backup if exists
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}_old"))
        logging.info(f"Dropped {table_name}_old if existed")

        # Check if main table exists
        result = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                AND table_name = :table_name
            """),
            {"table_name": table_name}
        ).scalar()

        if result:
            conn.execute(text(
                f"RENAME TABLE {table_name} TO {table_name}_old"
            ))
            logging.info(f"Renamed {table_name} → {table_name}_old")

        conn.commit()


# ============================================================
# ===================== CORE TRANSFER ========================
# ============================================================

def transfer_data(mssql_conn_str, mysql_conn_str, query_file, target_table):

    logging.info("Reading SQL file")
    query = text(Path(query_file).read_text())

    # MSSQL
    logging.info("Connecting to MSSQL")
    mssql_engine = create_engine(mssql_conn_str)

    with mssql_engine.connect() as conn:
        df = pd.read_sql(query, conn)

    logging.info(f"Fetched {len(df)} rows")

    df.drop_duplicates(inplace=True)
    logging.info("Dropped duplicate rows")

    # MySQL
    logging.info("Connecting to MySQL")
    mysql_engine = create_engine(mysql_conn_str)

    # Rotate tables
    rotate_table(mysql_engine, target_table)

    # Write new table
    logging.info("Writing data to MySQL")
    df.to_sql(
        target_table,
        con=mysql_engine,
        index=False,
        if_exists="replace",
        chunksize=1000
    )

    logging.info(f"Table {target_table} created successfully")

    return {
        "success": True,
        "row_count": len(df),
        "table": target_table
    }


# ============================================================
# =========================== MAIN ===========================
# ============================================================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--mode", required=True, choices=["pii_data", "master_insurance"])
    parser.add_argument("--sql_file", required=True)

    args = parser.parse_args()

    setup_logging(STATE_DIRECTORY)

    state_manager = StateManager(STATE_DIRECTORY, args.mode)
    state = state_manager.load()

    today = date.today().isoformat()

    # Skip if already successful today
    if state.get("last_processed_date") == today and state.get("success"):
        logging.info(f"{args.mode} master already processed successfully today. Skipping.")
        return

    logging.info(f"Starting transfer in mode: {args.mode}")

    try:

        if args.mode == "pii_data":
            target_table = "pii_data_table"
        elif args.mode == "master_insurance":
            target_table = "master_insurance_table"

        result = transfer_data(
            MSSQL_CONN_STR,
            MYSQL_CONN_STR,
            args.sql_file,
            target_table
        )

        state = {
            "last_processed_date": today,
            **result
        }

        state_manager.save(state)

        logging.info("Transfer completed successfully")

    except Exception as e:
        logging.exception("Transfer failed")

        state = {
            "last_processed_date": today,
            "success": False,
            "error": str(e)
        }

        state_manager.save(state)
        raise


if __name__ == "__main__":
    main()