import os
import json
import argparse
import logging
import sys
from datetime import date, datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from db_config import (
    STATE_DIR,
    V2_MASTER_MSSQL_CONN_STR,
    V2_MASTER_MYSQL_CONN_STR,
    V2_MASTER_MAPPING_MYSQL_CONN_STR,
    V2_MASTER_PII_DATA_JOIN_COLUMN,
    V2_MASTER_INSURANCE_JOIN_COLUMN,
)

MSSQL_CONN_STR   = V2_MASTER_MSSQL_CONN_STR
MYSQL_CONN_STR   = V2_MASTER_MYSQL_CONN_STR
MAPPING_CONN_STR = V2_MASTER_MAPPING_MYSQL_CONN_STR

STATE_DIRECTORY = os.path.join(STATE_DIR, "mapping_master_v2")


# ============================================================
# ===================== LOGGING SETUP ========================
# ============================================================

def setup_logging(state_dir):
    os.makedirs(state_dir, exist_ok=True)

    runner_dir = os.path.join(state_dir, "v2_master_update_runner")
    os.makedirs(runner_dir, exist_ok=True)

    log_file = os.path.join(
        runner_dir,
        f"v2_data_transfer_{datetime.now(IST).date().isoformat()}.log"
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
        self.state_file = os.path.join(state_dir, f"v2_{mode}_state.json")

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

        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}_old"))
        logging.info(f"Dropped {table_name}_old if existed")

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
# ============== LOAD nd_patient_id MAPPING ==================
# ============================================================

def load_patient_map(mapping_conn_str, join_column):
    """Load {join_column → nd_patient_id} dict from patient_mapping_table."""
    logging.info(f"Loading {join_column} → nd_patient_id mapping from patient_mapping_table")
    engine = create_engine(mapping_conn_str)
    with engine.connect() as conn:
        df = pd.read_sql(
            text(f"SELECT {join_column}, nd_patient_id FROM patient_mapping_table"),
            conn
        )
    patient_map = dict(zip(df[join_column], df["nd_patient_id"]))
    logging.info(f"Loaded {len(patient_map)} patient mappings via {join_column}")
    return patient_map


# ============================================================
# ===================== CORE TRANSFER ========================
# ============================================================

def transfer_data(mssql_conn_str, mysql_conn_str, mapping_conn_str, query_file, target_table, join_column):

    logging.info("Reading SQL file")
    query = text(Path(query_file).read_text())

    logging.info("Connecting to MSSQL")
    mssql_engine = create_engine(mssql_conn_str)

    with mssql_engine.connect() as conn:
        df = pd.read_sql(query, conn)

    logging.info(f"Fetched {len(df)} rows")

    df.drop_duplicates(inplace=True)
    logging.info("Dropped duplicate rows")

    patient_map = load_patient_map(mapping_conn_str, join_column)
    df["nd_patient_id"] = df[join_column].map(patient_map)

    unmapped = df["nd_patient_id"].isna().sum()
    if unmapped:
        logging.warning(f"{unmapped} rows have no nd_patient_id mapping — they will be written with NULL nd_patient_id")

    logging.info("Connecting to MySQL")
    mysql_engine = create_engine(mysql_conn_str)

    rotate_table(mysql_engine, target_table)

    logging.info("Writing data to MySQL")
    df.to_sql(
        target_table,
        con=mysql_engine,
        index=False,
        if_exists="replace",
        chunksize=1000
    )

    logging.info(f"Table {target_table} created successfully with nd_patient_id")

    return {
        "success": True,
        "row_count": len(df),
        "table": target_table,
        "unmapped_rows": int(unmapped)
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

    today = datetime.now(IST).date().isoformat()

    if state.get("last_processed_date") == today and state.get("success"):
        logging.info(f"V2 {args.mode} master already processed successfully today. Skipping.")
        return

    logging.info(f"Starting V2 transfer in mode: {args.mode}")

    try:

        if args.mode == "pii_data":
            target_table = "pii_data_table"
            join_column  = V2_MASTER_PII_DATA_JOIN_COLUMN
        elif args.mode == "master_insurance":
            target_table = "master_insurance_table"
            join_column  = V2_MASTER_INSURANCE_JOIN_COLUMN

        result = transfer_data(
            MSSQL_CONN_STR,
            MYSQL_CONN_STR,
            MAPPING_CONN_STR,
            args.sql_file,
            target_table,
            join_column,
        )

        state = {
            "last_processed_date": today,
            **result
        }

        state_manager.save(state)

        logging.info("V2 transfer completed successfully")

    except Exception as e:
        logging.exception("V2 transfer failed")

        state = {
            "last_processed_date": today,
            "success": False,
            "error": str(e)
        }

        state_manager.save(state)
        raise


if __name__ == "__main__":
    main()
