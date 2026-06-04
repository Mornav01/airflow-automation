import pymssql
import mysql.connector
import argparse
import logging
import sys
import os
import json
import random
from datetime import datetime, date, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))
from pathlib import Path

from db_config import (
    STATE_DIR,
    V2_MAPPING_MSSQL_CONN,
    V2_MAPPING_MYSQL_CONN,
    V2_MAPPING_PATIENT_IDENTIFIER_COLUMNS,
)

MSSQL_CONN = V2_MAPPING_MSSQL_CONN
MYSQL_CONN  = V2_MAPPING_MYSQL_CONN

# Identifier columns in patient_mapping_table (e.g. ["patientid", "profileid", "pid"])
# All columns are guaranteed 1:1; IDENTIFIER_COLUMNS[0] is used as the in-memory map key.
IDENTIFIER_COLUMNS = V2_MAPPING_PATIENT_IDENTIFIER_COLUMNS

BATCH_SIZE = 1000

STATE_DIRECTORY = os.path.join(STATE_DIR, "mapping_master_v2")


# ============================================================
# ===================== LOGGING SETUP ========================
# ============================================================

def setup_logging(state_dir):
    os.makedirs(state_dir, exist_ok=True)
    runner_dir = os.path.join(state_dir, "v2_mapping_runner")
    os.makedirs(runner_dir, exist_ok=True)

    log_file = os.path.join(
        runner_dir,
        f"v2_mapping_runner_{date.today().isoformat()}.log"
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

class MappingStateManager:

    def __init__(self, state_dir):
        self.state_file = os.path.join(state_dir, "v2_mapping_update_state.json")
        os.makedirs(state_dir, exist_ok=True)

    def load(self):
        if not os.path.exists(self.state_file):
            return {}
        with open(self.state_file, "r") as f:
            return json.load(f)

    def save(self, state):
        with open(self.state_file, "w") as f:
            json.dump(state, f, indent=4)


# ============================================================
# ============== ENSURE nd_ActiveFlag COLUMN =================
# ============================================================

def ensure_active_flag_column(mysql_conn, mysql_cursor):
    mysql_cursor.execute("""
        SELECT COUNT(*) AS cnt
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'encounter_mapping_table'
          AND COLUMN_NAME = 'nd_ActiveFlag'
    """)
    exists = mysql_cursor.fetchone()["cnt"]

    if not exists:
        logging.info("nd_ActiveFlag column not found. Creating and setting all existing rows to 'Y'.")
        mysql_cursor.execute("""
            ALTER TABLE encounter_mapping_table
            ADD COLUMN nd_ActiveFlag CHAR(1) NOT NULL DEFAULT 'Y'
        """)
        mysql_cursor.execute("UPDATE encounter_mapping_table SET nd_ActiveFlag = 'Y'")
        mysql_conn.commit()
        logging.info("nd_ActiveFlag column created and initialized.")
    else:
        logging.info("nd_ActiveFlag column already exists. Skipping initialization.")


# ============================================================
# =========== IDENTIFIER UNIQUENESS GUARD ====================
# ============================================================

def validate_identifier_uniqueness(src_data, identifier_columns):
    """Fail fast if identifier columns are not all 1:1 with each other."""
    seen = set()
    patient_rows = []
    for row in src_data:
        key = tuple(row.get(col) for col in identifier_columns)
        if key not in seen:
            seen.add(key)
            patient_rows.append(row)

    distinct_counts = {}
    for col in identifier_columns:
        distinct_counts[col] = len(set(r[col] for r in patient_rows if r[col] is not None))
    distinct_counts["combined"] = len(seen)

    counts = list(distinct_counts.values())
    if len(set(counts)) != 1:
        raise ValueError(
            f"Identifier columns are NOT 1:1. Distinct counts: {distinct_counts}. "
            f"Expected all counts to be equal. ETL aborted."
        )

    logging.info(
        f"Identifier uniqueness check passed — "
        f"{counts[0]} unique patients across columns: {identifier_columns}"
    )


# ============================================================
# ===================== ETL LOGIC ============================
# ============================================================

def run_etl(sql_file_path):

    now = datetime.now(IST).replace(tzinfo=None)

    query = Path(sql_file_path).read_text()

    conn = pymssql.connect(
        server=MSSQL_CONN["server"],
        user=MSSQL_CONN["user"],
        port=MSSQL_CONN["port"],
        password=MSSQL_CONN["password"],
        database=MSSQL_CONN["database"]
    )
    cursor = conn.cursor(as_dict=True)
    cursor.execute(query)
    src_data = cursor.fetchall()
    conn.close()

    logging.info(f"Fetched {len(src_data)} rows from MSSQL.")

    validate_identifier_uniqueness(src_data, IDENTIFIER_COLUMNS)

    mysql_conn = mysql.connector.connect(**MYSQL_CONN)
    mysql_cursor = mysql_conn.cursor(dictionary=True)

    # Adds nd_ActiveFlag to encounter_mapping_table if it doesn't exist yet.
    ensure_active_flag_column(mysql_conn, mysql_cursor)

    # Load patient map keyed on IDENTIFIER_COLUMNS[0] (all identifiers are 1:1 so any works)
    id_cols_csv = ", ".join(IDENTIFIER_COLUMNS)
    mysql_cursor.execute(f"SELECT {id_cols_csv}, nd_patient_id FROM patient_mapping_table")
    patient_map = {
        row[IDENTIFIER_COLUMNS[0]]: row["nd_patient_id"]
        for row in mysql_cursor.fetchall()
        if row[IDENTIFIER_COLUMNS[0]] is not None
    }
    logging.info(f"Loaded {len(patient_map)} patients into memory.")

    # encounter_mapping_table stores nd_patient_id (deidentified), not the source patient id.
    mysql_cursor.execute("""
        SELECT nd_patient_id, encounter_id, nd_encounter_id, nd_ActiveFlag
        FROM encounter_mapping_table
    """)
    encounter_rows = mysql_cursor.fetchall()
    logging.info(f"Loaded {len(encounter_rows)} encounters into memory.")

    encounter_map = {
        (row["nd_patient_id"], row["encounter_id"]): row["nd_encounter_id"]
        for row in encounter_rows
        if row["nd_ActiveFlag"] == "Y"
    }

    existing_encounter_ids = set(row["encounter_id"] for row in encounter_rows)

    encounter_id_to_patient = {eid: nd_pid for (nd_pid, eid) in encounter_map.keys()}

    last_nd_encounter_per_patient = {}
    for row in encounter_rows:
        nd_pid_key = row["nd_patient_id"]
        nd_enc = row["nd_encounter_id"]
        if nd_enc and (
            nd_pid_key not in last_nd_encounter_per_patient
            or nd_enc > last_nd_encounter_per_patient[nd_pid_key]
        ):
            last_nd_encounter_per_patient[nd_pid_key] = nd_enc

    mysql_cursor.execute(
        "SELECT COALESCE(MAX(nd_patient_id), 100000000000000) AS max_id FROM patient_mapping_table"
    )
    max_patient_id = mysql_cursor.fetchone()["max_id"]

    new_patients = []
    new_encounters = []
    deactivate_encounters = []
    transferred_encounters = set()

    for row in src_data:
        pid   = row[IDENTIFIER_COLUMNS[0]]
        eid   = row["encounter_id"]
        edate = row["encounter_date"]
        rdate = row["registration_date"]

        if pid not in patient_map:
            max_patient_id += 1
            nd_pid = max_patient_id
            patient_map[pid] = nd_pid

            offset = random.choice([
                random.randint(-38, -30),
                random.randint(30, 38)
            ])
            id_values = tuple(row.get(col) for col in IDENTIFIER_COLUMNS)
            # Columns: nd_patient_id, patientid, profileid, pid, offset, registration_date, created_at, updated_at
            new_patients.append(
                (nd_pid,) + id_values + (offset, rdate, now, now)
            )
        else:
            nd_pid = patient_map[pid]

        if eid is None:
            continue

        if eid in transferred_encounters:
            logging.warning(f"Encounter {eid} already transferred in this run. Skipping duplicate.")
            continue

        existing_nd_pid = encounter_id_to_patient.get(eid)

        if existing_nd_pid is not None and existing_nd_pid != nd_pid:
            logging.info(
                f"Encounter {eid} transferring from nd_patient_id {existing_nd_pid} to {nd_pid}. "
                f"Marking old record inactive."
            )
            deactivate_encounters.append((existing_nd_pid, eid))

            del encounter_map[(existing_nd_pid, eid)]
            del encounter_id_to_patient[eid]

            transferred_encounters.add(eid)

        elif (nd_pid, eid) in encounter_map or eid in existing_encounter_ids:
            continue

        last_nd = last_nd_encounter_per_patient.get(nd_pid)
        if last_nd:
            nd_enc_id = int(last_nd) + 1
        else:
            nd_enc_id = int(f"{nd_pid}0001")

        last_nd_encounter_per_patient[nd_pid] = nd_enc_id
        encounter_map[(nd_pid, eid)] = nd_enc_id
        encounter_id_to_patient[eid] = nd_pid
        existing_encounter_ids.add(eid)

        # Columns: nd_patient_id, encounter_id, nd_encounter_id, encounter_date, created_at, updated_at
        new_encounters.append((nd_pid, eid, nd_enc_id, edate, now, now))

    logging.info(
        f"Processing complete — "
        f"New patients: {len(new_patients)}, "
        f"New encounters: {len(new_encounters)}, "
        f"Transfers to deactivate: {len(deactivate_encounters)}"
    )

    if deactivate_encounters:
        for i in range(0, len(deactivate_encounters), BATCH_SIZE):
            batch = deactivate_encounters[i:i + BATCH_SIZE]
            where_clause = " OR ".join(
                ["(nd_patient_id = %s AND encounter_id = %s)"] * len(batch)
            )
            params = [val for pair in batch for val in pair]
            mysql_cursor.execute(
                f"UPDATE encounter_mapping_table "
                f"SET nd_ActiveFlag = 'N', updated_at = %s "
                f"WHERE {where_clause}",
                [now] + params
            )
            logging.info(f"Deactivated batch {i // BATCH_SIZE + 1} — {len(batch)} records.")
        mysql_conn.commit()

    if new_patients:
        col_names = ["nd_patient_id"] + IDENTIFIER_COLUMNS + [
            "offset", "registration_date", "created_at", "updated_at"
        ]
        cols_str     = ", ".join(col_names)
        placeholders = ", ".join(["%s"] * len(col_names))
        mysql_cursor.executemany(
            f"INSERT INTO patient_mapping_table ({cols_str}) VALUES ({placeholders})",
            new_patients
        )
        logging.info(f"Inserted {len(new_patients)} new patients.")

    if new_encounters:
        mysql_cursor.executemany("""
            INSERT INTO encounter_mapping_table
            (nd_patient_id, encounter_id, nd_encounter_id, encounter_date, created_at, updated_at, nd_ActiveFlag)
            VALUES (%s, %s, %s, %s, %s, %s, 'Y')
        """, new_encounters)
        logging.info(f"Inserted {len(new_encounters)} new encounters.")

    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()

    return {
        "success": True,
        "new_patients": len(new_patients),
        "new_encounters": len(new_encounters),
        "deactivated_encounters": len(deactivate_encounters)
    }


# ============================================================
# =========================== MAIN ===========================
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql_file", required=True)
    args = parser.parse_args()

    setup_logging(STATE_DIRECTORY)

    state_manager = MappingStateManager(STATE_DIRECTORY)
    state = state_manager.load()

    today = datetime.now(IST).date().isoformat()

    if state.get("last_processed_date") == today and state.get("success"):
        logging.info("V2 mapping already updated today. Skipping execution.")
        return

    logging.info("Starting V2 Mapping Update ETL")

    try:
        result = run_etl(args.sql_file)
        state = {"last_processed_date": today, **result}
        state_manager.save(state)
        logging.info("V2 Mapping Update Completed Successfully")

    except Exception as e:
        logging.exception("V2 Mapping Update Failed")
        state = {
            "last_processed_date": today,
            "success": False,
            "error": str(e)
        }
        state_manager.save(state)
        raise


if __name__ == "__main__":
    main()
