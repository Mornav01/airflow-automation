import pymssql
import mysql.connector
import argparse
import logging
import sys
import os
import json
from datetime import datetime, date
from pathlib import Path


# ---------- CONFIG ----------
MSSQL_CONN = {
    "server": "localhost",
    "port": 1437,
    "database": "mobiledoc_latest_2302",
    "user": "sa",
    "password": "ndADMIN2025"
}

MYSQL_CONN = {
    "host": "localhost",
    "database": "mapping_sep",
    "user": "ndadmin",
    "password": "ndADMIN@2025"
}
CREATED_BY = "system"

STATE_DIRECTORY = "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/state/mapping_master"



# ============================================================
# ===================== LOGGING SETUP ========================
# ============================================================

def setup_logging(state_dir):
    os.makedirs(state_dir, exist_ok=True)
    runner_dir = os.path.join(state_dir, "mapping_runner")
    os.makedirs(runner_dir, exist_ok=True)

    log_file = os.path.join(
        runner_dir,
        f"mapping_runner_{date.today().isoformat()}.log"
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
        self.state_file = os.path.join(state_dir, "mapping_update_state.json")
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
# ===================== ETL LOGIC (UNCHANGED) ================
# ============================================================

def run_etl(sql_file_path):

    # ---- READ SQL ----
    query = Path(sql_file_path).read_text()

    # ---- MSSQL ----
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

    mysql_conn = mysql.connector.connect(**MYSQL_CONN)
    mysql_cursor = mysql_conn.cursor(dictionary=True)

    mysql_cursor.execute("SELECT patient_id, nd_patient_id FROM patient_mapping_table")
    patient_map = {row["patient_id"]: row["nd_patient_id"] for row in mysql_cursor.fetchall()}

    mysql_cursor.execute("SELECT patient_id, encounter_id, nd_encounter_id FROM encounter_mapping_table")
    encounter_rows = mysql_cursor.fetchall()
    encounter_map = {(row["patient_id"], row["encounter_id"]): row["nd_encounter_id"] for row in encounter_rows}
    existing_encounter_ids = set(row["encounter_id"] for row in encounter_rows)

    last_nd_encounter_per_patient = {}
    for (pid, _), nd_enc in encounter_map.items():
        if pid not in last_nd_encounter_per_patient or nd_enc > last_nd_encounter_per_patient[pid]:
            last_nd_encounter_per_patient[pid] = nd_enc

    mysql_cursor.execute("SELECT COALESCE(MAX(nd_patient_id), 100000000000000) AS max_id FROM patient_mapping_table")
    max_patient_id = mysql_cursor.fetchone()["max_id"]

    new_patients = []
    new_encounters = []

    for row in src_data:
        pid = row["patient_id"]
        eid = row["encounter_id"]
        edate = row["encounter_date"]
        rdate = row["registration_date"]

        if pid not in patient_map:
            max_patient_id += 1
            nd_pid = max_patient_id
            patient_map[pid] = nd_pid

            new_patients.append((
                nd_pid, pid, 0, rdate, None,
                CREATED_BY, datetime.now(), CREATED_BY, datetime.now()
            ))
        else:
            nd_pid = patient_map[pid]

        if eid is None or (pid, eid) in encounter_map or eid in existing_encounter_ids:
            continue

        last_nd = last_nd_encounter_per_patient.get(pid)
        if last_nd:
            nd_enc_id = int(last_nd) + 1
        else:
            nd_enc_id = int(f"{nd_pid}0001")

        last_nd_encounter_per_patient[pid] = nd_enc_id
        encounter_map[(pid, eid)] = nd_enc_id
        existing_encounter_ids.add(eid)

        new_encounters.append((
            pid, eid, nd_enc_id, edate,
            CREATED_BY, datetime.now(), CREATED_BY, datetime.now()
        ))

    if new_patients:
        mysql_cursor.executemany("""
            INSERT INTO patient_mapping_table
            (nd_patient_id, patient_id, offset, registration_date, reference_mapping, created_by, created_at, updated_by, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, new_patients)

    if new_encounters:
        mysql_cursor.executemany("""
            INSERT INTO encounter_mapping_table
            (patient_id, encounter_id, nd_encounter_id, encounter_date, created_by, created_at, updated_by, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, new_encounters)

    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()

    return {
        "success": True,
        "new_patients": len(new_patients),
        "new_encounters": len(new_encounters)
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

    today = date.today().isoformat()

    # -------- SKIP LOGIC --------
    if state.get("last_processed_date") == today and state.get("success"):
        logging.info("Mapping already updated today. Skipping execution.")
        return

    logging.info("Starting Mapping Update ETL")

    try:
        result = run_etl(args.sql_file)

        state = {
            "last_processed_date": today,
            **result
        }

        state_manager.save(state)

        logging.info("Mapping Update Completed Successfully")

    except Exception as e:
        logging.exception("Mapping Update Failed")

        state = {
            "last_processed_date": today,
            "success": False,
            "error": str(e)
        }

        state_manager.save(state)
        raise


if __name__ == "__main__":
    main()