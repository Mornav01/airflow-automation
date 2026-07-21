import pymssql
import mysql.connector
import argparse
import logging
import sys
import os
import json
import random
from datetime import datetime, date
from pathlib import Path

from db_config import STATE_DIR, MAPPING_MSSQL_CONN, MAPPING_MYSQL_CONN

# ---------- CONFIG ----------
MSSQL_CONN = MAPPING_MSSQL_CONN
MYSQL_CONN  = MAPPING_MYSQL_CONN

CREATED_BY = "system"
BATCH_SIZE = 1000

STATE_DIRECTORY = os.path.join(STATE_DIR, "mapping_master")


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
# ===================== ETL LOGIC ============================
# ============================================================

def run_etl(sql_file_path):

    now = datetime.now()  # single timestamp for entire run

    # ---- READ SQL ----
    query = Path(sql_file_path).read_text()

    # ---- MSSQL: fetch all rows for in-memory dedup ----
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

    mysql_conn = mysql.connector.connect(**MYSQL_CONN)
    mysql_cursor = mysql_conn.cursor(dictionary=True)

    try:
        # ---- ENSURE nd_ActiveFlag EXISTS ----
        ensure_active_flag_column(mysql_conn, mysql_cursor)

        # ---- LOAD EXISTING MAPPINGS ----
        mysql_cursor.execute("SELECT patient_id, nd_patient_id FROM patient_mapping_table")
        patient_map = {row["patient_id"]: row["nd_patient_id"] for row in mysql_cursor.fetchall()}
        logging.info(f"Loaded {len(patient_map)} patients into memory.")

        mysql_cursor.execute("""
            SELECT patient_id, encounter_id, nd_encounter_id, nd_ActiveFlag
            FROM encounter_mapping_table
        """)
        encounter_rows = mysql_cursor.fetchall()
        logging.info(f"Loaded {len(encounter_rows)} encounters into memory.")

        # Active encounter map: (patient_id, encounter_id) -> nd_encounter_id
        encounter_map = {
            (row["patient_id"], row["encounter_id"]): row["nd_encounter_id"]
            for row in encounter_rows
            if row["nd_ActiveFlag"] == "Y"
        }

        # All encounter_ids regardless of active/inactive
        existing_encounter_ids = set(row["encounter_id"] for row in encounter_rows)

        # O(1) reverse lookup — encounter_id -> patient_id (active only)
        encounter_id_to_patient = {eid: pid for (pid, eid) in encounter_map.keys()}

        # Build last_nd_encounter_per_patient from ALL rows (active + inactive)
        last_nd_encounter_per_patient = {}
        for row in encounter_rows:
            pid = row["patient_id"]
            nd_enc = row["nd_encounter_id"]
            if nd_enc and (pid not in last_nd_encounter_per_patient or nd_enc > last_nd_encounter_per_patient[pid]):
                last_nd_encounter_per_patient[pid] = nd_enc

        mysql_cursor.execute("SELECT COALESCE(MAX(nd_patient_id), 100000000000000) AS max_id FROM patient_mapping_table")
        max_patient_id = mysql_cursor.fetchone()["max_id"]

        new_patients = []
        new_encounters = []
        deactivate_encounters = []
        transferred_encounters = set()

        for row in src_data:
            pid = row["patient_id"]
            eid = row["encounter_id"]
            edate = row["encounter_date"]
            rdate = row["registration_date"]

            # ---- PATIENT MAPPING ----
            if pid not in patient_map:
                max_patient_id += 1
                nd_pid = max_patient_id
                patient_map[pid] = nd_pid

                offset = random.choice([
                    random.randint(-38, -30),
                    random.randint(30, 38)
                ])
                new_patients.append((
                    nd_pid, pid, offset, rdate, None,
                    CREATED_BY, now, CREATED_BY, now
                ))
            else:
                nd_pid = patient_map[pid]

            if eid is None:
                continue

            if eid in transferred_encounters:
                logging.warning(f"Encounter {eid} already transferred in this run. Skipping duplicate.")
                continue

            existing_pid = encounter_id_to_patient.get(eid)

            if existing_pid is not None and existing_pid != pid:
                logging.info(
                    f"Encounter {eid} transferring from patient {existing_pid} to {pid}. "
                    f"Marking old record as inactive."
                )
                deactivate_encounters.append((existing_pid, eid))
                del encounter_map[(existing_pid, eid)]
                del encounter_id_to_patient[eid]
                transferred_encounters.add(eid)

            elif (pid, eid) in encounter_map or eid in existing_encounter_ids:
                continue

            # ---- ASSIGN NEW nd_encounter_id ----
            last_nd = last_nd_encounter_per_patient.get(pid)
            nd_enc_id = int(last_nd) + 1 if last_nd else int(f"{nd_pid}0001")

            last_nd_encounter_per_patient[pid] = nd_enc_id
            encounter_map[(pid, eid)] = nd_enc_id
            encounter_id_to_patient[eid] = pid
            existing_encounter_ids.add(eid)

            new_encounters.append((
                pid, eid, nd_enc_id, edate,
                CREATED_BY, now, CREATED_BY, now
            ))

        logging.info(
            f"Processing complete — "
            f"New patients: {len(new_patients)}, "
            f"New encounters: {len(new_encounters)}, "
            f"Transfers to deactivate: {len(deactivate_encounters)}"
        )

        # ---- BATCH DEACTIVATE ----
        if deactivate_encounters:
            for i in range(0, len(deactivate_encounters), BATCH_SIZE):
                batch = deactivate_encounters[i:i + BATCH_SIZE]
                where_clause = " OR ".join(
                    ["(patient_id = %s AND encounter_id = %s)"] * len(batch)
                )
                # updated_by and updated_at are parameterized to avoid string injection.
                update_params = [CREATED_BY, now] + [val for pair in batch for val in pair]
                mysql_cursor.execute(f"""
                    UPDATE encounter_mapping_table
                    SET nd_ActiveFlag = 'N',
                        updated_by = %s,
                        updated_at = %s
                    WHERE {where_clause}
                """, update_params)
                logging.info(f"Deactivated batch {i // BATCH_SIZE + 1} — {len(batch)} records.")
            mysql_conn.commit()

        # ---- INSERT NEW PATIENTS ----
        if new_patients:
            mysql_cursor.executemany("""
                INSERT INTO patient_mapping_table
                (nd_patient_id, patient_id, offset, registration_date, reference_mapping, created_by, created_at, updated_by, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, new_patients)
            logging.info(f"Inserted {len(new_patients)} new patients.")

        # ---- INSERT NEW ENCOUNTERS ----
        if new_encounters:
            mysql_cursor.executemany("""
                INSERT INTO encounter_mapping_table
                (patient_id, encounter_id, nd_encounter_id, encounter_date, created_by, created_at, updated_by, updated_at, nd_ActiveFlag)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'Y')
            """, new_encounters)
            logging.info(f"Inserted {len(new_encounters)} new encounters.")

        mysql_conn.commit()

        return {
            "success": True,
            "new_patients": len(new_patients),
            "new_encounters": len(new_encounters),
            "deactivated_encounters": len(deactivate_encounters)
        }

    finally:
        mysql_cursor.close()
        mysql_conn.close()


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

    if state.get("last_processed_date") == today and state.get("success"):
        logging.info("Mapping already updated today. Skipping execution.")
        return

    logging.info("Starting Mapping Update ETL")

    try:
        result = run_etl(args.sql_file)
        state = {"last_processed_date": today, **result}
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