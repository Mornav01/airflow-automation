import os
import mysql.connector
from itertools import islice

MYSQL_CONN = {
    "host": "localhost",
    "database": "mapping_test",
    "user": os.environ.get("DB_USER", ""),
    "password": os.environ.get("DB_PASS", "")
}

BATCH_SIZE = 100


def chunked(iterable, size):
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk


def fix_nd_encounter_ids():
    conn = mysql.connector.connect(**MYSQL_CONN)
    cursor = conn.cursor(dictionary=True)

    # Step 1: Find changed patients by comparing mapping (correct) vs mapping_sep (old)
    cursor.execute("""
        SELECT a.patient_id, a.nd_patient_id
        FROM mapping.patient_mapping_table a
        INNER JOIN mapping_sep.patient_mapping_table b ON a.patient_id = b.patient_id
        WHERE a.nd_patient_id <> b.nd_patient_id
    """)
    changed_patients = cursor.fetchall()

    if not changed_patients:
        print("No changed patients found. Nothing to update.")
        conn.close()
        return

    changed_patient_ids = [row["patient_id"] for row in changed_patients]
    nd_patient_map = {row["patient_id"]: row["nd_patient_id"] for row in changed_patients}

    print(f"Patients with changed nd_patient_id: {len(changed_patient_ids)}")

    # Step 2: Fetch encounters ordered only by original nd_encounter_id from mapping_sep
    format_strings = ",".join(["%s"] * len(changed_patient_ids))
    cursor.execute(f"""
        SELECT t.id, t.patient_id, t.encounter_id, t.nd_encounter_id
        FROM mapping_test.encounter_mapping_table t
        INNER JOIN mapping_sep.encounter_mapping_table s 
            ON t.patient_id = s.patient_id 
            AND t.encounter_id = s.encounter_id
        WHERE t.patient_id IN ({format_strings})
        ORDER BY s.nd_encounter_id ASC
    """, changed_patient_ids)
    rows = cursor.fetchall()

    updates = []
    last_nd_encounter_per_patient = {}

    for row in rows:
        pid = row["patient_id"]
        nd_pid = nd_patient_map[pid]

        last_nd = last_nd_encounter_per_patient.get(pid)
        if last_nd:
            new_nd_enc_id = int(last_nd) + 1
        else:
            new_nd_enc_id = int(f"{nd_pid}0001")

        last_nd_encounter_per_patient[pid] = new_nd_enc_id

        if row["nd_encounter_id"] != new_nd_enc_id:
            updates.append((new_nd_enc_id, row["id"]))

    print(f"Total encounter records to update: {len(updates)}")

    if not updates:
        print("No encounter updates needed.")
        cursor.close()
        conn.close()
        return

    # Step 3: Batch update using CASE WHEN for fewer round trips
    total_updated = 0
    for batch in chunked(updates, BATCH_SIZE):
        ids = [str(row[1]) for row in batch]
        case_clause = " ".join([f"WHEN {row[1]} THEN {row[0]}" for row in batch])
        id_list = ",".join(ids)

        sql = f"""
            UPDATE mapping_test.encounter_mapping_table
            SET nd_encounter_id = CASE id {case_clause} END,
                updated_by = 'system',
                updated_at = NOW()
            WHERE id IN ({id_list})
        """
        cursor.execute(sql)
        conn.commit()
        total_updated += len(batch)
        print(f"Updated {total_updated}/{len(updates)} records...")

    print("nd_encounter_id updated successfully.")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    fix_nd_encounter_ids()