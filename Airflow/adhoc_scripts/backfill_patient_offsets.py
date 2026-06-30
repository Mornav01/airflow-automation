import os
import mysql.connector
import random
import logging
import sys
from datetime import datetime

# ---------- CONFIG ----------
MYSQL_CONN = {
    "host": "localhost",
    "database": "mapping_sep",
    "user": os.environ.get("DB_USER", ""),
    "password": os.environ.get("DB_PASS", "")
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)


def random_offset():
    """Return a random offset from [-38, -30] or [30, 38] with equal probability."""
    return random.choice([
        random.randint(-38, -30),
        random.randint(30, 38)
    ])


def backfill_offsets():
    conn = mysql.connector.connect(**MYSQL_CONN)
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT nd_patient_id FROM patient_mapping_table WHERE offset = 0")
    rows = cursor.fetchall()

    if not rows:
        logging.info("No rows with offset = 0 found. Nothing to update.")
        cursor.close()
        conn.close()
        return

    logging.info(f"Found {len(rows)} rows with offset = 0. Assigning random offsets...")

    updates = [
        (random_offset(), datetime.now(), row["nd_patient_id"])
        for row in rows
    ]

    cursor.executemany(
        "UPDATE patient_mapping_table SET offset = %s, updated_at = %s WHERE nd_patient_id = %s",
        updates
    )

    conn.commit()

    logging.info(f"Successfully updated {cursor.rowcount} rows.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    backfill_offsets()
