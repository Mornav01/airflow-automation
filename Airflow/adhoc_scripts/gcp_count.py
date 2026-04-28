import csv
import os
import subprocess
import tempfile
from datetime import datetime

import pymysql

# ── CONFIG — edit these values for each machine ───────────────────────────────

MYSQL_HOST     = 'localhost'
MYSQL_USER     = 'ndadmin'
MYSQL_PASSWORD = 'ndADMIN@2025'
MYSQL_SCHEMA   = 'deidentified_mobiledoc_base'

GCS_BUCKET        = 'nd-platform-arizona'
GCS_BASE_PATH     = 'EHR/merge_stats'

DAG_ID = 'Arizona_priority_tables'

# ── Tables ─────────────────────────────────────────────────────────────────────

TABLES_CSV = "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/input/arizona_pt_for_deiden.csv"

with open(TABLES_CSV, newline="") as f:
    TABLES = [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]

# ── Fetch ─────────────────────────────────────────────────────────────────────

print(f"Connecting to MySQL ({MYSQL_HOST}/{MYSQL_SCHEMA})…")
conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_SCHEMA)
cur  = conn.cursor()


def _table_exists(cursor, schema, table):
    cursor.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s LIMIT 1",
        (schema, table),
    )
    return cursor.fetchone() is not None


snapshot_date = datetime.now().strftime('%Y-%m-%d')

rows   = []
errors = []
print(f"\n{'Table':<35} {'Total':>12} {'Active (Y)':>12}")
print("-" * 62)
for table in TABLES:
    if not _table_exists(cur, MYSQL_SCHEMA, table):
        print(f"{table:<35} {'MISSING':>12}")
        continue
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        total = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE nd_ActiveFlag = 'Y'")
        active = cur.fetchone()[0]
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{table:<35} {total:>12,} {active:>12,}")
        rows.append({
            "snapshot_date": snapshot_date,
            "dag_id":        DAG_ID,
            "table_name":    table,
            "mac_total":     int(total),
            "mac_active_y":  int(active),
            "gcp_total":     "",
            "gcp_active_y":  "",
            "discrepancy":   "",
            "created_at":    created_at,
        })
    except Exception as e:
        print(f"{table:<35} {'ERROR':>12}   {e}")
        errors.append((table, str(e)))

conn.close()

# ── Upload to GCS ─────────────────────────────────────────────────────────────

date_folder = datetime.now().strftime("%m%d%Y")
gcs_uri = f"gs://{GCS_BUCKET}/{GCS_BASE_PATH}/{date_folder}/merge_stats.csv"

print(f"\nUploading to {gcs_uri} …")
fieldnames = [
    "snapshot_date", "dag_id", "table_name",
    "mac_total", "mac_active_y",
    "gcp_total", "gcp_active_y", "discrepancy",
    "created_at",
]
with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="") as tmp:
    writer = csv.DictWriter(tmp, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    tmp_path = tmp.name

try:
    subprocess.run(["gsutil", "cp", tmp_path, gcs_uri], check=True)
finally:
    os.unlink(tmp_path)

print("Done.")

if errors:
    detail = "\n".join(f"  {t}: {msg}" for t, msg in errors)
    raise RuntimeError(f"Errors occurred for {len(errors)} table(s):\n{detail}")
