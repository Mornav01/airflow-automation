#!/usr/bin/env python3
"""
ad_mci_ecw.py — Cohort: patients with AD / MCI diagnoses in eClinicalWorks

Architecture (cross-DB):
  Source  (MSSQL) : enc + patients + edi_invoice + edi_inv_diagnosis
  Mapping (MySQL) : mapping_pg.patient_mapping_table  → ndid lookup
  Dest    (MySQL) : cohort.ad_mci_ecw  (or any DEST_TABLE below)

Flow:
  1. Query MSSQL — DISTINCT (patient_id, diag_code) matching code filters
  2. Fetch all results into memory (bounded by DISTINCT)
  3. Batch-lookup ndid from MySQL mapping table
  4. TRUNCATE + INSERT into MySQL destination (full refresh each run)

Requires:
    pip install pymssql pymysql python-dotenv tqdm

.env keys:
    DB_HOST, DB_PORT, DB_USER, DB_PASS, MAPPING_DB_NAME        MySQL (mapping + dest)
    SOURCE_DB_HOST, SOURCE_DB_PORT, SOURCE_DB_USER,
    SOURCE_DB_PASS, BASE_DB_NAME                               MSSQL (ECW source)
"""

import os
import sys
import time
from datetime import datetime
from urllib.parse import unquote
import pymysql
import pymssql
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# ══════════════════════════════════════════════════════════════════════════════
#  PARAMETERISATION — edit here to run for a different cohort
# ══════════════════════════════════════════════════════════════════════════════

# Exact codes (matched after stripping dots and uppercasing)
DIAG_EXACT_CODES: list = [
    "3310",    # Alzheimer's disease (ICD-9)
    "33183",   # Mild cognitive impairment (ICD-9)
    "G3184",   # Mild cognitive impairment (ICD-10)
]

# Prefix patterns — each entry produces LIKE '<prefix>%'
DIAG_LIKE_PREFIXES: list = [
    "G30",     # Alzheimer's disease (ICD-10 family)
]

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

DEST_TABLE  = "ndai_cohorts.ad_mci_patients"
BATCH_SIZE  = 1_000          # rows per mapping-lookup + insert batch

# MySQL — mapping table lives here; destination table is written here.
# DB_PASS is stored URL-encoded in .env (legacy %40); unquote() to the raw value
# for the direct pymysql connection — same pattern as scripts/db_config.py.
MYSQL_CONFIG = {
    "host":            os.environ.get("DB_HOST", "localhost"),
    "port":            int(os.environ.get("DB_PORT", "3306")),
    "user":            os.environ.get("DB_USER", ""),
    "password":        unquote(os.environ.get("DB_PASS", "")),
    "database":        os.environ.get("MAPPING_DB_NAME", "mapping_pg"),
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

# MSSQL — eClinicalWorks source (SOURCE_DB_PASS is stored raw)
MSSQL_CONFIG = {
    "server":        os.environ.get("SOURCE_DB_HOST", "localhost"),
    "port":          int(os.environ.get("SOURCE_DB_PORT", "1437")),
    "user":          os.environ.get("SOURCE_DB_USER", ""),
    "password":      os.environ.get("SOURCE_DB_PASS", ""),
    "database":      os.environ.get("BASE_DB_NAME", ""),
    "login_timeout": 30,
    "timeout":       21600,
    "charset":       "UTF-8",
}


# ══════════════════════════════════════════════════════════════════════════════
#  SQL BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def _ecw_sql_and_params():
    """
    Build the MSSQL SELECT and its parameter tuple from the code filter lists.
    pymssql uses %s placeholders (same as pymysql).
    """
    if not DIAG_EXACT_CODES and not DIAG_LIKE_PREFIXES:
        raise ValueError("At least one of DIAG_EXACT_CODES or DIAG_LIKE_PREFIXES must be set.")

    conditions = []
    params     = []

    if DIAG_EXACT_CODES:
        ph = ", ".join(["%s"] * len(DIAG_EXACT_CODES))
        conditions.append(f"UPPER(REPLACE(c.Code, '.', '')) IN ({ph})")
        params.extend(code.upper() for code in DIAG_EXACT_CODES)

    for prefix in DIAG_LIKE_PREFIXES:
        conditions.append("UPPER(REPLACE(c.Code, '.', '')) LIKE %s")
        params.append(f"{prefix.upper()}%")

    where = " OR ".join(conditions)

    sql = f"""
        SELECT DISTINCT
            p.pid       AS patient_id,
            p.ControlNo AS ControlNo,
            c.Code      AS diag_code
        FROM enc e
        JOIN patients p
            ON  e.patientID    = p.pid
            AND p.nd_ActiveFlag = 'Y'
        INNER JOIN edi_invoice b
            ON  e.encounterID  = b.EncounterId
            AND b.nd_ActiveFlag = 'Y'
        INNER JOIN edi_inv_diagnosis c
            ON  b.id           = c.invoiceid
            AND c.nd_ActiveFlag = 'Y'
        WHERE e.nd_ActiveFlag = 'Y'
          AND ({where})
    """
    return sql, tuple(params)


# ══════════════════════════════════════════════════════════════════════════════
#  CONNECTIONS
# ══════════════════════════════════════════════════════════════════════════════

def get_mysql():
    return pymysql.connect(**MYSQL_CONFIG)


def get_mssql():
    return pymssql.connect(
        server       = MSSQL_CONFIG["server"],
        port         = MSSQL_CONFIG["port"],
        user         = MSSQL_CONFIG["user"],
        password     = MSSQL_CONFIG["password"],
        database     = MSSQL_CONFIG["database"],
        timeout      = MSSQL_CONFIG["timeout"],
        login_timeout= MSSQL_CONFIG["login_timeout"],
        charset      = MSSQL_CONFIG["charset"],
    )


# ══════════════════════════════════════════════════════════════════════════════
#  SETUP
# ══════════════════════════════════════════════════════════════════════════════

def _table_exists(cur, full_name: str) -> bool:
    sch, tbl = full_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (sch, tbl),
    )
    return cur.fetchone()[0] > 0


def setup_dest_table(conn):
    cur = conn.cursor()
    if not _table_exists(cur, DEST_TABLE):
        cur.execute(f"""  # nosec B608 — DEST_TABLE is a hardcoded module constant, not user input
            CREATE TABLE {DEST_TABLE} (
                id               BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
                patient_id       VARCHAR(255),
                ControlNo        VARCHAR(255),
                ndid             VARCHAR(255),
                diag_code        VARCHAR(50),
                created_datetime DATETIME     DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_patient_id (patient_id),
                INDEX idx_ndid       (ndid)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
        print(f"    created {DEST_TABLE}")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {DEST_TABLE}")  # nosec B608
        print(f"    {DEST_TABLE} exists — {cur.fetchone()[0]:,} rows (will truncate)")
    cur.close()


# ══════════════════════════════════════════════════════════════════════════════
#  MAPPING LOOKUP
# ══════════════════════════════════════════════════════════════════════════════

def lookup_ndids(mysql_conn, patient_ids: list) -> dict:
    """Returns {patient_id_str: ndid} for the given list."""
    if not patient_ids:
        return {}
    ph  = ", ".join(["%s"] * len(patient_ids))
    cur = mysql_conn.cursor()
    cur.execute(
        f"SELECT patientid, nd_patient_id "
        f"FROM mapping_pg.patient_mapping_table "
        f"WHERE patientid IN ({ph})",
        tuple(str(pid) for pid in patient_ids),
    )
    result = {str(row[0]): row[1] for row in cur.fetchall()}
    cur.close()
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN ETL
# ══════════════════════════════════════════════════════════════════════════════

def run():
    t0 = time.time()

    # ── Step 1: query MSSQL ──────────────────────────────────────────────────
    print("  Connecting to MSSQL...")
    mssql_conn = get_mssql()
    mssql_cur  = mssql_conn.cursor()

    sql, params = _ecw_sql_and_params()
    print("  Executing ECW query...")
    mssql_cur.execute(sql, params)

    print("  Fetching results...")
    rows = mssql_cur.fetchall()   # list of (patient_id, ControlNo, diag_code)
    mssql_cur.close()
    mssql_conn.close()

    print(f"    {len(rows):,} DISTINCT (patient_id, diag_code) rows")

    if not rows:
        print("  No matching patients found. Exiting.")
        return 0

    # ── Step 2: prepare MySQL ────────────────────────────────────────────────
    print("\n  Connecting to MySQL...")
    mysql_conn = get_mysql()

    print("  Setting up destination table...")
    setup_dest_table(mysql_conn)

    cur = mysql_conn.cursor()
    cur.execute(f"TRUNCATE TABLE {DEST_TABLE}")  # nosec B608
    mysql_conn.commit()
    cur.close()

    # ── Step 3: batch ndid lookup + INSERT ──────────────────────────────────
    total_inserted = 0

    with tqdm(total=len(rows), desc="  Inserting", unit="row") as pbar:
        for i in range(0, len(rows), BATCH_SIZE):
            batch       = rows[i : i + BATCH_SIZE]
            patient_ids = [str(r[0]) for r in batch]

            ndid_map = lookup_ndids(mysql_conn, patient_ids)

            insert_rows = [
                (
                    str(row[0]),                          # patient_id
                    str(row[1]) if row[1] else None,      # ControlNo
                    ndid_map.get(str(row[0])),            # ndid (NULL if no mapping)
                    str(row[2]) if row[2] else None,      # diag_code
                )
                for row in batch
            ]

            cur = mysql_conn.cursor()
            cur.executemany(
                f"INSERT INTO {DEST_TABLE} (patient_id, ControlNo, ndid, diag_code) "
                f"VALUES (%s, %s, %s, %s)",
                insert_rows,
            )
            mysql_conn.commit()
            total_inserted += cur.rowcount
            cur.close()
            pbar.update(len(batch))

    mysql_conn.close()

    elapsed = round(time.time() - t0, 1)
    return total_inserted, elapsed


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{'='*70}")
    print(f"  AD / MCI Cohort — ECW  —  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  dest        : {DEST_TABLE}")
    print(f"  exact codes : {DIAG_EXACT_CODES}")
    print(f"  prefixes    : {DIAG_LIKE_PREFIXES}")
    print(f"{'='*70}\n")

    result = run()

    if result == 0:
        sys.exit(0)

    total, elapsed = result
    print(f"\n{'='*70}")
    print(f"  [DONE]  {total:,} rows inserted into {DEST_TABLE}  ({elapsed}s)")
    print(f"  Note: patients without a mapping entry have ndid = NULL")
    print(f"  Check: SELECT COUNT(*) FROM {DEST_TABLE} WHERE ndid IS NOT NULL")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
