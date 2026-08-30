#!/usr/bin/env python
"""
Greenway — backfill personid into mapping_pg.patient_mapping_table_test
-------------------------------------------------------------------------
Manual, one-off script. Steps:
  1. Pull (patientid, personid) from Greenway PROD (db_config.PROD_DB, MSSQL)
     using scripts/sql/greenway/patient_mapping.sql
  2. Add a nullable `personid` column to patient_mapping_table_test in
     mapping_pg (no-op if it already exists), then backfill it by matching
     on patientid
  3. Once every row has a personid, tighten the column to NOT NULL. If any
     row is still NULL (patientid with no PROD match), the constraint change
     is skipped and the offending rows are logged instead of failing loudly.

Usage:
    python greenway_backfill_personid.py [--mapping_db mapping_pg] [--dry-run]
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from db_config import PROD_DB  # noqa: E402 (path insert above)

import mysql.connector
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

SQL_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "scripts", "sql", "greenway", "patient_mapping.sql"
)

TABLE = "patient_mapping_table"
BATCH_SIZE = 1000


def mysql_conn_kwargs(mapping_db: str) -> dict:
    return {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "3306")),
        "database": mapping_db,
        "user": os.environ.get("DB_USER", ""),
        "password": os.environ.get("DB_PASS", ""),
    }


def fetch_prod_patient_person() -> list:
    """Run patient_mapping.sql against Greenway PROD, return (patientid, personid) pairs."""
    with open(SQL_FILE) as f:
        sql = f.read()

    engine = create_engine(PROD_DB, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql)).fetchall()
    finally:
        engine.dispose()

    pairs = [(r.patientid, r.personid) for r in rows if r.personid is not None]
    logger.info(
        "Fetched %d patient rows from PROD (%d with a non-null personid)",
        len(rows), len(pairs),
    )
    return pairs


def ensure_personid_column(cursor) -> None:
    cursor.execute(
        """
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = 'personid'
        """,
        (TABLE,),
    )
    (exists,) = cursor.fetchone()
    if exists:
        logger.info("Column personid already exists on %s — skipping ADD COLUMN", TABLE)
        return

    logger.info("Adding nullable personid column to %s", TABLE)
    cursor.execute(f"ALTER TABLE {TABLE} ADD COLUMN personid BIGINT NULL")


def backfill_personid(cursor, conn, pairs: list) -> None:
    """Bulk-backfill personid via a single JOIN UPDATE against a temp table.

    The previous approach ran one `UPDATE ... WHERE patientid = %s` per row via
    executemany() — mysql.connector only batches executemany() into fewer round
    trips for INSERT/REPLACE, so an UPDATE loop costs one network round trip per
    patient. Loading pairs into a temp table (batched INSERT) and updating via a
    single JOIN turns N round trips into ~2, regardless of N.
    """
    logger.info("Backfilling personid for %d patients into %s", len(pairs), TABLE)

    cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_personid_backfill")
    cursor.execute(
        "CREATE TEMPORARY TABLE tmp_personid_backfill ("
        "patientid BIGINT NOT NULL PRIMARY KEY, personid BIGINT NOT NULL)"
    )

    for i in range(0, len(pairs), BATCH_SIZE):
        batch = pairs[i : i + BATCH_SIZE]
        cursor.executemany(
            "INSERT INTO tmp_personid_backfill (patientid, personid) VALUES (%s, %s)",
            batch,
        )
    conn.commit()
    logger.info("Loaded %d pairs into temp table", len(pairs))

    cursor.execute(
        f"UPDATE {TABLE} t "
        "JOIN tmp_personid_backfill s ON t.patientid = s.patientid "
        "SET t.personid = s.personid"
    )
    conn.commit()
    logger.info("Backfilled personid via single JOIN UPDATE (%d rows matched)", cursor.rowcount)

    cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_personid_backfill")


def enforce_not_null(cursor) -> bool:
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE personid IS NULL")
    (null_count,) = cursor.fetchone()
    if null_count:
        cursor.execute(f"SELECT patientid FROM {TABLE} WHERE personid IS NULL LIMIT 20")
        sample = [row[0] for row in cursor.fetchall()]
        logger.warning(
            "%d row(s) in %s still have personid IS NULL — skipping NOT NULL constraint "
            "change. Sample unmatched patientid(s): %s",
            null_count, TABLE, sample,
        )
        return False

    logger.info("All rows have personid populated — tightening column to NOT NULL")
    cursor.execute(f"ALTER TABLE {TABLE} MODIFY COLUMN personid BIGINT NOT NULL")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mapping_db", default="mapping_pg",
        help="mapping_pg MySQL schema name (default: mapping_pg)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch/report from PROD only — no changes made to mapping_pg",
    )
    args = parser.parse_args()

    pairs = fetch_prod_patient_person()
    if not pairs:
        logger.info("No patientid/personid pairs found in PROD — nothing to backfill.")
        return

    if args.dry_run:
        logger.info("--dry-run set: skipping mapping_pg changes.")
        return

    conn = mysql.connector.connect(**mysql_conn_kwargs(args.mapping_db))
    cursor = conn.cursor()
    try:
        ensure_personid_column(cursor)
        conn.commit()

        backfill_personid(cursor, conn, pairs)

        enforce_not_null(cursor)
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    logger.info("Done.")


if __name__ == "__main__":
    main()
