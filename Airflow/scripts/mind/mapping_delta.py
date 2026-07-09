#!/usr/bin/env python
"""
Mapping Delta Script

This script applies patient and encounter mapping deltas into the mapping schema.
It is a .py version of NOTEBOOK/MIND/mapping_delta.ipynb.

It:
- Reads existing patient/encounter mappings from mapping_schema
- Reads source patients/encounters from staging_schema
- Inserts/updates patient_mapping_table and encounter_mapping_table

Usage:
    python mapping_delta.py --mapping_schema "mapping" --staging_schema "Primerecord_diff_staging"
"""

import argparse
import json
import logging
import os
import random
import pandas as pd
from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Column,
    DATETIME,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    UniqueConstraint,
    Index,
    create_engine,
    update,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import bindparam


MYSQL_USER = os.environ.get("DB_USER", "")
MYSQL_PASS = os.environ.get("DB_PASS", "")
MYSQL_HOST = os.environ.get("DB_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("DB_PORT", "3306"))

MSSQL_USER = os.environ.get("SOURCE_USER", "sa")
MSSQL_PASS = os.environ.get("SOURCE_PW", "")
MSSQL_HOST = os.environ.get("DB_HOST", "localhost")
MSSQL_PORT = int(os.environ.get("MSSQL_PORT", "1433"))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def create_mysql_engine(schema: str):
    """Create SQLAlchemy engine for a MySQL schema."""
    url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{schema}"
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True)


def create_mssql_engine(schema: str):
    """Create SQLAlchemy engine for an MSSQL schema (staging)."""
    from urllib.parse import quote_plus
    odbc_driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server").replace(" ", "+")
    url = (
        f"mssql+pyodbc://{quote_plus(MSSQL_USER)}:{quote_plus(MSSQL_PASS)}"
        f"@{MSSQL_HOST}:{MSSQL_PORT}/{schema}"
        f"?driver={odbc_driver}&TrustServerCertificate=yes"
    )
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True)


def ensure_active_flag_column(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) AS cnt
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'encounter_mapping_table'
              AND COLUMN_NAME = 'nd_ActiveFlag'
        """))
        if result.scalar() == 0:
            conn.execute(text("""
                ALTER TABLE encounter_mapping_table
                ADD COLUMN nd_ActiveFlag CHAR(1) NOT NULL DEFAULT 'Y'
            """))
            conn.execute(text("UPDATE encounter_mapping_table SET nd_ActiveFlag = 'Y'"))
            conn.commit()
            logger.info("nd_ActiveFlag column added and initialized to 'Y'")
        else:
            logger.info("nd_ActiveFlag column already exists")


def run_mapping_delta(mapping_schema: str, staging_schema: str, state_file: str = ""):
    """
    Run mapping delta logic:
    - Update/insert into patient_mapping_table
    - Update/insert into encounter_mapping_table
    """
    logger.info(
        "Starting mapping delta: mapping_schema=%s, staging_schema=%s",
        mapping_schema,
        staging_schema,
    )

    # Engines — staging is MSSQL (Greenway/PrimeSUITE), mapping is MySQL
    old_engine = create_mysql_engine(mapping_schema)
    source_engine = create_mssql_engine(staging_schema)
    dest_engine = create_mysql_engine(mapping_schema)

    # Ensure nd_ActiveFlag exists before any SELECT touches it
    ensure_active_flag_column(dest_engine)

    metadata = MetaData()

    # -------------------------------------------------------------------------
    # Load existing mapping data from mapping_schema
    # -------------------------------------------------------------------------
    logger.info("Loading existing mapping data from %s", mapping_schema)
    with old_engine.connect() as conn:
        old_query = text(
            """
    SELECT DISTINCT
        a.patient_id,
        a.nd_patient_id,
        a.offset,
        b.encounter_id,
        b.nd_encounter_id,
        b.created_by,
        b.created_at,
        b.updated_by,
        b.updated_at,
        b.nd_ActiveFlag
    FROM patient_mapping_table AS a
    LEFT JOIN encounter_mapping_table AS b
        ON a.patient_id = b.patient_id
    """
        )
        result = conn.execute(old_query)
        old_data = result.fetchall()

    logger.info("Loaded %d existing mapping rows", len(old_data))

    old_pat_ids: dict[int, list] = {}
    old_pat_encids: dict[int, dict] = defaultdict(dict)
    old_enc_to_patient: dict[int, int] = {}  # encounter_id → patient_id (active only, for transfer detection)
    old_created_by = None
    old_created_at = None
    old_updated_by = None
    old_updated_at = None

    last_patid = 0

    for row in old_data:
        # created/updated metadata from encounter rows if present
        if row[5]:
            old_created_by = row[5]
            old_created_at = row[6]
            old_updated_by = row[7]
            old_updated_at = row[8]

        # Track max nd_patient_id
        if row[1] > last_patid:
            last_patid = row[1]

        # patient_id → [nd_patient_id, offset]
        old_pat_ids[row[0]] = [row[1], row[2]]

        # encounter mapping
        if row[3]:
            if (
                "last_encid" not in old_pat_encids[row[0]]
                or row[4] > old_pat_encids[row[0]]["last_encid"]
            ):
                old_pat_encids[row[0]]["last_encid"] = row[4]

            old_pat_encids[row[0]][row[3]] = row[4]

            # Build reverse map for active encounters (used for transfer detection)
            if row[10] == 'Y':
                old_enc_to_patient[row[3]] = row[0]

    logger.info(
        "Existing patient mappings: %d, encounter mappings: %d, last_patid=%s, "
        "created_by=%s, created_at=%s, updated_by=%s, updated_at=%s",
        len(old_pat_ids),
        len(old_pat_encids),
        last_patid,
        old_created_by,
        old_created_at,
        old_updated_by,
        old_updated_at,
    )

    # -------------------------------------------------------------------------
    # Load delta data from staging_schema
    # -------------------------------------------------------------------------
    logger.info(f"Loading delta patients from Patient and br_Account tables from {staging_schema}")
    with source_engine.connect() as conn:
        query = text(
            f"""
        select p.PatientID, acc.AcctOpenDate, p.PersonID 
        from Patient as p
        left join br_Account as acc
        on p.PatientID = acc.PatientID
        order by 1, 2
        """
        )
        result = conn.execute(query)
        users_data = result.fetchall()

    logger.info("Loaded %d user registration rows", len(users_data))

    logger.info(f"Loading delta encounters from Visit table from {staging_schema}")
    with source_engine.connect() as conn:
        query = text(
            f"""
        select v.PatientID, v.CreateDate, v.VisitID
        from Visit as v
        order by 1, 2, 3
        """
        )
        result = conn.execute(query)
        enc_data = result.fetchall()

    logger.info("Loaded %d encounter rows", len(enc_data))

    # -------------------------------------------------------------------------
    # Ensure mapping tables exist in mapping_schema
    # -------------------------------------------------------------------------
    patient_mapping_table = Table(
        "patient_mapping_table",
        metadata,
        Column("nd_patient_id", BigInteger, primary_key=True, autoincrement=True),
        Column("patient_id", Integer, nullable=False),
        Column("offset", Integer, nullable=False),
        Column("registration_date", DATETIME, nullable=True),
        Column("reference_mapping", Integer, nullable=True),
        Column("created_by", String(50), nullable=False),
        Column("created_at", DATETIME, nullable=False),
        Column("updated_by", String(50), nullable=False),
        Column("updated_at", DATETIME, nullable=False),
        UniqueConstraint("patient_id", name="uq_patient_id"),
        Index("ix_patient_id", "patient_id"),
    )

    encounter_mapping_table = Table(
        "encounter_mapping_table",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("patient_id", Integer, nullable=False),
        Column("encounter_id", Integer, nullable=False),
        Column("nd_encounter_id", Numeric(20, 0), nullable=False),
        Column("encounter_date", DATETIME, nullable=True),
        Column("created_by", String(50), nullable=False),
        Column("created_at", DATETIME, nullable=False),
        Column("updated_by", String(50), nullable=False),
        Column("updated_at", DATETIME, nullable=False),
        Column("nd_ActiveFlag", String(1), nullable=False, server_default="Y"),
        UniqueConstraint("nd_encounter_id", name="uq_nd_encounter_id"),
        Index("ix_patient_id", "patient_id"),
    )

    metadata.create_all(dest_engine)
    logger.info("Ensured patient_mapping_table and encounter_mapping_table exist")

    # -------------------------------------------------------------------------
    # STEP 1: Process & upsert delta patients
    # -------------------------------------------------------------------------
    possible_offsets = list(range(-38, -29)) + list(range(30, 39))

    existing_patient_ids: dict[int, list] = {}
    patient_encounters: dict[int, int] = {}

    if users_data:
        patients_to_insert = []
        patients_to_update = []

        for patient_id, registration_date, reference_id in users_data:
            # New patient: not in history and not yet processed in this run
            if patient_id not in old_pat_ids and patient_id not in existing_patient_ids:
                offset = random.choice(possible_offsets)
                last_patid += 1
                nd_patient_id = last_patid

                patients_to_insert.append(
                    {
                        "nd_patient_id": nd_patient_id,
                        "patient_id": patient_id,
                        "offset": offset,
                        "registration_date": registration_date,
                        "reference_mapping": reference_id,
                        "created_by": "nd-admin",
                        "created_at": datetime.now(timezone.utc),
                        "updated_by": "nd-admin",
                        "updated_at": datetime.now(timezone.utc),
                    }
                )

                # Update in-memory map for use in Step 2 (Encounters)
                existing_patient_ids[patient_id] = [nd_patient_id, offset]
            else:
                patients_to_update.append(
                    {
                        "patient_id": patient_id,
                        "registration_date": registration_date,
                        "updated_by": "nd-admin",
                        "updated_at": datetime.now(timezone.utc),
                    }
                )

        logger.info(
            "Patients to insert: %d, to update: %d",
            len(patients_to_insert),
            len(patients_to_update),
        )

        if patients_to_insert or patients_to_update:
            with dest_engine.connect() as conn:
                trans = conn.begin()
                try:
                    if patients_to_insert:
                        conn.execute(
                            patient_mapping_table.insert(), patients_to_insert
                        )

                    if patients_to_update:
                        stmt = (
                            update(patient_mapping_table)
                            .where(
                                patient_mapping_table.c.patient_id == bindparam(
                                    "b_pid"
                                )
                            )
                            .values(
                                {
                                    "registration_date": bindparam("b_reg_date"),
                                    "updated_by": bindparam("b_user"),
                                    "updated_at": bindparam("b_ts"),
                                }
                            )
                        )

                        bulk_params = [
                            {
                                "b_pid": d["patient_id"],
                                "b_reg_date": d["registration_date"],
                                "b_user": d["updated_by"],
                                "b_ts": d["updated_at"],
                            }
                            for d in patients_to_update
                        ]

                        conn.execute(stmt, bulk_params)

                    trans.commit()
                    logger.info("Patient upsert completed successfully")
                except SQLAlchemyError as e:
                    trans.rollback()
                    logger.error("Error during patient upsert: %s", e)
                    raise
        else:
            logger.info("No patient records to insert or update")
    else:
        logger.info("No delta patient records found in staging")

    # -------------------------------------------------------------------------
    # STEP 2: Process & upsert delta encounters
    # -------------------------------------------------------------------------
    if enc_data:
        encounters_to_insert = []
        encounters_to_update = []
        encounters_to_deactivate = []

        # Build a set of ALL nd_encounter_ids already in use (from history) to prevent duplicates
        used_nd_encounter_ids = set()
        for pid, enc_map in old_pat_encids.items():
            for k, v in enc_map.items():
                if k != 'last_encid' and v:
                    used_nd_encounter_ids.add(int(v))

        # 1. Prepare data for INSERT or UPDATE
        for patient_id, encounter_date, mind_encounter_id in enc_data:
            # --- FIX 1: CLEAR ALL VARIABLES AT START OF LOOP ---
            target_nd_patient_id = None
            hist = None

            # Unified lookup for the patient mapping
            patient_info = old_pat_ids.get(patient_id) or existing_patient_ids.get(patient_id)

            if patient_info:
                target_nd_patient_id = patient_info[0]
            else:
                # If we can't find the patient, skip immediately to avoid ghosting
                continue
            
            # TRANSFER CHECK: encounter exists in mapping but under a different patient
            existing_patient = old_enc_to_patient.get(mind_encounter_id)
            if existing_patient is not None and existing_patient != patient_id:
                logger.info(
                    "Encounter %s transferring from patient %s to %s — marking old record inactive",
                    mind_encounter_id, existing_patient, patient_id,
                )
                encounters_to_deactivate.append({
                    "b_pid": existing_patient,
                    "b_enc_id": mind_encounter_id,
                    "b_updated_by": "nd-admin",
                    "b_updated_at": datetime.now(timezone.utc),
                })
                del old_enc_to_patient[mind_encounter_id]
                old_pat_encids.get(existing_patient, {}).pop(mind_encounter_id, None)
                # Fall through to INSERT logic below

            # DELTA CHECK: encounter already exists in history
            hist = old_pat_encids.get(patient_id)
            if hist and mind_encounter_id in hist:
                # --- UPDATE LOGIC ---
                
                encounters_to_update.append({
                    'encounter_id': mind_encounter_id, # Key for WHERE clause
                    'encounter_date': encounter_date,
                    'updated_by': 'nd-admin',
                    'updated_at': datetime.now(timezone.utc)
                })
                continue

            # 2. Generate New nd_encounter_id
            
            # Get the last known nd_encounter_id for this patient (historical or run-time)
            last_encid_hist = hist['last_encid'] if hist else 0
            last_encid_run = patient_encounters.get(patient_id, 0)

            max_last_encid = max(last_encid_hist, last_encid_run)

            if max_last_encid > 0:
                # Case A: History or current run has generated IDs, increment from the max
                nd_encounter_id = max_last_encid + 1
            else:
                # Case B: First encounter for this patient (either entirely new)
                # Use the base formula
                nd_encounter_id = (target_nd_patient_id * 10000) + 1

            # FIX: Ensure nd_encounter_id is globally unique (avoids duplicate key when different
            # patients share same nd_patient_id or when IDs collide across batch)
            while nd_encounter_id in used_nd_encounter_ids:
                nd_encounter_id += 1
            used_nd_encounter_ids.add(nd_encounter_id)
            
            # Update the run-time tracker
            patient_encounters[patient_id] = nd_encounter_id

            # Prepare for insertion
            encounters_to_insert.append({
                'patient_id': patient_id,
                'encounter_id': mind_encounter_id,
                'nd_encounter_id': nd_encounter_id,
                'encounter_date': encounter_date,
                'nd_ActiveFlag': 'Y',
                'created_by': 'nd-admin',
                'created_at': datetime.now(timezone.utc),
                'updated_by': 'nd-admin',
                'updated_at': datetime.now(timezone.utc)
            })

        logger.info(
            "Encounters to insert: %d, to update: %d, to deactivate: %d",
            len(encounters_to_insert),
            len(encounters_to_update),
            len(encounters_to_deactivate),
        )

        if encounters_to_insert or encounters_to_update or encounters_to_deactivate:
            with dest_engine.connect() as conn:
                trans = conn.begin()
                try:
                    if encounters_to_deactivate:
                        stmt = (
                            update(encounter_mapping_table)
                            .where(
                                (encounter_mapping_table.c.patient_id == bindparam("b_pid"))
                                & (encounter_mapping_table.c.encounter_id == bindparam("b_enc_id"))
                            )
                            .values({
                                "nd_ActiveFlag": "N",
                                "updated_by": bindparam("b_updated_by"),
                                "updated_at": bindparam("b_updated_at"),
                            })
                        )
                        conn.execute(stmt, encounters_to_deactivate)
                        logger.info(
                            "Deactivated %d transferred encounter records",
                            len(encounters_to_deactivate),
                        )

                    if encounters_to_insert:
                        conn.execute(
                            encounter_mapping_table.insert(), encounters_to_insert
                        )
                        logger.info(
                            "Bulk inserted %d encounter records",
                            len(encounters_to_insert),
                        )

                    if encounters_to_update:
                        stmt = (
                            update(encounter_mapping_table)
                            .where(
                                encounter_mapping_table.c.encounter_id
                                == bindparam("b_enc_id")
                            )
                            .values(
                                {
                                    "encounter_date": bindparam("b_enc_date"),
                                    "updated_by": bindparam("b_updated_by"),
                                    "updated_at": bindparam("b_updated_at"),
                                }
                            )
                        )

                        bulk_update_data = [
                            {
                                "b_enc_id": row["encounter_id"],
                                "b_enc_date": row["encounter_date"],
                                "b_updated_by": row["updated_by"],
                                "b_updated_at": row["updated_at"],
                            }
                            for row in encounters_to_update
                        ]

                        conn.execute(stmt, bulk_update_data)
                        logger.info(
                            "Bulk updated %d encounter records",
                            len(encounters_to_update),
                        )

                    trans.commit()
                    logger.info("Encounter upsert completed successfully")
                except SQLAlchemyError as e:
                    trans.rollback()
                    logger.error("Error during encounter upsert: %s", e)
                    raise
        else:
            logger.info("No encounter records to insert or update")
    else:
        logger.info("No delta encounter records found in staging")

    logger.info("Mapping delta processing complete")

    if state_file:
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, "w") as f:
            json.dump({"success": True, "completed_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        logger.info("Wrote state file: %s", state_file)


def main():
    parser = argparse.ArgumentParser(
        description="Apply mapping deltas to patient_mapping_table and encounter_mapping_table"
    )
    parser.add_argument(
        "--mapping_schema",
        required=True,
        help="Mapping schema name (e.g. 'mapping')",
    )
    parser.add_argument(
        "--staging_schema",
        required=True,
        help="Staging schema name (e.g. 'Primerecord_diff_staging')",
    )
    parser.add_argument(
        "--state_file",
        default="",
        help="Optional path to write JSON state file on success",
    )

    args = parser.parse_args()

    run_mapping_delta(
        mapping_schema=args.mapping_schema,
        staging_schema=args.staging_schema,
        state_file=args.state_file,
    )


if __name__ == "__main__":
    main()

