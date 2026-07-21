#!/usr/bin/env python
"""
Master Insurance Delta Script

This script updates the master_insurance_table in the given master schema
using deltas from the staging schema.

It is a .py version of NOTEBOOK/MIND/master_insurance_delta.ipynb.

Usage:
    python master_insurance_delta.py --master_schema "master" --staging_schema "Primerecord_diff_staging"
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, text


MYSQL_USER = os.environ.get("DB_USER", "")
MYSQL_PASS = os.environ.get("DB_PASS", "")
MYSQL_HOST = os.environ.get("DB_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("DB_PORT", "3306"))

MSSQL_USER = os.environ.get("SOURCE_DB_USER", "sa")
MSSQL_PASS = os.environ.get("SOURCE_DB_PASS", "")
MSSQL_HOST = os.environ.get("SOURCE_DB_HOST") or os.environ.get("DB_HOST", "localhost")
MSSQL_PORT = int(os.environ.get("SOURCE_DB_PORT", "1433"))


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


def run_master_insurance_delta(master_schema: str, staging_schema: str, state_file: str = ""):
    """
    Apply delta updates to master_insurance_table in master_schema from staging_schema.
    """
    logger.info(
        "Starting master insurance delta: master_schema=%s, staging_schema=%s",
        master_schema,
        staging_schema,
    )

    # Engines — staging is MSSQL (Greenway/PrimeSUITE), master is MySQL
    old_engine = create_mysql_engine(master_schema)
    source_engine = create_mssql_engine(staging_schema)
    dest_engine = create_mysql_engine(master_schema)

    # -------------------------------------------------------------------------
    # Load existing insurance patient IDs from master_insurance_table
    # -------------------------------------------------------------------------
    logger.info(
        "Loading existing patient_ids from %s.master_insurance_table", master_schema
    )
    with old_engine.connect() as conn:
        query = text(
            """
        SELECT DISTINCT
            patient_id
        FROM
            master_insurance_table
        """
        )
        result = conn.execute(query)
        insurance_data = result.fetchall()

    logger.info("Existing insurance rows: %d", len(insurance_data))

    existing_patient_ids = {row[0] for row in insurance_data}
    logger.info(
        "Number of existing Patient IDs in master_insurance_table: %d",
        len(existing_patient_ids),
    )

    # -------------------------------------------------------------------------
    # Load insurance source data from staging (hcfa + edi_invoice)
    # -------------------------------------------------------------------------
    logger.info(f"Loading insurance data from Patient, InsuranceCoverage, InsurancePlan, and InsuranceProviders tables from {staging_schema}")
    with source_engine.connect() as conn:
        query = text(
            """
        select a.PatientID as patient_id, NULL as encounter_id,
        b.InsID as InsuranceCoverage_InsID, b.PolicyName as InsuranceCoverage_PolicyName, b.EmployerName as InsuranceCoverage_EmployerName, 
        b.PolicyGroupNumber as InsuranceCoverage_PolicyGroupNumber, b.PolicyNumber as InsuranceCoverage_PolicyNumber,
        b.PolicyHolder as InsuranceCoverage_PolicyHolder,
        c.PlanNumber as InsurancePlan_PlanNumber, c.PlanName as InsurancePlan_PlanName, c.ClaimPayorID as InsurancePlan_ClaimPayorID, 
        d.ProviderName as InsuranceProviders_ProviderName, d.ElectronicPayerIDNumber as InsuranceProviders_ElectronicPayerIDNumber, 
        d.ClaimPayorID as InsuranceProviders_ClaimPayorID, d.EligibilityProviderFName as InsuranceProviders_EligibilityProviderFName,
        d.EligibilityProviderMName as InsuranceProviders_EligibilityProviderMName, d.EligibilityProviderLName as InsuranceProviders_EligibilityProviderLName, d.EligibilityProviderIDNumber as InsuranceProviders_EligibilityProviderIDNumber
        from Patient a 
        INNER JOIN InsuranceCoverage b on a.PersonID=b.PersonID
        LEFT JOIN InsurancePlan c on b.PlanID=c.PlanID
        LEFT JOIN InsuranceProviders d on c.PlanProviderID = d.InsuranceProviderID;
        """
        )
        result = conn.execute(query)
        ins_data = result.fetchall()

    logger.info("Loaded %d insurance source rows", len(ins_data))

    # -------------------------------------------------------------------------
    # Categorize into insert/update lists
    # -------------------------------------------------------------------------
    INSURANCE_COLUMNS = [
        "patient_id",
        "encounter_id",
        "InsuranceCoverage_InsID",
        "InsuranceCoverage_PolicyName",
        "InsuranceCoverage_EmployerName",
        "InsuranceCoverage_PolicyGroupNumber",
        "InsuranceCoverage_PolicyNumber",
        "InsuranceCoverage_PolicyHolder",
        "InsurancePlan_PlanNumber",
        "InsurancePlan_PlanName",
        "InsurancePlan_ClaimPayorID",
        "InsuranceProviders_ProviderName",
        "InsuranceProviders_ElectronicPayerIDNumber",
        "InsuranceProviders_ClaimPayorID",
        "InsuranceProviders_EligibilityProviderFName",
        "InsuranceProviders_EligibilityProviderMName",
        "InsuranceProviders_EligibilityProviderLName",
        "InsuranceProviders_EligibilityProviderIDNumber"
    ]

    data_for_insurance_insert = []
    data_for_insurance_update = []
    total_records = len(ins_data)

    for row in ins_data:
        insurance_record = dict(zip(INSURANCE_COLUMNS, row))
        patient_id = insurance_record["patient_id"]

        if patient_id in existing_patient_ids:
            data_for_insurance_update.append(insurance_record)
        else:
            data_for_insurance_insert.append(insurance_record)

    logger.info(
        "Total Insurance records processed: %d, INSERT: %d, UPDATE: %d",
        total_records,
        len(data_for_insurance_insert),
        len(data_for_insurance_update),
    )

    # -------------------------------------------------------------------------
    # Execute inserts and updates on master_insurance_table
    # -------------------------------------------------------------------------
    if data_for_insurance_insert:
        cols = ", ".join(f"`{k}`" for k in INSURANCE_COLUMNS)
        params = ", ".join(f":{k}" for k in INSURANCE_COLUMNS)

        insert_query = text(
            f"""
        INSERT INTO master_insurance_table ({cols})
        VALUES ({params})
        """
        )

        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(insert_query, data_for_insurance_insert)

        logger.info(
            "Inserted %d new insurance records", len(data_for_insurance_insert)
        )

    if data_for_insurance_update:
        set_clauses = [
            f"`{k}` = :{k}" for k in INSURANCE_COLUMNS if k != "patient_id"
        ]
        set_clause_str = ", ".join(set_clauses)

        update_query = text(
            f"""
        UPDATE master_insurance_table
        SET {set_clause_str}
        WHERE patient_id = :patient_id
        """
        )

        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(update_query, data_for_insurance_update)

        logger.info(
            "Updated %d existing insurance records",
            len(data_for_insurance_update),
        )

    logger.info("Master insurance delta processing complete")

    if state_file:
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, "w") as f:
            json.dump({"success": True, "completed_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        logger.info("Wrote state file: %s", state_file)


def main():
    parser = argparse.ArgumentParser(
        description="Apply master_insurance_table deltas"
    )
    parser.add_argument(
        "--master_schema",
        required=True,
        help="Master schema name (e.g. 'master')",
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

    run_master_insurance_delta(
        master_schema=args.master_schema,
        staging_schema=args.staging_schema,
        state_file=args.state_file,
    )


if __name__ == "__main__":
    main()
