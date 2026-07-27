#!/usr/bin/env python
"""
Master PII Delta Script

This script updates the PII master table (pii_data_table) in the given master schema
using deltas from the staging schema.

It is a .py version of NOTEBOOK/MIND/master_pii_delta.ipynb.

Usage:
    python master_pii_delta.py --master_schema "master" --staging_schema "Primerecord_diff_staging"
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import unquote

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


MYSQL_USER = os.environ.get("DB_USER", "")
MYSQL_PASS = unquote(os.environ.get("DB_PASS", ""))  # tolerate a legacy %40-encoded value
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
    url = URL.create(
        "mysql+pymysql",
        username=MYSQL_USER,
        password=MYSQL_PASS,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=schema,
    )
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True)


def create_mssql_engine(schema: str):
    """Create SQLAlchemy engine for an MSSQL schema (staging)."""
    odbc_driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
    url = URL.create(
        "mssql+pyodbc",
        username=MSSQL_USER,
        password=MSSQL_PASS,
        host=MSSQL_HOST,
        port=MSSQL_PORT,
        database=schema,
        query={"driver": odbc_driver, "TrustServerCertificate": "yes"},
    )
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True)


# Column names must exactly match the SELECT aliases in the source query below.
ALL_PII_COLUMNS = [
    "patient_id",
    "person_id",
    "Patient_AltPatientID",
    "Patient_OrgPatientID",
    "Person_PersonID",
    "Person_DateOfBirth",
    "Person_FirstName",
    "Person_MiddleName1",
    "Person_MiddleName2",
    "Person_LastName",
    "Person_NickName",
    "Person_MaidenName",
    "Person_SSN",
    "Person_DriverLicNum",
    "Person_Pager",
    "Person_CellPhone1",
    "Person_CellPhone2",
    "Person_EMail1",
    "Person_EMail2",
    "Person_Occupation",
    "Person_PrimaryPhone",
    "Person_PrimaryWorkPhone",
    "RESIDENTIAL_AddressLine1",
    "RESIDENTIAL_AddressLine2",
    "RESIDENTIAL_City",
    "RESIDENTIAL_County",
    "RESIDENTIAL_PostalCode",
    "RESIDENTIAL_PhoneNum1",
    "RESIDENTIAL_PhoneNum2",
    "RESIDENTIAL_FaxNumber",
    "PersonEmployers_PersonalPhoneNumber",
    "Employers_Name",
    "Employers_Address1",
    "Employers_Address2",
    "Employers_City",
    "Employers_County",
    "Employers_Phone",
    "Employers_Fax",
    "Employers_ContactName",
    "BILLING_AddressLine1",
    "BILLING_AddressLine2",
    "BILLING_City",
    "BILLING_County",
    "BILLING_PostalCode",
    "BILLING_PhoneNum1",
    "BILLING_PhoneNum2",
    "BILLING_FaxNumber",
]


def run_master_pii_delta(master_schema: str, staging_schema: str, state_file: str = ""):
    """
    Apply delta updates to pii_data_table in master_schema from staging_schema.
    """
    logger.info(
        "Starting master PII delta: master_schema=%s, staging_schema=%s",
        master_schema,
        staging_schema,
    )

    # Engines — staging is MSSQL (Greenway/PrimeSUITE), master is MySQL
    old_engine = create_mysql_engine(master_schema)
    source_engine = create_mssql_engine(staging_schema)
    dest_engine = create_mysql_engine(master_schema)

    # -------------------------------------------------------------------------
    # Load existing PII patient IDs from master
    # -------------------------------------------------------------------------
    logger.info("Loading existing patient_ids from %s.pii_data_table", master_schema)
    with old_engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT patient_id FROM pii_data_table"))
        pii_data = result.fetchall()

    existing_patient_ids = {row[0] for row in pii_data}
    logger.info(
        "Existing PII rows: %d unique patient IDs: %d",
        len(pii_data),
        len(existing_patient_ids),
    )

    # -------------------------------------------------------------------------
    # Load all PII data from staging via single combined query (MSSQL)
    # -------------------------------------------------------------------------
    logger.info(
        "Loading PII data from Patient/Person/Address/PersonEmployers/Employers in %s",
        staging_schema,
    )
    with source_engine.connect() as conn:
        query = text(
            """
        SELECT
            a.PatientID                         AS patient_id,
            a.PersonID                          AS person_id,
            a.AltPatientID                      AS Patient_AltPatientID,
            a.OrgPatientID                      AS Patient_OrgPatientID,
            b.PersonID                          AS Person_PersonID,
            b.DateOfBirth                       AS Person_DateOfBirth,
            b.FirstName                         AS Person_FirstName,
            b.MiddleName1                       AS Person_MiddleName1,
            b.MiddleName2                       AS Person_MiddleName2,
            b.LastName                          AS Person_LastName,
            b.NickName                          AS Person_NickName,
            b.MaidenName                        AS Person_MaidenName,
            b.SSN                               AS Person_SSN,
            b.DriverLicNum                      AS Person_DriverLicNum,
            b.Pager                             AS Person_Pager,
            b.CellPhone1                        AS Person_CellPhone1,
            b.CellPhone2                        AS Person_CellPhone2,
            b.EMail1                            AS Person_EMail1,
            b.EMail2                            AS Person_EMail2,
            b.Occupation                        AS Person_Occupation,
            b.PrimaryPhone                      AS Person_PrimaryPhone,
            b.PrimaryWorkPhone                  AS Person_PrimaryWorkPhone,
            c.AddressLine1                      AS RESIDENTIAL_AddressLine1,
            c.AddressLine2                      AS RESIDENTIAL_AddressLine2,
            c.City                              AS RESIDENTIAL_City,
            c.County                            AS RESIDENTIAL_County,
            c.PostalCode                        AS RESIDENTIAL_PostalCode,
            c.PhoneNum1                         AS RESIDENTIAL_PhoneNum1,
            c.PhoneNum2                         AS RESIDENTIAL_PhoneNum2,
            c.FaxNumber                         AS RESIDENTIAL_FaxNumber,
            e.PersonalPhoneNumber               AS PersonEmployers_PersonalPhoneNumber,
            f.Name                              AS Employers_Name,
            f.Address1                          AS Employers_Address1,
            f.Address2                          AS Employers_Address2,
            f.City                              AS Employers_City,
            f.County                            AS Employers_County,
            f.Phone                             AS Employers_Phone,
            f.Fax                               AS Employers_Fax,
            f.ContactName                       AS Employers_ContactName,
            d.AddressLine1                      AS BILLING_AddressLine1,
            d.AddressLine2                      AS BILLING_AddressLine2,
            d.City                              AS BILLING_City,
            d.County                            AS BILLING_County,
            d.PostalCode                        AS BILLING_PostalCode,
            d.PhoneNum1                         AS BILLING_PhoneNum1,
            d.PhoneNum2                         AS BILLING_PhoneNum2,
            d.FaxNumber                         AS BILLING_FaxNumber
        FROM Patient a
        LEFT JOIN Person b          ON a.PersonID = b.PersonID
        LEFT JOIN Address c         ON b.ResidentialAddressID = c.AddressID
        LEFT JOIN Address d         ON b.BillingAddressID = d.AddressID
        LEFT JOIN PersonEmployers e ON b.PersonID = e.PersonID
        LEFT JOIN Employers f       ON e.EmployerID = f.EmployerID
            """
        )
        result = conn.execute(query)
        source_rows = result.fetchall()

    logger.info("Loaded %d source PII rows", len(source_rows))

    # -------------------------------------------------------------------------
    # Build combined source data keyed by patient_id
    # Each patient may appear multiple times (multiple employers/addresses) —
    # last row wins; extend this if you need multi-row merge logic.
    # -------------------------------------------------------------------------
    combined_source_data: dict[int, dict] = {}
    for row in source_rows:
        record = dict(zip(ALL_PII_COLUMNS, row))
        patient_id = record["patient_id"]
        combined_source_data[patient_id] = record

    data_for_insert = []
    data_for_update = []

    for patient_id, record in combined_source_data.items():
        if patient_id in existing_patient_ids:
            data_for_update.append(record)
        else:
            data_for_insert.append(record)

    logger.info(
        "Combined records: %d, INSERT: %d, UPDATE: %d",
        len(combined_source_data),
        len(data_for_insert),
        len(data_for_update),
    )

    # -------------------------------------------------------------------------
    # Execute inserts and updates on pii_data_table
    # -------------------------------------------------------------------------
    if data_for_insert:
        cols = ", ".join(f"`{k}`" for k in ALL_PII_COLUMNS)
        params = ", ".join(f":{k}" for k in ALL_PII_COLUMNS)

        insert_query = text(
            f"INSERT INTO pii_data_table ({cols}) VALUES ({params})"
        )

        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(insert_query, data_for_insert)

        logger.info("Inserted %d new PII records", len(data_for_insert))

    if data_for_update:
        set_clauses = [f"`{k}` = :{k}" for k in ALL_PII_COLUMNS if k != "patient_id"]
        set_clause_str = ", ".join(set_clauses)

        update_query = text(
            f"UPDATE pii_data_table SET {set_clause_str} WHERE patient_id = :patient_id"
        )

        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(update_query, data_for_update)

        logger.info("Updated %d existing PII records", len(data_for_update))

    logger.info("Master PII delta processing complete")

    if state_file:
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, "w") as f:
            json.dump({"success": True, "completed_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        logger.info("Wrote state file: %s", state_file)


def main():
    parser = argparse.ArgumentParser(
        description="Apply PII master table deltas (pii_data_table)"
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

    run_master_pii_delta(
        master_schema=args.master_schema,
        staging_schema=args.staging_schema,
        state_file=args.state_file,
    )


if __name__ == "__main__":
    main()
