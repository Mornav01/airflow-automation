#!/usr/bin/env python
"""
Master PII Delta Script  (new master_pg structure — Greenway / PrimeSUITE)

Updates the PII master table (pii_data_table) in the given master schema using
deltas from the staging schema.

New master_pg structure: pii_data_table is keyed on nd_patient_id (join key) with
patientid retained (person_id is kept as a normal PII/reference column). This script
resolves each source patient_id → nd_patient_id via patient_mapping_table in
--mapping_schema, then writes with a single INSERT ... ON DUPLICATE KEY UPDATE keyed
on uq_nd_patient_id. NULL source values never overwrite an existing value (COALESCE).

Staging is MSSQL (Greenway/PrimeSUITE); master and mapping (dest) schemas are MySQL.

Usage:
    python master_pii_delta.py --master_schema "master_pg" \
        --staging_schema "Primerecord_diff_staging" --mapping_schema "mapping_pg"
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
    """MySQL engine for master / mapping schemas (always MySQL)."""
    url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{schema}"
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True)


def create_mssql_engine(schema: str):
    """MSSQL engine for the Greenway/PrimeSUITE staging source."""
    from urllib.parse import quote_plus
    odbc_driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server").replace(" ", "+")
    url = (
        f"mssql+pyodbc://{quote_plus(MSSQL_USER)}:{quote_plus(MSSQL_PASS)}"
        f"@{MSSQL_HOST}:{MSSQL_PORT}/{schema}"
        f"?driver={odbc_driver}&TrustServerCertificate=yes"
    )
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True)


def load_patient_map(mapping_schema: str) -> dict:
    """Return {patientid (raw source id) → nd_patient_id} from patient_mapping_table."""
    engine = create_mysql_engine(mapping_schema)
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT patientid, nd_patient_id FROM patient_mapping_table")
        ).fetchall()
    return {row[0]: row[1] for row in rows}


# Value columns stored in pii_data_table (patient_id resolves nd_patient_id and is
# kept as `patientid`; everything else — including person_id — is a value column).
PII_VALUE_COLUMNS = [
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

# Order of columns returned by the source SELECT below (patient_id first).
SOURCE_COLUMNS = ["patient_id"] + PII_VALUE_COLUMNS


def run_master_pii_delta(master_schema: str, staging_schema: str, mapping_schema: str, state_file: str = ""):
    """
    Apply delta updates to pii_data_table in master_schema from staging_schema,
    keyed on nd_patient_id resolved via mapping_schema.
    """
    logger.info(
        "Starting master PII delta: master_schema=%s, staging_schema=%s, mapping_schema=%s",
        master_schema,
        staging_schema,
        mapping_schema,
    )

    source_engine = create_mssql_engine(staging_schema)
    dest_engine = create_mysql_engine(master_schema)

    # -------------------------------------------------------------------------
    # Load patient_id → nd_patient_id map
    # -------------------------------------------------------------------------
    patient_map = load_patient_map(mapping_schema)
    logger.info("Loaded %d patient_id → nd_patient_id mappings", len(patient_map))

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
    # Build combined source data keyed by patient_id (one row per patient — last
    # row wins), then resolve nd_patient_id.
    # -------------------------------------------------------------------------
    combined_source_data: dict[int, dict] = {}
    for row in source_rows:
        record = dict(zip(SOURCE_COLUMNS, row))
        combined_source_data[record["patient_id"]] = record

    data_for_upsert = []
    unmapped = 0
    for patient_id, record in combined_source_data.items():
        nd_patient_id = patient_map.get(patient_id)
        if nd_patient_id is None:
            unmapped += 1
            continue
        record = dict(record)
        record.pop("patient_id", None)
        data_for_upsert.append({"nd_patient_id": nd_patient_id, "patientid": patient_id, **record})

    logger.info(
        "Combined source patients: %d, resolved: %d, unmapped (skipped): %d",
        len(combined_source_data),
        len(data_for_upsert),
        unmapped,
    )
    if unmapped:
        logger.warning(
            "%d source patients had no nd_patient_id mapping and were skipped. "
            "Run mapping_delta before master_pii_delta so every patient is mapped.",
            unmapped,
        )

    # -------------------------------------------------------------------------
    # Upsert into pii_data_table (keyed on uq_nd_patient_id)
    # -------------------------------------------------------------------------
    if data_for_upsert:
        value_cols = ["patientid"] + PII_VALUE_COLUMNS
        all_cols = ["nd_patient_id"] + value_cols
        cols = ", ".join(f"`{k}`" for k in all_cols)
        params = ", ".join(f":{k}" for k in all_cols)
        updates = ", ".join(
            f"`{k}` = COALESCE(VALUES(`{k}`), `{k}`)" for k in value_cols
        )

        upsert_query = text(
            f"""
        INSERT INTO pii_data_table ({cols})
        VALUES ({params})
        ON DUPLICATE KEY UPDATE {updates}
        """
        )

        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(upsert_query, data_for_upsert)

        logger.info("Upserted %d PII records", len(data_for_upsert))
    else:
        logger.info("No PII records to upsert")

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
        help="Master schema name (e.g. 'master_pg')",
    )
    parser.add_argument(
        "--staging_schema",
        required=True,
        help="Staging schema name (e.g. 'Primerecord_diff_staging')",
    )
    parser.add_argument(
        "--mapping_schema",
        required=True,
        help="Mapping schema name for nd_patient_id resolution (e.g. 'mapping_pg')",
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
        mapping_schema=args.mapping_schema,
        state_file=args.state_file,
    )


if __name__ == "__main__":
    main()
