#!/usr/bin/env python
"""
Master Insurance Delta Script  (new master_pg structure — Greenway / PrimeSUITE)

Updates master_insurance_table in the given master schema using deltas from the
staging schema.

New master_pg structure: master_insurance_table is MULTI-ROW per patient (primary +
secondary coverages), keyed for lookup on nd_patient_id (plain index, not unique),
with patientid retained. This script resolves each source patient_id → nd_patient_id
via patient_mapping_table in --mapping_schema, then refreshes each staged patient:
DELETE their existing rows, then INSERT the fresh set (one transaction).

Staging is MSSQL (Greenway/PrimeSUITE); master and mapping (dest) schemas are MySQL.

Usage:
    python master_insurance_delta.py --master_schema "master_pg" \
        --staging_schema "Primerecord_diff_staging" --mapping_schema "mapping_pg"
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


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
    """MySQL engine for master / mapping schemas (always MySQL)."""
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
    """MSSQL engine for the Greenway/PrimeSUITE staging source."""
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


def load_patient_map(mapping_schema: str) -> dict:
    """Return {patientid (raw source id) → nd_patient_id} from patient_mapping_table."""
    engine = create_mysql_engine(mapping_schema)
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT patientid, nd_patient_id FROM patient_mapping_table")
        ).fetchall()
    return {row[0]: row[1] for row in rows}


# Columns returned by the source SELECT (patient_id first — resolves nd_patient_id
# and is kept as `patientid`; everything after is a value column).
SOURCE_COLUMNS = [
    "patient_id",
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
    "InsuranceProviders_EligibilityProviderIDNumber",
]
INSURANCE_VALUE_COLUMNS = [c for c in SOURCE_COLUMNS if c != "patient_id"]


def run_master_insurance_delta(master_schema: str, staging_schema: str, mapping_schema: str, state_file: str = ""):
    """
    Apply delta updates to master_insurance_table in master_schema from staging_schema,
    keyed on nd_patient_id resolved via mapping_schema (multi-row per patient).
    """
    logger.info(
        "Starting master insurance delta: master_schema=%s, staging_schema=%s, mapping_schema=%s",
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
    # Load insurance source data from staging (Patient + Insurance* tables)
    # -------------------------------------------------------------------------
    logger.info(
        "Loading insurance data from Patient/InsuranceCoverage/InsurancePlan/InsuranceProviders in %s",
        staging_schema,
    )
    with source_engine.connect() as conn:
        query = text(
            """
        select a.PatientID as patient_id,
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
    # Resolve nd_patient_id, keep ALL rows (insurance is multi-row per patient)
    # -------------------------------------------------------------------------
    rows_to_insert = []
    nd_patient_ids: set[int] = set()
    unmapped = 0
    for row in ins_data:
        source_record = dict(zip(SOURCE_COLUMNS, row))
        patient_id = source_record.pop("patient_id")
        nd_patient_id = patient_map.get(patient_id)
        if nd_patient_id is None:
            unmapped += 1
            continue
        rows_to_insert.append(
            {"nd_patient_id": nd_patient_id, "patientid": patient_id, **source_record}
        )
        nd_patient_ids.add(nd_patient_id)

    logger.info(
        "Insurance source rows: %d, resolved rows: %d, distinct patients: %d, unmapped (skipped): %d",
        len(ins_data),
        len(rows_to_insert),
        len(nd_patient_ids),
        unmapped,
    )
    if unmapped:
        logger.warning(
            "%d insurance rows had no nd_patient_id mapping and were skipped. "
            "Run mapping_delta before master_insurance_delta so every patient is mapped.",
            unmapped,
        )

    # -------------------------------------------------------------------------
    # Refresh into master_insurance_table (multi-row): for every patient present
    # in this delta, DELETE their existing rows then INSERT the fresh set. Patients
    # not in the delta are untouched. One transaction.
    # -------------------------------------------------------------------------
    if rows_to_insert:
        all_cols = ["nd_patient_id", "patientid"] + INSURANCE_VALUE_COLUMNS
        cols = ", ".join(f"`{k}`" for k in all_cols)
        params = ", ".join(f":{k}" for k in all_cols)
        insert_query = text(f"INSERT INTO master_insurance_table ({cols}) VALUES ({params})")

        ids = list(nd_patient_ids)
        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            for i in range(0, len(ids), 1000):
                chunk = ids[i:i + 1000]
                placeholders = ", ".join(f":p{j}" for j in range(len(chunk)))
                conn.execute(
                    text(f"DELETE FROM master_insurance_table WHERE nd_patient_id IN ({placeholders})"),
                    {f"p{j}": v for j, v in enumerate(chunk)},
                )
            conn.execute(insert_query, rows_to_insert)

        logger.info(
            "Refreshed insurance for %d patients (%d rows)", len(ids), len(rows_to_insert)
        )
    else:
        logger.info("No insurance records to write")

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

    run_master_insurance_delta(
        master_schema=args.master_schema,
        staging_schema=args.staging_schema,
        mapping_schema=args.mapping_schema,
        state_file=args.state_file,
    )


if __name__ == "__main__":
    main()
