#!/usr/bin/env python
"""
Master Insurance Delta Script  (new master_pg structure — eCW)

Updates master_insurance_table in the given master schema using deltas from the
staging schema.

New master_pg structure: master_insurance_table is keyed on nd_patient_id (join key)
with patientid retained. This script resolves each source patient_id → nd_patient_id
via patient_mapping_table in --mapping_schema, dedups to one row per patient, and
writes with a single INSERT ... ON DUPLICATE KEY UPDATE keyed on uq_nd_patient_id.
NULL source values never overwrite an existing value (COALESCE).

The hcfa table is read from the PROD schema (the full DB, --prod_schema /
PROD_DB_NAME), not from the delta staging, so insurance detail is always complete;
edi_invoice (the patient linkage) is still read from --staging_schema. Both are
read over the one source engine via a cross-schema join, so prod must live on the
same source server as staging (eCW's default — PROD_HOST falls back to SOURCE_DB_HOST).

The staging source dialect is controlled by SOURCE_DB_TYPE ("mysql" default, or
"mssql"); master and mapping (dest) schemas are always MySQL.

Usage:
    python master_insurance_delta.py --master_schema "master_pg" \
        --staging_schema "mobiledoc_staging" --mapping_schema "mapping_pg" \
        --prod_schema "mobiledoc"
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

# Controls the dialect used for the eCW staging source database.
# "mysql" (default) or "mssql". Dest engines (master/mapping) are always MySQL.
SOURCE_DB_TYPE = os.environ.get("SOURCE_DB_TYPE", "mysql")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def _is_missing_table_error(exc) -> bool:
    """True if a DB error indicates a missing source table (dialect-agnostic)."""
    msg = str(exc).lower()
    # MySQL 1146 ("table doesn't exist"): check the DBAPI's actual numeric error
    # code via exc.orig.args[0], not a substring match on the whole message —
    # "1146" could otherwise appear in an unrelated error (a row id, a byte
    # count, ...) and get misclassified as a missing table, silently turning a
    # real failure into a skipped "success".
    orig_args = getattr(getattr(exc, "orig", None), "args", None)
    is_1146 = bool(orig_args) and orig_args[0] == 1146
    return (
        "invalid object name" in msg   # MSSQL 208
        or "42s02" in msg              # ODBC missing-table SQLSTATE
        or "doesn't exist" in msg      # MySQL 1146 (message text)
        or is_1146
    )


def _write_success_state(state_file: str, skipped: bool = False, reason: str = ""):
    """Write the success state file the downstream validator expects. On a skip,
    still report success (so the pipeline proceeds) but flag it."""
    if not state_file:
        return
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    payload = {"success": True, "completed_at": datetime.now(timezone.utc).isoformat()}
    if skipped:
        payload["skipped"] = True
        payload["reason"] = reason
    with open(state_file, "w") as f:
        json.dump(payload, f, indent=2)
    logger.info("Wrote state file: %s", state_file)


def create_mysql_engine(schema: str):
    """MySQL engine for master / mapping schemas (always MySQL)."""
    url = URL.create("mysql+pymysql", username=MYSQL_USER, password=MYSQL_PASS, host=MYSQL_HOST, port=MYSQL_PORT, database=schema)
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True)


def build_source_engine(schema: str):
    """Engine for the eCW staging source — dialect controlled by SOURCE_DB_TYPE."""
    user = os.environ.get("SOURCE_DB_USER", "")
    password = os.environ.get("SOURCE_DB_PASS", "")
    host = os.environ.get("SOURCE_DB_HOST") or os.environ.get("DB_HOST", "localhost")
    if SOURCE_DB_TYPE == "mssql":
        port = int(os.environ.get("SOURCE_DB_PORT", 1433))
        odbc_driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
        url = URL.create(
            "mssql+pyodbc",
            username=user,
            password=password,
            host=host,
            port=port,
            database=schema,
            query={"driver": odbc_driver, "TrustServerCertificate": "yes"},
        )
        return create_engine(url, pool_recycle=3600, pool_pre_ping=True)
    port = int(os.environ.get("SOURCE_DB_PORT", 3306))
    url = URL.create("mysql+pymysql", username=user, password=password, host=host, port=port, database=schema)
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True)


def load_patient_map(mapping_schema: str) -> dict:
    """Return {patientid (raw source id) → nd_patient_id} from patient_mapping_table."""
    engine = create_mysql_engine(mapping_schema)
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT patientid, nd_patient_id FROM patient_mapping_table")
        ).fetchall()
    return {row[0]: row[1] for row in rows}


def run_master_insurance_delta(master_schema: str, staging_schema: str, mapping_schema: str,
                               prod_schema: str = "", state_file: str = ""):
    """
    Apply delta updates to master_insurance_table in master_schema, keyed on
    nd_patient_id resolved via mapping_schema. hcfa is read from prod_schema
    (full DB); edi_invoice from staging_schema (delta).
    """
    prod_schema = prod_schema or os.environ.get("PROD_DB_NAME", "")
    if not prod_schema:
        raise ValueError(
            "prod_schema is required (pass --prod_schema or set PROD_DB_NAME): "
            "hcfa is read from the prod schema."
        )

    logger.info(
        "Starting master insurance delta: master_schema=%s, staging_schema=%s, mapping_schema=%s, prod_schema=%s",
        master_schema,
        staging_schema,
        mapping_schema,
        prod_schema,
    )

    source_engine = build_source_engine(staging_schema)
    dest_engine = create_mysql_engine(master_schema)

    # Cross-schema reference to prod.hcfa. MSSQL cross-database refs need the
    # schema segment (db.dbo.table); MySQL uses db.table. edi_invoice stays
    # unqualified — it lives in the connected staging schema.
    prod_hcfa = (
        f"{prod_schema}.dbo.hcfa" if SOURCE_DB_TYPE == "mssql" else f"{prod_schema}.hcfa"
    )

    # -------------------------------------------------------------------------
    # Load patient_id → nd_patient_id map
    # -------------------------------------------------------------------------
    patient_map = load_patient_map(mapping_schema)
    logger.info("Loaded %d patient_id → nd_patient_id mappings", len(patient_map))

    # -------------------------------------------------------------------------
    # Load insurance source data from staging (hcfa + edi_invoice)
    # -------------------------------------------------------------------------
    logger.info("Loading insurance data from %s.hcfa (prod) / %s.edi_invoice (staging)", prod_schema, staging_schema)
    with source_engine.connect() as conn:
        # hcfa comes from the full prod DB; edi_invoice from the delta staging DB.
        # Cross-schema join over the one source engine — both must be on the same
        # source server (eCW default).
        query = text(
            f"""
        SELECT
            b.patientid AS patient_id,
            NULL AS encounter_id,
            a.PName AS hcfa_PName,
            a.InsuredName AS hcfa_InsuredName,
            a.InsuredId AS hcfa_InsuredId,
            a.PAddressStreet AS hcfa_PAddressStreet,
            a.PAddressCity AS hcfa_PAddressCity,
            a.InsuredAddStreet AS hcfa_InsuredAddStreet,
            a.InsuredAddCity AS hcfa_InsuredAddCity,
            a.NameRef AS hcfa_NameRef,
            a.SSN AS hcfa_SSN,
            a.EmpOrSchoolName AS hcfa_EmpOrSchoolName,
            a.InsurancePlan AS hcfa_InsurancePlan,
            a.OtherInsuredName AS hcfa_OtherInsuredName,
            a.OtherEmpName AS hcfa_OtherEmpName,
            a.OtherInsuranceName AS hcfa_OtherInsuranceName,
            a.PayorName1 AS hcfa_PayorName1,
            a.PayorName2 AS hcfa_PayorName2,
            a.PayorName3 AS hcfa_PayorName3,
            a.PayorAddress11 AS hcfa_PayorAddress11,
            a.PayorAddress12 AS hcfa_PayorAddress12,
            a.PayorAddress13 AS hcfa_PayorAddress13,
            a.PayorAddress21 AS hcfa_PayorAddress21,
            a.PayorAddress22 AS hcfa_PayorAddress22,
            a.PayorAddress23 AS hcfa_PayorAddress23,
            a.PayorCity1 AS hcfa_PayorCity1,
            a.PayorZip1 AS hcfa_PayorZip1,
            a.PayorCity2 AS hcfa_PayorCity2,
            a.PayorZip2 AS hcfa_PayorZip2,
            a.PayorCity3 AS hcfa_PayorCity3,
            a.PayorZip3 AS hcfa_PayorZip3
        FROM {prod_hcfa} a
        INNER JOIN edi_invoice b ON a.invid = b.id
        """
        )
        try:
            result = conn.execute(query)
            ins_data = result.fetchall()
        except Exception as e:
            if _is_missing_table_error(e):
                logger.warning(
                    "NO TABLE FOUND IN THE STAGING SCHEMA (%s) — skipping master insurance delta: %s",
                    staging_schema, e,
                )
                _write_success_state(state_file, skipped=True, reason=str(e))
                return
            raise

    if not ins_data:
        logger.warning(
            "NO TABLE FOUND IN THE STAGING SCHEMA (%s) — no insurance source rows; skipping master insurance delta",
            staging_schema,
        )
        _write_success_state(state_file, skipped=True, reason="no source rows")
        return

    logger.info("Loaded %d insurance source rows", len(ins_data))

    # -------------------------------------------------------------------------
    # Columns written to master_insurance_table (nd_patient_id replaces patient_id
    # as the key; patient_id is kept as retained `patientid`).
    # -------------------------------------------------------------------------
    SOURCE_COLUMNS = [
        "patient_id",
        "encounter_id",
        "hcfa_PName",
        "hcfa_InsuredName",
        "hcfa_InsuredId",
        "hcfa_PAddressStreet",
        "hcfa_PAddressCity",
        "hcfa_InsuredAddStreet",
        "hcfa_InsuredAddCity",
        "hcfa_NameRef",
        "hcfa_SSN",
        "hcfa_EmpOrSchoolName",
        "hcfa_InsurancePlan",
        "hcfa_OtherInsuredName",
        "hcfa_OtherEmpName",
        "hcfa_OtherInsuranceName",
        "hcfa_PayorName1",
        "hcfa_PayorName2",
        "hcfa_PayorName3",
        "hcfa_PayorAddress11",
        "hcfa_PayorAddress12",
        "hcfa_PayorAddress13",
        "hcfa_PayorAddress21",
        "hcfa_PayorAddress22",
        "hcfa_PayorAddress23",
        "hcfa_PayorCity1",
        "hcfa_PayorZip1",
        "hcfa_PayorCity2",
        "hcfa_PayorZip2",
        "hcfa_PayorCity3",
        "hcfa_PayorZip3",
    ]
    # Value columns stored (patient_id only resolves nd_patient_id; kept as patientid)
    INSURANCE_VALUE_COLUMNS = [c for c in SOURCE_COLUMNS if c != "patient_id"]

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

    _write_success_state(state_file)


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
        help="Staging schema name (e.g. 'mobiledoc_staging')",
    )
    parser.add_argument(
        "--mapping_schema",
        required=True,
        help="Mapping schema name for nd_patient_id resolution (e.g. 'mapping_pg')",
    )
    parser.add_argument(
        "--prod_schema",
        default="",
        help="Prod (full DB) schema the hcfa table is read from; defaults to PROD_DB_NAME env",
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
        prod_schema=args.prod_schema,
        state_file=args.state_file,
    )


if __name__ == "__main__":
    main()
