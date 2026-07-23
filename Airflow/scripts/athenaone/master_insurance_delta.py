#!/usr/bin/env python
"""
Master Insurance Delta Script — AthenaOne (master_pg structure)

True-delta replacement for master_recreate_v3_athenaone.py's full rebuild of
master_insurance_table. Reads only the incremental Snowflake pull
(--staging_schema), resolves each source patientid → nd_patient_id via
mapping_pg.patient_mapping_table, and refreshes only those patients.

Insurance is multi-row per patient (several policies), so the refresh is
per-patient DELETE-then-INSERT: for every nd_patient_id in this delta, drop its
existing rows and insert the fresh set. Patients not in the delta are untouched.

Arg signature matches the shared make_master_insurance factory task.

Usage:
    python master_insurance_delta.py --master_schema master_pg \
        --staging_schema <incremental_schema> --mapping_schema mapping_pg \
        [--state_file /path/state.json]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

_DB_USER = os.environ.get("DB_USER", "")
_DB_PASS = os.environ.get("DB_PASS", "")
_DB_HOST = os.environ.get("DB_HOST", "localhost")
_DB_PORT = int(os.environ.get("DB_PORT", "3306"))

_SQL_FILE = "master_insurance_data.sql"
_TARGET_TABLE = "master_insurance_table"
_JOIN_COL = "patientid"


def create_mysql_engine(schema: str):
    """Engine for the master / mapping schemas (local MySQL, DB_* family)."""
    url = URL.create("mysql+pymysql", username=_DB_USER, password=_DB_PASS, host=_DB_HOST, port=_DB_PORT, database=schema)
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True)


def build_source_engine(schema: str):
    """Engine for the ingested AthenaOne MySQL (ATHENAONE_MYSQL_*, fallback DB_*)."""
    user = os.environ.get("ATHENAONE_MYSQL_USER") or _DB_USER
    password = os.environ.get("ATHENAONE_MYSQL_PASSWORD") or _DB_PASS
    host = os.environ.get("ATHENAONE_MYSQL_HOST") or _DB_HOST
    url = URL.create("mysql+pymysql", username=user, password=password, host=host, port=3306, database=schema)
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True)


def load_patient_map(mapping_schema: str) -> dict:
    """Return {patientid → nd_patient_id} from patient_mapping_table."""
    engine = create_mysql_engine(mapping_schema)
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT patientid, nd_patient_id FROM patient_mapping_table")
        ).fetchall()
    return {row[0]: row[1] for row in rows}


def _load_sql() -> str:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql", _SQL_FILE)
    return Path(path).read_text()


def _ensure_table(dest_engine, value_cols: list[str]):
    cols_ddl = ",\n  ".join(f"`{c}` LONGTEXT" for c in value_cols)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{_TARGET_TABLE}` (
      nd_patient_id BIGINT NOT NULL,
      `{_JOIN_COL}` BIGINT,
      {cols_ddl},
      INDEX idx_nd_patient_id (nd_patient_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """
    with dest_engine.begin() as conn:
        conn.execute(text(ddl))


def run_master_insurance_delta(master_schema: str, staging_schema: str, mapping_schema: str, state_file: str = ""):
    logger.info(
        "Starting AthenaOne master insurance delta: master=%s, staging=%s, mapping=%s",
        master_schema, staging_schema, mapping_schema,
    )
    source_engine = build_source_engine(staging_schema)
    dest_engine = create_mysql_engine(master_schema)

    patient_map = load_patient_map(mapping_schema)
    logger.info("Loaded %d patientid → nd_patient_id mappings", len(patient_map))

    with source_engine.connect() as conn:
        result = conn.execute(text(_load_sql()))
        cols = list(result.keys())
        src_rows = result.fetchall()
    logger.info("Loaded %d source insurance rows from %s", len(src_rows), staging_schema)

    if _JOIN_COL not in cols:
        raise ValueError(f"Source SQL must return '{_JOIN_COL}'. Got columns: {cols}")
    value_cols = [c for c in cols if c != _JOIN_COL]

    rows_to_insert = []
    nd_ids: set = set()
    unmapped = 0
    for row in src_rows:
        rec = dict(zip(cols, row))
        pid = rec.pop(_JOIN_COL)
        nd = patient_map.get(pid)
        if nd is None:
            unmapped += 1
            continue
        rows_to_insert.append({"nd_patient_id": nd, _JOIN_COL: pid, **rec})
        nd_ids.add(nd)

    logger.info(
        "Source rows: %d, resolved: %d, distinct patients: %d, unmapped (skipped): %d",
        len(src_rows), len(rows_to_insert), len(nd_ids), unmapped,
    )
    if unmapped:
        logger.warning(
            "%d insurance rows had no nd_patient_id mapping and were skipped. "
            "Run mapping before master so every patient is mapped.", unmapped,
        )

    _ensure_table(dest_engine, value_cols)

    if rows_to_insert:
        all_cols = ["nd_patient_id", _JOIN_COL] + value_cols
        cols_sql = ", ".join(f"`{k}`" for k in all_cols)
        params_sql = ", ".join(f":{k}" for k in all_cols)
        insert_query = text(f"INSERT INTO `{_TARGET_TABLE}` ({cols_sql}) VALUES ({params_sql})")

        ids = list(nd_ids)
        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            for i in range(0, len(ids), 1000):
                chunk = ids[i:i + 1000]
                placeholders = ", ".join(f":p{j}" for j in range(len(chunk)))
                conn.execute(
                    text(f"DELETE FROM `{_TARGET_TABLE}` WHERE nd_patient_id IN ({placeholders})"),
                    {f"p{j}": v for j, v in enumerate(chunk)},
                )
            conn.execute(insert_query, rows_to_insert)
        logger.info("Refreshed insurance for %d patients (%d rows)", len(ids), len(rows_to_insert))
    else:
        logger.info("No insurance records to write")

    logger.info("AthenaOne master insurance delta complete")

    if state_file:
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, "w") as f:
            json.dump({"success": True, "completed_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        logger.info("Wrote state file: %s", state_file)


def main():
    parser = argparse.ArgumentParser(description="Apply AthenaOne master_insurance_table deltas")
    parser.add_argument("--master_schema", required=True, help="Master schema (e.g. 'master_pg')")
    parser.add_argument("--staging_schema", required=True, help="Ingested/incremental schema (delta source)")
    parser.add_argument("--mapping_schema", required=True, help="Mapping schema for nd_patient_id resolution (e.g. 'mapping_pg')")
    parser.add_argument("--state_file", default="", help="Optional path to write JSON state file on success")
    args = parser.parse_args()

    run_master_insurance_delta(
        master_schema=args.master_schema,
        staging_schema=args.staging_schema,
        mapping_schema=args.mapping_schema,
        state_file=args.state_file,
    )


if __name__ == "__main__":
    main()
