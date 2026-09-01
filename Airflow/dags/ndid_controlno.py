"""
Airflow DAG: ndid_controlno_pipeline

Flow:
  1. Read nd_patient_id list from a CSV.
  2. SOURCE DB    -> fetch patientid from the mapping table for those nd_patient_id.
  3. CONTROLNO DB -> fetch ControlNo from the patients table for those patientid.
  4. TARGET DB    -> create the output table if missing, then APPEND the rows
                    (nd_patient_id, patientid, ControlNo, date_of_extraction).

Only the nd_patient_id values present in the CSV are processed.

Connections:
  Built as SQLAlchemy engines from separate host/port/user/pass params in your
  .env. No Airflow connections/hooks needed.
    - mapping + output (MySQL): DB_HOST / DB_PORT / DB_USER / DB_PASS
    - patients (MSSQL):         SOURCE_DB_HOST / SOURCE_DB_PORT /
                                SOURCE_DB_USER / SOURCE_DB_PASS
                                (database name from PROD_DB_NAME)

  Each of the three DB steps can be pointed at either server independently —
  set SOURCE_DB / CONTROLNO_DB / TARGET_DB below to "mapping_mysql" or
  "patients_mssql". The target table is created with dialect-aware DDL, so
  the output can live on either DB.

  NOTE: when you point a step at a different DB, its table/column names must
  match that DB. e.g. "mapping_pg.table" is a MySQL database qualifier; on
  MSSQL the prefix is a schema (like "dbo.table").

------------------------------------------------------------------------------
BEFORE RUNNING — verify (marked CHECK): table/column names and the CSV header.
------------------------------------------------------------------------------
"""

from __future__ import annotations

import csv
import os
from datetime import date
from pathlib import Path
from urllib.parse import quote

import pendulum
from airflow.sdk import dag, task
from sqlalchemy import (
    Column,
    Date,
    MetaData,
    String,
    Table,
    bindparam,
    create_engine,
    insert,
    text,
)

# ----------------------------------------------------------------------------
# Load the repo .env so the DB_* / SOURCE_DB_* / PROD_DB_NAME / NDID_CSV_PATH
# variables are available.
# The .env is located by walking UP from this file until one is found, so it
# works no matter which sub-folder of dags/ the DAG is placed in. Uses
# python-dotenv if present, otherwise a minimal built-in parser. Existing
# environment variables always win (never overridden).
# ----------------------------------------------------------------------------
def _load_env_file():
    start = Path(__file__).resolve().parent
    env_file = next(
        (d / ".env" for d in [start, *start.parents] if (d / ".env").is_file()),
        None,
    )
    if env_file is None:
        return None
    try:
        from dotenv import load_dotenv

        load_dotenv(env_file, override=False)
    except ImportError:
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))
    return env_file


ENV_PATH = _load_env_file()

# ----------------------------------------------------------------------------
# CONFIG  (CHECK all of these)
# ----------------------------------------------------------------------------
# MySQL server holding mapping_pg.patient_mapping_table, given as separate
# params in the .env (DB_HOST / DB_PORT / DB_USER / DB_PASS) rather than a URL.
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# MSSQL server holding the patients table, given as separate params in the
# .env (SOURCE_DB_HOST / SOURCE_DB_PORT / SOURCE_DB_USER / SOURCE_DB_PASS).
# SOURCE_DB_HOST falls back to DB_HOST when blank, matching the repo .env
# convention (the source is usually local).
SOURCE_DB_HOST = os.getenv("SOURCE_DB_HOST") or os.getenv("DB_HOST")
SOURCE_DB_PORT = os.getenv("SOURCE_DB_PORT", "1433")
SOURCE_DB_USER = os.getenv("SOURCE_DB_USER")
SOURCE_DB_PASS = os.getenv("SOURCE_DB_PASS")

# Which DB each step talks to. Set independently to "mapping_mysql" (the DB_*
# MySQL server above) or "patients_mssql" (the SOURCE_DB_* MSSQL server above).
# Defaults: mapping on the DB_* MySQL server, ControlNo on the SOURCE_DB_*
# MSSQL server, output written back to the same DB_* MySQL server.
SOURCE_DB    = "mapping_mysql"    # where the mapping table (nd_patient_id -> patientid) lives
CONTROLNO_DB = "patients_mssql"   # where the patients table (patientid -> ControlNo) lives
TARGET_DB    = "mapping_mysql"    # where the ndid_controlno output table is written

# Source: mapping table (nd_patient_id -> patientid)
MAPPING_TABLE      = "mapping_pg.patient_mapping_table"
MAP_NDID_COL       = "nd_patient_id"        # CHECK column name in mapping table
MAP_PATIENTID_COL  = "patientid"            # CHECK column name in mapping table

# Source: patients table (patientid -> ControlNo)
# The database is taken from PROD_DB_NAME in the .env; the query targets
# {PROD_DB_NAME}.dbo.patients (MSSQL three-part name).
PROD_DB_NAME           = os.getenv("PROD_DB_NAME")
PATIENTS_TABLE         = "dbo.patients"     # CHECK schema/table (db prefix from PROD_DB_NAME)
PATIENTS_PID_COL       = "pid"              # patients.pid  ==  mapping table's patientid
PATIENTS_CONTROLNO_COL = "ControlNo"        # CHECK column name

# Target: output table
TARGET_TABLE = "mapping_pg.ndid_controlno"

# Input CSV. Header is expected to be the column name below.
# Path resolution order (first hit wins), applied at task runtime:
#   1. per-run conf:  {"csv_path": "/path/to/file.csv"}
#   2. .env var NDID_CSV_PATH
#   3. fallback: a file next to this DAG
FALLBACK_CSV_PATH = str(Path(__file__).resolve().parent / "nd_patient_ids.csv")
CSV_ID_COLUMN     = "nd_patient_id"

# Chunk size for building IN (...) clauses so we never blow the query size limit.
CHUNK = 1000


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def _mapping_mysql_url() -> str:
    """Build a MySQL URL from the DB_HOST / DB_PORT / DB_USER / DB_PASS params."""
    missing = [
        name
        for name, val in (("DB_HOST", DB_HOST), ("DB_USER", DB_USER), ("DB_PASS", DB_PASS))
        if not val
    ]
    if missing:
        raise ValueError(
            f"{', '.join(missing)} not set. Add them to {ENV_PATH} or export them "
            f"in the environment Airflow runs in."
        )
    user = quote(DB_USER, safe="")
    pwd = quote(DB_PASS, safe="")
    return f"mysql+pymysql://{user}:{pwd}@{DB_HOST}:{DB_PORT}"


def _patients_mssql_url() -> str:
    """Build an MSSQL URL from the SOURCE_DB_* params in the .env."""
    missing = [
        name
        for name, val in (
            ("SOURCE_DB_HOST (or DB_HOST)", SOURCE_DB_HOST),
            ("SOURCE_DB_USER", SOURCE_DB_USER),
            ("SOURCE_DB_PASS", SOURCE_DB_PASS),
        )
        if not val
    ]
    if missing:
        raise ValueError(
            f"{', '.join(missing)} not set. Add them to {ENV_PATH} or export them "
            f"in the environment Airflow runs in."
        )
    user = quote(SOURCE_DB_USER, safe="")
    pwd = quote(SOURCE_DB_PASS, safe="")
    return f"mssql+pymssql://{user}:{pwd}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}"


def _engine_for(which: str):
    """Return an engine for 'mapping_mysql' or 'patients_mssql'."""
    which = which.lower()
    if which == "mapping_mysql":
        return create_engine(_mapping_mysql_url(), pool_pre_ping=True)
    if which == "patients_mssql":
        return create_engine(_patients_mssql_url(), pool_pre_ping=True)
    raise ValueError(
        f"Unknown DB selector {which!r}; use 'mapping_mysql' or 'patients_mssql'."
    )


def _chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


@dag(
    dag_id="ndid_controlno_pipeline",
    schedule=None,                       # trigger manually; set e.g. "@daily" if needed
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["mapping_pg", "mysql", "mssql"],
)
def ndid_controlno_pipeline():

    # -- 1. Read nd_patient_id values from the CSV --------------------------
    @task
    def read_nd_patient_ids(**context) -> list[str]:
        conf = context["dag_run"].conf or {}
        csv_path = conf.get("csv_path") or os.getenv("NDID_CSV_PATH") or FALLBACK_CSV_PATH

        path = Path(csv_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV not found: {csv_path}")

        ids: list[str] = []
        with path.open(newline="") as fh:
            reader = csv.DictReader(fh)
            if CSV_ID_COLUMN not in (reader.fieldnames or []):
                raise ValueError(
                    f"CSV must have a '{CSV_ID_COLUMN}' header. Found: {reader.fieldnames}"
                )
            for row in reader:
                val = (row.get(CSV_ID_COLUMN) or "").strip()
                if val:
                    ids.append(val)

        ids = list(dict.fromkeys(ids))  # de-dup, preserve order
        if not ids:
            raise ValueError("No nd_patient_id values found in the CSV.")
        print(f"Read {len(ids)} nd_patient_id(s) from {csv_path}")
        return ids

    # -- 2. SOURCE DB: nd_patient_id -> patientid --------------------------
    @task
    def fetch_patientids(nd_ids: list[str]) -> list[dict]:
        engine = _engine_for(SOURCE_DB)
        rows: list[dict] = []
        try:
            with engine.connect() as conn:
                for chunk in _chunks(nd_ids, CHUNK):
                    stmt = text(
                        f"SELECT {MAP_NDID_COL}, {MAP_PATIENTID_COL} "
                        f"FROM {MAPPING_TABLE} "
                        f"WHERE {MAP_NDID_COL} IN :ids"
                    ).bindparams(bindparam("ids", expanding=True))
                    for nd_id, patientid in conn.execute(stmt, {"ids": list(chunk)}):
                        rows.append(
                            {"nd_patient_id": str(nd_id), "patientid": str(patientid)}
                        )
        finally:
            engine.dispose()

        found = {r["nd_patient_id"] for r in rows}
        missing = [x for x in nd_ids if x not in found]
        if missing:
            print(f"WARNING: no patientid mapping for {len(missing)} nd_patient_id(s): "
                  f"{missing[:20]}{' ...' if len(missing) > 20 else ''}")
        print(f"Resolved {len(rows)} patientid(s).")
        return rows

    # -- 3. CONTROLNO DB: patientid -> ControlNo ---------------------------
    @task
    def fetch_control_nos(mapping_rows: list[dict]) -> list[dict]:
        if not mapping_rows:
            return []

        # patientid -> list of nd_patient_id (handles a patientid shared by >1 nd_id)
        pid_to_ndids: dict[str, list[str]] = {}
        for r in mapping_rows:
            pid_to_ndids.setdefault(r["patientid"], []).append(r["nd_patient_id"])

        if not PROD_DB_NAME:
            raise ValueError(
                f"PROD_DB_NAME is not set. Add it to {ENV_PATH} or export it in "
                f"the environment Airflow runs in."
            )
        patients_table = f"{PROD_DB_NAME}.{PATIENTS_TABLE}"

        patient_ids = list(pid_to_ndids.keys())
        engine = _engine_for(CONTROLNO_DB)
        pid_to_control: dict[str, str | None] = {}
        try:
            with engine.connect() as conn:
                for chunk in _chunks(patient_ids, CHUNK):
                    stmt = text(
                        f"SELECT {PATIENTS_PID_COL}, {PATIENTS_CONTROLNO_COL} "
                        f"FROM {patients_table} "
                        f"WHERE {PATIENTS_PID_COL} IN :ids"
                    ).bindparams(bindparam("ids", expanding=True))
                    for pid, control_no in conn.execute(stmt, {"ids": list(chunk)}):
                        pid_to_control[str(pid)] = (
                            None if control_no is None else str(control_no)
                        )
        finally:
            engine.dispose()

        result: list[dict] = []
        for pid, nd_ids in pid_to_ndids.items():
            control_no = pid_to_control.get(pid)  # None if patients row missing
            for nd_id in nd_ids:
                result.append(
                    {"nd_patient_id": nd_id, "patientid": pid, "ControlNo": control_no}
                )

        no_ctrl = {r["patientid"] for r in result if r["ControlNo"] is None}
        if no_ctrl:
            print(f"WARNING: no ControlNo for {len(no_ctrl)} patientid(s).")
        print(f"Built {len(result)} final row(s).")
        return result

    # -- 4. TARGET DB: create-if-missing + append --------------------------
    @task
    def load_to_target(final_rows: list[dict], **context) -> int:
        if not final_rows:
            print("Nothing to load.")
            return 0

        # "current date" = this run's logical date (idempotent for backfills).
        doe = date.fromisoformat(context["ds"])

        # Define the target table with portable types so the CREATE works on
        # either MySQL or MSSQL. schema.table is split for SQLAlchemy.
        if "." in TARGET_TABLE:
            schema, tbl = TARGET_TABLE.split(".", 1)
        else:
            schema, tbl = None, TARGET_TABLE
        metadata = MetaData()
        target = Table(
            tbl,
            metadata,
            Column("nd_patient_id", String(50)),
            Column("patientid", String(50)),
            Column("ControlNo", String(50)),
            Column("date_of_extraction", Date),
            schema=schema,
        )

        engine = _engine_for(TARGET_DB)
        try:
            # checkfirst=True -> only creates when missing, using the right dialect DDL
            metadata.create_all(engine, checkfirst=True)
            with engine.begin() as conn:  # transaction: commits on success
                payload = [
                    {
                        "nd_patient_id": r["nd_patient_id"],
                        "patientid": r["patientid"],
                        "ControlNo": r["ControlNo"],
                        "date_of_extraction": doe,
                    }
                    for r in final_rows
                ]
                conn.execute(insert(target), payload)  # executemany, appends
        finally:
            engine.dispose()

        print(f"Appended {len(final_rows)} row(s) into {TARGET_TABLE} "
              f"({TARGET_DB}) with date_of_extraction={doe}")
        return len(final_rows)

    # -- wiring ------------------------------------------------------------
    nd_ids       = read_nd_patient_ids()
    mapping_rows = fetch_patientids(nd_ids)
    final_rows   = fetch_control_nos(mapping_rows)
    load_to_target(final_rows)


dag = ndid_controlno_pipeline()
