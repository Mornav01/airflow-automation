import os
from pathlib import Path
from urllib.parse import quote as _url_quote
from dotenv import load_dotenv

# Load Airflow/.env (scripts/athenaone/config.py → scripts/ → Airflow/). In the
# DAG the env is already populated by shared/__init__.py; this makes standalone
# `python incremental_mapping_update_v5_athenaone.py` runs pick up the same file.
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env", override=True)


def _bool(key: str, default: bool = False) -> bool:
    val = os.getenv(key)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes")


def _int(key: str, default: int = 0) -> int:
    val = os.getenv(key)
    return int(val) if val is not None else default


def _list(key: str, default: list | None = None) -> list:
    val = os.getenv(key)
    if not val:
        return default or []
    return [v.strip() for v in val.split(",")]








STATE_DIR     = os.environ["STATE_DIR"]


# Source DB — works for both MSSQL and MySQL sources (source_type in YAML picks the driver)
MAPPING_SOURCE_CONN = {
    "host":     os.environ["MAPPING_SOURCE_HOST"],
    "port":     int(os.environ.get("MAPPING_SOURCE_PORT", "1433")),
    "database": os.environ["MAPPING_SOURCE_DATABASE"],
    "user":     os.environ["MAPPING_SOURCE_USER"],
    "password": os.environ["MAPPING_SOURCE_PASSWORD"],
}
# Destination DB — always MySQL
MAPPING_DEST_CONN = {
    "host":     os.environ["MAPPING_DEST_HOST"],
    "database": os.environ["MAPPING_DEST_DATABASE"],
    "user":     os.environ["MAPPING_DEST_USER"],
    "password": os.environ["MAPPING_DEST_PASSWORD"],
}
# Only the mapping DAG uses --config (passed explicitly), so this is optional.
MAPPING_CONFIG_FILE = os.environ.get("MAPPING_CONFIG_FILE", "")

# ── Master recreate — connection strings (legacy v3 recreate only) ──────────
# AthenaOne master is now built by the true-delta scripts (master_pii_delta.py /
# master_insurance_delta.py), which read DB_* / ATHENAONE_MYSQL_* directly and do
# NOT import this module. These remain only for any legacy master_recreate use and
# are optional so importing config.py (for mapping) never requires them.
MASTER_SOURCE_CONN_STR  = os.environ.get("MASTER_SOURCE_CONN", "")    # source DB (mssql+pymssql:// or mysql+pymysql://)
MASTER_DEST_CONN_STR    = os.environ.get("MASTER_DEST_CONN", "")      # destination MySQL (where master tables are written)
MASTER_MAPPING_CONN_STR = os.environ.get("MASTER_MAPPING_CONN", "")   # mapping MySQL (where patient_mapping_table lives)
