"""
Central configuration.
Loads all settings from Airflow/.env

NOTE: python-dotenv on some versions does NOT expand ${VAR} references within
the same .env file.  All composite values are therefore built here in Python
from the primitive env vars that carry literal values.
"""

import os
import logging
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Load .env at parse time so os.environ.get() calls below work correctly even
# when the Airflow worker process doesn't inherit the shell environment.
# Uses setdefault — does NOT override values already set in the environment.
# ──────────────────────────────────────────────────────────────────────────────
def _load_env_file(path: str) -> None:
    try:
        with open(path) as _f:
            for _line in _f:
                _line = _line.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _key, _, _val = _line.partition("=")
                _key = _key.strip()
                _val = _val.strip().strip('"').strip("'")
                os.environ.setdefault(_key, _val)
    except FileNotFoundError:
        logger.warning("No .env file found at %s", path)


_load_env_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

# ── Primitive env vars — literal values, no ${VAR} expansion needed ─────────────
_SOURCE_USER  = os.environ.get("SOURCE_USER", "")
_SOURCE_PW    = os.environ.get("SOURCE_PW", "")
_DEST_USER    = os.environ.get("DEST_USER", "")
_DEST_PW      = os.environ.get("DEST_PW", "")
_DB_HOST      = os.environ.get("DB_HOST", "localhost")
_PROD_HOST    = os.environ.get("PROD_HOST", os.environ.get("DB_HOST", "localhost"))
_MSSQL_PORT   = os.environ.get("MSSQL_PORT", "1433")
_MYSQL_PORT   = os.environ.get("MYSQL_PORT", "3306")

_BASE_DB_NAME    = os.environ.get("BASE_DB_NAME", "")
_PROD_DB_NAME    = os.environ.get("PROD_DB_NAME", "")
_STAGING_DB_NAME = os.environ.get("STAGING_DB_NAME", "")
_MERGED_DB_NAME  = os.environ.get("MERGED_DB_NAME", "")
_MAPPING_DB_NAME = os.environ.get("MAPPING_DB_NAME", "mapping")
_MASTER_DB_NAME  = os.environ.get("MASTER_DB_NAME", "master")
_DEID_BASE_NAME    = os.environ.get("DEID_BASE_NAME", "deidentified")
_DEID_STAGING_NAME = os.environ.get("DEID_STAGING_NAME", "deidentified_staging")
_DEID_MERGED_NAME  = os.environ.get("DEID_MERGED_NAME", "deidentified_merged")

# ODBC driver name — configurable per machine (installs vary: Driver 17 vs 18).
# Set ODBC_DRIVER in .env to override; spaces are URL-encoded to '+' for the
# pyodbc/SQLAlchemy query string. `odbcinst -q -d` lists installed drivers.
_ODBC_DRIVER = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
_ODBC = f"?driver={_ODBC_DRIVER.replace(' ', '+')}&TrustServerCertificate=yes"

# ── State directory ─────────────────────────────────────────────
STATE_DIR     = os.environ["STATE_DIR"]
DEID_REPO_DIR = os.environ.get("DEID_REPO_DIR", "")

# ── Merge config ────────────────────────────────────────────────
PK_CSV                       = os.environ.get("PK_CSV", "")
HISTORICAL_DATA_DEFAULT_DATE = os.environ.get("HISTORICAL_DATA_DEFAULT_DATE", "")

# ── MSSQL connection strings — built from primitives ───────────────────────────
BASE_DB         = f"mssql+pyodbc://{_SOURCE_USER}:{_SOURCE_PW}@{_DB_HOST}:{_MSSQL_PORT}/{_BASE_DB_NAME}{_ODBC}"
PROD_DB         = f"mssql+pyodbc://{_SOURCE_USER}:{_SOURCE_PW}@{_PROD_HOST}:{_MSSQL_PORT}/{_PROD_DB_NAME}{_ODBC}"
DIFF_STAGING_DB = f"mssql+pyodbc://{_SOURCE_USER}:{_SOURCE_PW}@{_DB_HOST}:{_MSSQL_PORT}/{_STAGING_DB_NAME}{_ODBC}"
DIFFMERGED_CONN = f"mssql+pyodbc://{_SOURCE_USER}:{_SOURCE_PW}@{_DB_HOST}:{_MSSQL_PORT}/{_MERGED_DB_NAME}{_ODBC}"

DB_TYPE     = "mssql"
DIFF_SCHEMA = "dbo"

# ── MySQL connection strings — built from primitives ───────────────────────────
MYSQL_DEID_BASE_CONN       = f"mysql+pymysql://{_DEST_USER}:{_DEST_PW}@{_DB_HOST}:{_MYSQL_PORT}/{_DEID_BASE_NAME}"
MYSQL_DEID_STAGING_CONN    = f"mysql+pymysql://{_DEST_USER}:{_DEST_PW}@{_DB_HOST}:{_MYSQL_PORT}/{_DEID_STAGING_NAME}"
MYSQL_DEID_DIFFMERGED_CONN = f"mysql+pymysql://{_DEST_USER}:{_DEST_PW}@{_DB_HOST}:{_MYSQL_PORT}/{_DEID_MERGED_NAME}"

# ── Deid config ──────────────────────────────────────────────────
DBDETAILSMODEL_NAME = os.environ.get("DBDETAILSMODEL_NAME", "")

# ── Tables CSV (per DAG) ─────────────────────────────────────────
DIFF_MERGE_TABLES_CSV        = os.environ.get("DIFF_MERGE_TABLES_CSV", "")
DEID_TABLES_CSV              = os.environ.get("DEID_TABLES_CSV", "")
DEID_POST_PROCESS_TABLES_CSV = os.environ.get("DEID_POST_PROCESS_TABLES_CSV", "")

# ── Mapping dict (mssql_incremental_mapping_update*.py) ─────────────────────────
MAPPING_MSSQL_CONN = {
    "server":   _PROD_HOST,
    "port":     int(_MSSQL_PORT),
    "database": _PROD_DB_NAME,
    "user":     _SOURCE_USER,
    "password": _SOURCE_PW,
}
MAPPING_MYSQL_CONN = {
    "host":     _DB_HOST,
    "database": _MAPPING_DB_NAME,
    "user":     _DEST_USER,
    "password": _DEST_PW,
}

# ── Master recreate connection strings (mssql_incremental_master_recreate.py) ───
MASTER_MSSQL_CONN_STR = f"mssql+pyodbc://{_SOURCE_USER}:{_SOURCE_PW}@{_PROD_HOST}:{_MSSQL_PORT}/{_PROD_DB_NAME}{_ODBC}"
MASTER_MYSQL_CONN_STR = f"mysql+pymysql://{_DEST_USER}:{_DEST_PW}@{_DB_HOST}:{_MYSQL_PORT}/{_MASTER_DB_NAME}"

# ── S3 transfer — optional, only populated when S3 transfer DAG is configured ─
S3_TRANSFER_MYSQL_HOST     = os.environ.get("S3_TRANSFER_MYSQL_HOST", "")
S3_TRANSFER_MYSQL_PORT     = os.environ.get("S3_TRANSFER_MYSQL_PORT", "3306")
S3_TRANSFER_MYSQL_USER     = os.environ.get("S3_TRANSFER_MYSQL_USER", "")
S3_TRANSFER_MYSQL_PASSWORD = os.environ.get("S3_TRANSFER_MYSQL_PASSWORD", "")
S3_TRANSFER_MYSQL_SCHEMA   = os.environ.get("S3_TRANSFER_MYSQL_SCHEMA", "")
S3_TRANSFER_MYSQLDUMP_PATH = os.environ.get("S3_TRANSFER_MYSQLDUMP_PATH", "mysqldump")
S3_TRANSFER_MYSQL_PATH     = os.environ.get("S3_TRANSFER_MYSQL_PATH", "mysql")
S3_TRANSFER_BUCKET         = os.environ.get("S3_TRANSFER_BUCKET", "")
S3_TRANSFER_PREFIX         = os.environ.get("S3_TRANSFER_PREFIX", "")
S3_TRANSFER_OUTPUT_BASE    = os.environ.get("S3_TRANSFER_OUTPUT_BASE", "")


MAPPING_SQL_FILE=''
MASTER_PII_SQL_FILE=''
MASTER_INSURANCE_SQL_FILE=''


# ── LEGACY COMPAT — athena / veradigm clients on the v3/v4/v5 engine ──────────
# Role-based connection constants required by the legacy mapping/master scripts
# (incremental_mapping_update_v3/v4/v5.py, master_recreate_v3.py) that predate
# mind's engine-based (MYSQL/MSSQL) connection split. Read lazily via
# os.environ.get so db_config stays importable for the new EHR pipelines (eCW /
# Greenway / MIND), which do not set these vars. Remove once these clients are
# re-ported onto the delta-based architecture.
MAPPING_SOURCE_CONN = {
    "host":     os.environ.get("MAPPING_SOURCE_HOST", ""),
    "port":     int(os.environ.get("MAPPING_SOURCE_PORT", "1433")),
    "database": os.environ.get("MAPPING_SOURCE_DATABASE", ""),
    "user":     os.environ.get("MAPPING_SOURCE_USER", ""),
    "password": os.environ.get("MAPPING_SOURCE_PASSWORD", ""),
}
MAPPING_DEST_CONN = {
    "host":     os.environ.get("MAPPING_DEST_HOST", ""),
    "database": os.environ.get("MAPPING_DEST_DATABASE", ""),
    "user":     os.environ.get("MAPPING_DEST_USER", ""),
    "password": os.environ.get("MAPPING_DEST_PASSWORD", ""),
}
MAPPING_CONFIG_FILE     = os.environ.get("MAPPING_CONFIG_FILE", "")
MASTER_SOURCE_CONN_STR  = os.environ.get("MASTER_SOURCE_CONN", "")
MASTER_DEST_CONN_STR    = os.environ.get("MASTER_DEST_CONN", "")
MASTER_MAPPING_CONN_STR = os.environ.get("MASTER_MAPPING_CONN", "")

# Legacy AWS/S3 transfer DAG (dags/legacy/aws_transfer.py), superseded by
# s3_transfer.py + s3_transfer_dag.py. Lazy reads keep it parse-clean.
AWS_TRANSFER_MYSQL_HOST         = os.environ.get("AWS_TRANSFER_MYSQL_HOST", "")
AWS_TRANSFER_MYSQL_PORT         = os.environ.get("AWS_TRANSFER_MYSQL_PORT", "3306")
AWS_TRANSFER_MYSQL_USER         = os.environ.get("AWS_TRANSFER_MYSQL_USER", "")
AWS_TRANSFER_MYSQL_PASSWORD     = os.environ.get("AWS_TRANSFER_MYSQL_PASSWORD", "")
AWS_TRANSFER_MYSQL_SCHEMA       = os.environ.get("AWS_TRANSFER_MYSQL_SCHEMA", "")
AWS_TRANSFER_MYSQLDUMP_PATH     = os.environ.get("AWS_TRANSFER_MYSQLDUMP_PATH", "mysqldump")
AWS_TRANSFER_MYSQL_PATH         = os.environ.get("AWS_TRANSFER_MYSQL_PATH", "mysql")
AWS_TRANSFER_S3_BUCKET          = os.environ.get("AWS_TRANSFER_S3_BUCKET", "")
AWS_TRANSFER_S3_PREFIX          = os.environ.get("AWS_TRANSFER_S3_PREFIX", "")
AWS_TRANSFER_OUTPUT_BASE        = os.environ.get("AWS_TRANSFER_OUTPUT_BASE", "")
AWS_TRANSFER_COUNT_SCHEMA       = os.environ.get("AWS_TRANSFER_COUNT_SCHEMA", "")
AWS_TRANSFER_COUNT_REPORT_LABEL = os.environ.get("AWS_TRANSFER_COUNT_REPORT_LABEL", "")
AWS_TRANSFER_COUNT_S3_PREFIX    = os.environ.get("AWS_TRANSFER_COUNT_S3_PREFIX", "")