"""
Central configuration.
Loads all settings from Airflow/.env

NOTE: python-dotenv on some versions does NOT expand ${VAR} references within
the same .env file.  All composite values are therefore built here in Python
from the primitive env vars that carry literal values.
"""

import os
import re
import logging
from pathlib import Path

from dotenv import load_dotenv

from db_url import make_conn_url, mssql_odbc_query

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
                _val = _val.strip()
                # Quoted value: take the literal contents and ignore anything
                # after the closing quote (e.g. a trailing inline comment).
                if _val[:1] in ("'", '"'):
                    _q = _val[0]
                    _end = _val.find(_q, 1)
                    _val = _val[1:_end] if _end != -1 else _val[1:]
                else:
                    # Unquoted: strip an inline comment — '#' preceded by
                    # whitespace (dotenv semantics). A '#' with no preceding
                    # space (e.g. inside a password/token) is kept.
                    _c = re.search(r"\s#", _val)
                    if _c:
                        _val = _val[:_c.start()]
                    _val = _val.strip()
                os.environ.setdefault(_key, _val)
    except FileNotFoundError:
        logger.warning("No .env file found at %s", path)


_load_env_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

# ── Primitive env vars — literal values, no ${VAR} expansion needed ─────────────
# SOURCE_DB_* = the client source DB (read side). DB_* = the local MySQL working
# DB (write side). SOURCE_DB_HOST falls back to DB_HOST (source is usually local);
# PROD_HOST (remote prod MSSQL for mapping/master) falls back to SOURCE_DB_HOST.
_SOURCE_USER  = os.environ.get("SOURCE_DB_USER", "")
_SOURCE_PW    = os.environ.get("SOURCE_DB_PASS", "")
_DB_USER      = os.environ.get("DB_USER", "")
_DB_PASS      = os.environ.get("DB_PASS", "")
_DB_HOST      = os.environ.get("DB_HOST", "localhost")
_SOURCE_HOST  = os.environ.get("SOURCE_DB_HOST") or _DB_HOST
_SOURCE_PORT  = os.environ.get("SOURCE_DB_PORT", "1433")
_DB_PORT      = os.environ.get("DB_PORT", "3306")
# PROD_* = the remote upstream prod MSSQL (read by the diff/merge + mapping/master
# scripts). Each falls back to its SOURCE_DB_* counterpart when unset, so a
# same-credentials remote needs only PROD_HOST; set PROD_USER/PROD_PASS/PROD_PORT
# only when the remote prod uses DIFFERENT credentials or port than the source DB.
_PROD_HOST    = os.environ.get("PROD_HOST") or _SOURCE_HOST
_PROD_USER    = os.environ.get("PROD_USER") or _SOURCE_USER
_PROD_PW      = os.environ.get("PROD_PASS") or _SOURCE_PW
_PROD_PORT    = os.environ.get("PROD_PORT") or _SOURCE_PORT

_BASE_DB_NAME    = os.environ.get("BASE_DB_NAME", "")
_PROD_DB_NAME    = os.environ.get("PROD_DB_NAME", "")
_STAGING_DB_NAME = os.environ.get("STAGING_DB_NAME", "")
_MERGED_DB_NAME  = os.environ.get("MERGED_DB_NAME", "")
_MAPPING_DB_NAME = os.environ.get("MAPPING_DB_NAME", "mapping")
_MASTER_DB_NAME  = os.environ.get("MASTER_DB_NAME", "master")
_DEID_BASE_NAME    = os.environ.get("DEID_BASE_NAME", "deidentified")
_DEID_STAGING_NAME = os.environ.get("DEID_STAGING_NAME", "deidentified_staging")
_DEID_MERGED_NAME  = os.environ.get("DEID_MERGED_NAME", "deidentified_merged")
_IGNORE_ROWS_DB_NAME = os.environ.get("IGNORE_ROWS_DB_NAME", "")

# ODBC driver name — configurable per machine (installs vary: Driver 17 vs 18).
# Set ODBC_DRIVER in .env to override. `odbcinst -q -d` lists installed drivers.
# Passed WITH spaces to make_conn_url — SQLAlchemy encodes it (do not pre-replace).
_ODBC_DRIVER = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
_ODBC_QUERY = mssql_odbc_query(_ODBC_DRIVER)

# ── State directory ─────────────────────────────────────────────
STATE_DIR     = os.environ["STATE_DIR"]
DEID_REPO_DIR = os.environ.get("DEID_REPO_DIR", "")

# ── Merge config ────────────────────────────────────────────────
PK_CSV                       = os.environ.get("PK_CSV", "")
HISTORICAL_DATA_DEFAULT_DATE = os.environ.get("HISTORICAL_DATA_DEFAULT_DATE", "")

# ── MSSQL connection strings — built from RAW primitives (make_conn_url encodes) ─
BASE_DB         = make_conn_url("mssql+pyodbc", username=_SOURCE_USER, password=_SOURCE_PW, host=_SOURCE_HOST, port=_SOURCE_PORT, database=_BASE_DB_NAME, query=_ODBC_QUERY)
PROD_DB         = make_conn_url("mssql+pyodbc", username=_PROD_USER, password=_PROD_PW, host=_PROD_HOST, port=_PROD_PORT, database=_PROD_DB_NAME, query=_ODBC_QUERY)
DIFF_STAGING_DB = make_conn_url("mssql+pyodbc", username=_SOURCE_USER, password=_SOURCE_PW, host=_SOURCE_HOST, port=_SOURCE_PORT, database=_STAGING_DB_NAME, query=_ODBC_QUERY)
DIFFMERGED_CONN = make_conn_url("mssql+pyodbc", username=_SOURCE_USER, password=_SOURCE_PW, host=_SOURCE_HOST, port=_SOURCE_PORT, database=_MERGED_DB_NAME, query=_ODBC_QUERY)
IGNORE_ROWS_DB  = make_conn_url("mssql+pyodbc", username=_SOURCE_USER, password=_SOURCE_PW, host=_SOURCE_HOST, port=_SOURCE_PORT, database=_IGNORE_ROWS_DB_NAME, query=_ODBC_QUERY)

DB_TYPE     = "mssql"
DIFF_SCHEMA = "dbo"

# ── MySQL connection strings — built from RAW primitives (make_conn_url encodes) ─
MYSQL_DEID_BASE_CONN       = make_conn_url("mysql+pymysql", username=_DB_USER, password=_DB_PASS, host=_DB_HOST, port=_DB_PORT, database=_DEID_BASE_NAME)
MYSQL_DEID_STAGING_CONN    = make_conn_url("mysql+pymysql", username=_DB_USER, password=_DB_PASS, host=_DB_HOST, port=_DB_PORT, database=_DEID_STAGING_NAME)
MYSQL_DEID_DIFFMERGED_CONN = make_conn_url("mysql+pymysql", username=_DB_USER, password=_DB_PASS, host=_DB_HOST, port=_DB_PORT, database=_DEID_MERGED_NAME)

# ── Deid config ──────────────────────────────────────────────────
DBDETAILSMODEL_NAME = os.environ.get("DBDETAILSMODEL_NAME", "")

# ── Tables CSV (per DAG) ─────────────────────────────────────────
DIFF_MERGE_TABLES_CSV        = os.environ.get("DIFF_MERGE_TABLES_CSV", "")
DEID_TABLES_CSV              = os.environ.get("DEID_TABLES_CSV", "")
DEID_POST_PROCESS_TABLES_CSV = os.environ.get("DEID_POST_PROCESS_TABLES_CSV", "")

# ── Mapping dict (mssql_incremental_mapping_update*.py) ─────────────────────────
MAPPING_MSSQL_CONN = {
    "server":   _PROD_HOST,
    "port":     int(_PROD_PORT),
    "database": _PROD_DB_NAME,
    "user":     _PROD_USER,
    "password": _PROD_PW,
}
MAPPING_MYSQL_CONN = {
    "host":     _DB_HOST,
    "database": _MAPPING_DB_NAME,
    "user":     _DB_USER,
    "password": _DB_PASS,
}

# ── Master recreate connection strings (mssql_incremental_master_recreate.py) ───
MASTER_MSSQL_CONN_STR = make_conn_url("mssql+pyodbc", username=_PROD_USER, password=_PROD_PW, host=_PROD_HOST, port=_PROD_PORT, database=_PROD_DB_NAME, query=_ODBC_QUERY)
MASTER_MYSQL_CONN_STR = make_conn_url("mysql+pymysql", username=_DB_USER, password=_DB_PASS, host=_DB_HOST, port=_DB_PORT, database=_MASTER_DB_NAME)

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
    "port":     int(os.environ.get("MAPPING_DEST_PORT", "3306")),
    "database": os.environ.get("MAPPING_DEST_DATABASE", ""),
    "user":     os.environ.get("MAPPING_DEST_USER", ""),
    "password": os.environ.get("MAPPING_DEST_PASSWORD", ""),
}
MAPPING_CONFIG_FILE     = os.environ.get("MAPPING_CONFIG_FILE", "")
MASTER_SOURCE_CONN_STR  = os.environ.get("MASTER_SOURCE_CONN", "")
MASTER_DEST_CONN_STR    = os.environ.get("MASTER_DEST_CONN", "")
MASTER_MAPPING_CONN_STR = os.environ.get("MASTER_MAPPING_CONN", "")
