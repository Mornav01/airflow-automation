"""
Central configuration.
Loads all settings from Airflow/.env
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# ── State directory ─────────────────────────────────────────────
STATE_DIR     = os.environ["STATE_DIR"]
DEID_REPO_DIR = os.environ["DEID_REPO_DIR"]

# ── Merge config ────────────────────────────────────────────────
PK_CSV                      = os.environ["PK_CSV"]
HISTORICAL_DATA_DEFAULT_DATE = os.environ["HISTORICAL_DATA_DEFAULT_DATE"]

# ── MSSQL ──────────────────────────────────────────────────────
BASE_DB         = os.environ["BASE_DB"]
PROD_DB         = os.environ["PROD_DB"]
DIFF_STAGING_DB = os.environ["DIFF_STAGING_DB"]
DIFFMERGED_CONN = os.environ["DIFFMERGED_CONN"]

DB_TYPE     = "mssql"
DIFF_SCHEMA = "dbo"

# ── MySQL ───────────────────────────────────────────────────────
MYSQL_DEID_BASE_CONN       = os.environ["DEID_BASE_CONN"]
MYSQL_DEID_STAGING_CONN    = os.environ["DEID_STAGING_CONN"]
MYSQL_DEID_DIFFMERGED_CONN = os.environ["DEID_DIFFMERGED_CONN"]

# ── SQL files ───────────────────────────────────────────────────
MAPPING_SQL_FILE          = os.environ["MAPPING_SQL_FILE"]
MASTER_PII_SQL_FILE       = os.environ["MASTER_PII_SQL_FILE"]
MASTER_INSURANCE_SQL_FILE = os.environ["MASTER_INSURANCE_SQL_FILE"]

# ── Deid config ──────────────────────────────────────────────────
DBDETAILSMODEL_NAME = os.environ["DBDETAILSMODEL_NAME"]

# ── Tables CSV (per DAG) ─────────────────────────────────────────
DIFF_MERGE_TABLES_CSV        = os.environ["DIFF_MERGE_TABLES_CSV"]
DEID_TABLES_CSV              = os.environ["DEID_TABLES_CSV"]
DEID_POST_PROCESS_TABLES_CSV = os.environ["DEID_POST_PROCESS_TABLES_CSV"]

# ── Mapping (dict-style) — shared by mapping_update + mapping_update_activeflag ─
MAPPING_MSSQL_CONN = {
    "server":   os.environ["MAPPING_MSSQL_SERVER"],
    "port":     int(os.environ["MAPPING_MSSQL_PORT"]),
    "database": os.environ["MAPPING_MSSQL_DATABASE"],
    "user":     os.environ["MAPPING_MSSQL_USER"],
    "password": os.environ["MAPPING_MSSQL_PASSWORD"],
}
MAPPING_MYSQL_CONN = {
    "host":     os.environ["MAPPING_MYSQL_HOST"],
    "database": os.environ["MAPPING_MYSQL_DATABASE"],
    "user":     os.environ["MAPPING_MYSQL_USER"],
    "password": os.environ["MAPPING_MYSQL_PASSWORD"],
}

# ── Master recreate (connection string style) ────────────────────
MASTER_MSSQL_CONN_STR = os.environ["MASTER_MSSQL_CONN"]
MASTER_MYSQL_CONN_STR = os.environ["MASTER_MYSQL_CONN"]
