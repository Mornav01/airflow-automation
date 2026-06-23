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


# ── Deid config ──────────────────────────────────────────────────
DBDETAILSMODEL_NAME = os.environ["DBDETAILSMODEL_NAME"]
PROJECT_NAME = os.environ["PROJECT_NAME"]

# ── Tables CSV (per DAG) ─────────────────────────────────────────
DIFF_MERGE_TABLES_CSV        = os.environ["DIFF_MERGE_TABLES_CSV"]
DEID_TABLES_CSV              = os.environ["DEID_TABLES_CSV"]
DEID_POST_PROCESS_TABLES_CSV = os.environ["DEID_POST_PROCESS_TABLES_CSV"]




# ── AWS transfer (aws_transfer.py) ───────────────────────────────
AWS_TRANSFER_MYSQL_HOST            = os.environ["AWS_TRANSFER_MYSQL_HOST"]
AWS_TRANSFER_MYSQL_PORT            = os.environ["AWS_TRANSFER_MYSQL_PORT"]
AWS_TRANSFER_MYSQL_USER            = os.environ["AWS_TRANSFER_MYSQL_USER"]
AWS_TRANSFER_MYSQL_PASSWORD        = os.environ["AWS_TRANSFER_MYSQL_PASSWORD"]
AWS_TRANSFER_MYSQL_SCHEMA          = os.environ["AWS_TRANSFER_MYSQL_SCHEMA"]
AWS_TRANSFER_MYSQLDUMP_PATH        = os.environ["AWS_TRANSFER_MYSQLDUMP_PATH"]
AWS_TRANSFER_MYSQL_PATH            = os.environ["AWS_TRANSFER_MYSQL_PATH"]
AWS_TRANSFER_S3_BUCKET             = os.environ["AWS_TRANSFER_S3_BUCKET"]
AWS_TRANSFER_S3_PREFIX             = os.environ["AWS_TRANSFER_S3_PREFIX"]
AWS_TRANSFER_S3_REGION             = os.environ["AWS_TRANSFER_S3_REGION"]
AWS_TRANSFER_AWS_ACCESS_KEY_ID     = os.environ["AWS_TRANSFER_AWS_ACCESS_KEY_ID"]
AWS_TRANSFER_AWS_SECRET_ACCESS_KEY = os.environ["AWS_TRANSFER_AWS_SECRET_ACCESS_KEY"]
AWS_TRANSFER_OUTPUT_BASE           = os.environ["AWS_TRANSFER_OUTPUT_BASE"]
AWS_TRANSFER_COUNT_SCHEMA          = os.environ["AWS_TRANSFER_COUNT_SCHEMA"]
AWS_TRANSFER_COUNT_REPORT_LABEL    = os.environ["AWS_TRANSFER_COUNT_REPORT_LABEL"]
AWS_TRANSFER_COUNT_S3_PREFIX       = os.environ["AWS_TRANSFER_COUNT_S3_PREFIX"]


# ── Mapping connections & config (v3 / v4 / v5) ──────────────────
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
MAPPING_CONFIG_FILE = os.environ["MAPPING_CONFIG_FILE"]

# ── Master recreate — connection strings (v3 / v4 / v5) ──────────
MASTER_SOURCE_CONN_STR  = os.environ["MASTER_SOURCE_CONN"]    # source DB (mssql+pymssql:// or mysql+pymysql://)
MASTER_DEST_CONN_STR    = os.environ["MASTER_DEST_CONN"]      # destination MySQL (where master tables are written)
MASTER_MAPPING_CONN_STR = os.environ["MASTER_MAPPING_CONN"]   # mapping MySQL (where patient_mapping_table lives)

# ── DAG Schedules (None = manual trigger only) ──────────────────
DIFF_MERGE_SCHEDULE        = os.environ.get("DIFF_MERGE_SCHEDULE") or None
DEID_SCHEDULE              = os.environ.get("DEID_SCHEDULE") or None
DEID_POST_PROCESS_SCHEDULE = os.environ.get("DEID_POST_PROCESS_SCHEDULE") or None
AWS_TRANSFER_SCHEDULE      = os.environ.get("AWS_TRANSFER_SCHEDULE") or None
GCP_TRANSFER_SCHEDULE      = os.environ.get("GCP_TRANSFER_SCHEDULE") or None
