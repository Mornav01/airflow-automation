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
MSSQL_BASE_CONN         = os.environ["MSSQL_BASE_CONN"]
MSSQL_PROD_CONN         = os.environ["MSSQL_PROD_CONN"]
MSSQL_DIFF_STAGING_CONN = os.environ["MSSQL_DIFF_STAGING_CONN"]
MSSQL_DIFFMERGED_CONN   = os.environ["MSSQL_DIFFMERGED_CONN"]

# ── MySQL ───────────────────────────────────────────────────────
MYSQL_DEID_BASE_CONN      = os.environ["MYSQL_DEID_BASE_CONN"]
MYSQL_DEID_STAGING_CONN   = os.environ["MYSQL_DEID_STAGING_CONN"]
MYSQL_DEID_DIFFMERGED_CONN = os.environ["MYSQL_DEID_DIFFMERGED_CONN"]
