import os
from pathlib import Path
from urllib.parse import quote as _url_quote
from dotenv import load_dotenv

# Load .env from the project root (one directory above services/)
load_dotenv(Path(__file__).parent.parent / ".env", override=True)


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
MAPPING_CONFIG_FILE = os.environ["MAPPING_CONFIG_FILE"]

# ── Master recreate — connection strings (v3 / v4 / v5) ──────────
MASTER_SOURCE_CONN_STR  = os.environ["MASTER_SOURCE_CONN"]    # source DB (mssql+pymssql:// or mysql+pymysql://)
MASTER_DEST_CONN_STR    = os.environ["MASTER_DEST_CONN"]      # destination MySQL (where master tables are written)
MASTER_MAPPING_CONN_STR = os.environ["MASTER_MAPPING_CONN"]   # mapping MySQL (where patient_mapping_table lives)
