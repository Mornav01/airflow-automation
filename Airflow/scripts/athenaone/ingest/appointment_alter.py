#!/usr/bin/env python3
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

# Import configuration from Django model
try:
    from config_loader import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, INCREMENTAL_SCHEMA
    CONN_STR = URL.create("mysql+pymysql", username=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, database=INCREMENTAL_SCHEMA)
    SCHEMA = INCREMENTAL_SCHEMA
except ImportError as e:
    raise RuntimeError(
        f"Could not import config_loader — check ingest/config_loader.py and .env: {e}"
    ) from e

# Table names
OLD_TABLE = "APPOINTMENT"
NEW_TABLE = "APPOINTMENT_2"

rename_sql = f"ALTER TABLE `{SCHEMA}`.`{OLD_TABLE}` RENAME TO `{SCHEMA}`.`{NEW_TABLE}`;"

def main():
    engine = create_engine(CONN_STR, pool_pre_ping=True)
    try:
        with engine.begin() as conn:
            conn.execute(text(rename_sql))
            print(f"Table renamed successfully: {OLD_TABLE} -> {NEW_TABLE}")
    except SQLAlchemyError as e:
        print(f"Failed to rename table. Error: {e}")

if __name__ == "__main__":
    main()
