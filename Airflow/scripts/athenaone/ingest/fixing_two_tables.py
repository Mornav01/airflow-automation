from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Import configuration from Django model
try:
    from config_loader import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, INCREMENTAL_SCHEMA
    conn_str = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{INCREMENTAL_SCHEMA}"
    schema_name = INCREMENTAL_SCHEMA
except ImportError as e:
    raise RuntimeError(
        f"Could not import config_loader — check ingest/config_loader.py and .env: {e}"
    ) from e

# SQL statements to execute
sql_commands = [
    f"""
    ALTER TABLE {schema_name}.MEDICATION
    MODIFY COLUMN GCNCLINICALFORUMULATIONID INT NULL;
    """,
    f"""
    ALTER TABLE {schema_name}.INTERFACEMESSAGEDATACLOB
    MODIFY COLUMN MESSAGETEXT LONGTEXT;
    """,
    f"""
    ALTER TABLE {schema_name}.INTERFACEMESSAGEDATACLOB
    MODIFY COLUMN RESPONSETEXT LONGTEXT;
    """
]

def run_alter_commands():
    engine = create_engine(conn_str, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        for command in sql_commands:
            try:
                print(f"Executing:\n{command.strip()}")
                conn.execute(text(command))
                print("Success\n")
            except SQLAlchemyError as e:
                print(f"Error executing command: {e}\n")

if __name__ == "__main__":
    run_alter_commands()
