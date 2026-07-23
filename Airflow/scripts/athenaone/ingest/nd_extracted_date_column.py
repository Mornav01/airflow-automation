from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

# Import configuration from Django model
from config_loader import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, INCREMENTAL_SCHEMA
connection_string = URL.create("mysql+pymysql", username=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, database=INCREMENTAL_SCHEMA)
schema_name = INCREMENTAL_SCHEMA

engine = create_engine(connection_string)
inspector = inspect(engine)

def main():
    """Main function to add extraction date column to all tables"""
    with engine.connect() as conn:
        tables = inspector.get_table_names(schema=schema_name)

        for table in tables:
            try:
                print(f"Processing table: {table}")

                # Column existence check
                columns = inspector.get_columns(table, schema=schema_name)
                column_names = [col["name"].lower() for col in columns]

                if "nd_extracted_date" not in column_names:
                    alter_sql = text(f"""
                        ALTER TABLE `{schema_name}`.`{table}`
                        ADD COLUMN `nd_extracted_date` DATE;
                    """)
                    conn.execute(alter_sql)
                    print("Column added successfully.")

                update_sql = text(f"""
                    UPDATE `{schema_name}`.`{table}`
                    SET `nd_extracted_date` = CURRENT_DATE();
                """)
                conn.execute(update_sql)

                print(f"Successfully updated: {table}\n")

            except SQLAlchemyError as e:
                print(f"Error processing {table}: {e}\n")

    print("All tables processed successfully.")

if __name__ == "__main__":
    main()
