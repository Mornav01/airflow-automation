import mysql.connector

# Function to establish a connection to the MySQL database
def create_connection(host, user, password, database):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

# Function to copy a table from the source database to the destination database
def copy_table(source_conn, dest_conn, table_name, new_table_name, columns_to_drop):
    source_cursor = source_conn.cursor()
    dest_cursor = dest_conn.cursor()

    # Retrieve the table creation statement from the source database
    source_cursor.execute(f"SHOW CREATE TABLE {table_name}")
    create_table_stmt = source_cursor.fetchone()[1]

    # Create the table in the destination database
    dest_cursor.execute(f"DROP TABLE IF EXISTS {new_table_name}")
    create_table_stmt = create_table_stmt.replace(table_name, new_table_name)
    dest_cursor.execute(create_table_stmt)

    # Fetch all data from the source table
    source_cursor.execute(f"SELECT * FROM {table_name}")
    rows = source_cursor.fetchall()

    # Get column names
    column_names = [i[0] for i in source_cursor.description]

    # Insert data into the destination table
    placeholders = ', '.join(['%s'] * len(column_names))
    insert_stmt = f"INSERT INTO {new_table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
    
    batch_size = 1000
    # Iterate over the data in batches
    for start in range(0, len(rows), batch_size):
        end = start + batch_size
        batch = rows[start:end]
        dest_cursor.executemany(insert_stmt, batch)
        dest_conn.commit()
    
    # Drop specified columns from the destination table
    for column in columns_to_drop:
        dest_cursor.execute(f"ALTER TABLE {new_table_name} DROP COLUMN {column}")
    dest_conn.commit()
    
    dest_cursor.execute(f"alter table {new_table_name} rename column uid to patient_id;")
    dest_conn.commit()
    
    dest_cursor.execute(f"alter table {new_table_name} ADD COLUMN offset_value INT;")
    dest_conn.commit()
    dest_cursor.execute(f"update {new_table_name} SET offset_value = FLOOR(RAND() * 61) - 30;")
    dest_conn.commit()
    # Close cursors
    source_cursor.close()
    dest_cursor.close()

# Main function to execute the table copy and column drop
def main():
    # Source database connection details
    source_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'Neuro@123',
        'database': 'ndsource'
    }

    # Destination database connection details
    dest_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'Neuro@123',
        'database': 'masterdb'
    }

    # Table to copy and columns to drop
    table_name = 'users'
    new_table_name = "pii_data_table"
    
    columns_to_drop = ["register_date", "notes", "medical_note"]

    # Establish connections
    source_conn = create_connection(**source_config)
    dest_conn = create_connection(**dest_config)

    # Copy table and drop columns
    copy_table(source_conn, dest_conn, table_name, new_table_name, columns_to_drop)

    # Close connections
    source_conn.close()
    dest_conn.close()

if __name__ == '__main__':
    main()
