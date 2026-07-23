
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
import os


# Import configuration from Django model
from config_loader import (
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_INSECURE_MODE, MYSQL_USER, MYSQL_PASSWORD,
    MYSQL_HOST, INCREMENTAL_SCHEMA, CONTEXT_IDS, BATCH_SIZE, MAX_THREADS,
    TEST_TABLES, EXTRACTION_DATE, FROM_DATE, TO_DATE
)
# Use config values
user = SNOWFLAKE_USER
password = SNOWFLAKE_PASSWORD
account = SNOWFLAKE_ACCOUNT
database = SNOWFLAKE_DATABASE
schema = 'SCHEDULING'
warehouse = SNOWFLAKE_WAREHOUSE
TEST_TABLES = None

# SQL query to get all tables with metadata
query = f"""
SELECT
   table_name,
   table_schema,
   table_type,
   created,
   last_altered
FROM {database}.INFORMATION_SCHEMA.TABLES
WHERE table_schema = '{schema}';
"""

# Snowflake connection
try:
    insecure_mode = SNOWFLAKE_INSECURE_MODE
except NameError:
    insecure_mode = True  # Default to insecure mode if not defined

snowflake_engine = create_engine(
    f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}",
    connect_args={'insecure_mode': insecure_mode}
)


# MySQL connection
mysql_engine = create_engine(
    URL.create("mysql+pymysql", username=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, database=INCREMENTAL_SCHEMA)
)

# Setup logging
def setup_logging():
    """Setup logging configuration for daily extraction"""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create log filename with current date
    current_date = datetime.now().strftime("%Y%m%d")
    log_filename = os.path.join(log_dir, f"daily_extraction_{current_date}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def get_extraction_date():
    """Get the date for extraction. Returns yesterday if EXTRACTION_DATE is None"""
    if EXTRACTION_DATE is None:
        # Get yesterday's date
        yesterday = datetime.now() - timedelta(days=1)
        return yesterday.strftime("%Y-%m-%d")
    else:
        return EXTRACTION_DATE

def get_date_range():
    """Get date range for extraction. Returns single date or date range based on config"""
    try:
        from config_loader import FROM_DATE, TO_DATE
        if FROM_DATE is not None and TO_DATE is not None:
            return FROM_DATE, TO_DATE
        elif FROM_DATE is not None:
            return FROM_DATE, FROM_DATE
        else:
            # Default to single day extraction
            return get_extraction_date(), None
    except (ImportError, NameError):
        # Fallback to single day extraction
        try:
            if FROM_DATE is not None and TO_DATE is not None:
                return FROM_DATE, TO_DATE
            elif FROM_DATE is not None:
                return FROM_DATE, FROM_DATE
        except:
            pass
        return get_extraction_date(), None

# Initialize logging
logger = setup_logging()


# Snowflake to MySQL Data Type Mapping
data_type_mapping = {
   'BOOLEAN': 'TINYINT(1)',
   'DATE': 'DATE',
   'FLOAT': 'FLOAT',


   # NUMBER(p,s)
   'NUMBER(1,0)': 'TINYINT',
   'NUMBER(2,0)': 'TINYINT',
   'NUMBER(3,0)': 'TINYINT',
   'NUMBER(4,0)': 'SMALLINT',
   'NUMBER(5,0)': 'INT',
   'NUMBER(6,0)': 'MEDIUMINT',
   'NUMBER(7,0)': 'MEDIUMINT',
   'NUMBER(8,0)': 'INT',
   'NUMBER(10,0)': 'INT',
   'NUMBER(11,0)': 'INT',
   'NUMBER(12,0)': 'BIGINT',
   'NUMBER(13,0)': 'BIGINT',
   'NUMBER(14,4)': 'DECIMAL(14,4)',
   'NUMBER(16,0)': 'BIGINT',
   'NUMBER(18,0)': 'BIGINT',
   'NUMBER(19,0)': 'BIGINT',
   'NUMBER(20,2)': 'DECIMAL(20,2)',
   'NUMBER(20,8)': 'DECIMAL(20,8)',
   'NUMBER(21,5)': 'DECIMAL(21,5)',
   'NUMBER(22,0)': 'BIGINT',
   'NUMBER(22,2)': 'DECIMAL(22,2)',
   'NUMBER(22,3)': 'DECIMAL(22,3)',
   'NUMBER(24,6)': 'DECIMAL(24,6)',
   'NUMBER(28,8)': 'DECIMAL(28,8)',
   'NUMBER(30,0)': 'DECIMAL(30,0)',
   'NUMBER(32,2)': 'DECIMAL(32,2)',
   'NUMBER(38,0)': 'DECIMAL(38,0)',
   'NUMBER(38,5)': 'DECIMAL(38,5)',
   'NUMBER(38,10)': 'DECIMAL(38,10)',


   'NUMBER(4,2)': 'DECIMAL(4,2)',
   'NUMBER(5,2)': 'DECIMAL(5,2)',
   'NUMBER(5,3)': 'DECIMAL(5,3)',
   'NUMBER(6,0)': 'MEDIUMINT',
   'NUMBER(8,2)': 'DECIMAL(8,2)',
   'NUMBER(8,3)': 'DECIMAL(8,3)',
   'NUMBER(8,4)': 'DECIMAL(8,4)',
   'NUMBER(8,6)': 'DECIMAL(8,6)',
   'NUMBER(10,2)': 'DECIMAL(10,2)',
   'NUMBER(10,4)': 'DECIMAL(10,4)',
   'NUMBER(10,6)': 'DECIMAL(10,6)',
   'NUMBER(11,2)': 'DECIMAL(11,2)',
   'NUMBER(11,3)': 'DECIMAL(11,3)',
   'NUMBER(12,1)': 'DECIMAL(12,1)',
   'NUMBER(12,2)': 'DECIMAL(12,2)',
   'NUMBER(12,3)': 'DECIMAL(12,3)',
   'NUMBER(12,4)': 'DECIMAL(12,4)',
   'NUMBER(12,6)': 'DECIMAL(12,6)',
   'NUMBER(17,5)': 'DECIMAL(17,5)',
   'NUMBER(18,5)': 'DECIMAL(18,5)',
   'NUMBER(18,6)': 'DECIMAL(18,6)',


   # TIMESTAMP
   'TIMESTAMP_NTZ(9)': 'DATETIME',
   'TIMESTAMP_LTZ(9)': 'DATETIME',
   'TIMESTAMP_TZ(9)':  'DATETIME',  # or just DATETIME depending on precision need


   # VARCHAR(n)
   'VARCHAR(1)': 'VARCHAR(1)',
   'VARCHAR(2)': 'VARCHAR(2)',
   'VARCHAR(6)': 'VARCHAR(6)',
   'VARCHAR(7)': 'VARCHAR(7)',
   'VARCHAR(10)': 'VARCHAR(10)',
   'VARCHAR(11)': 'VARCHAR(11)',
   'VARCHAR(12)': 'VARCHAR(12)',
   'VARCHAR(13)': 'VARCHAR(13)',
   'VARCHAR(18)': 'VARCHAR(18)',
   'VARCHAR(20)': 'VARCHAR(20)',
   'VARCHAR(28)': 'VARCHAR(28)',
   'VARCHAR(30)': 'VARCHAR(30)',
   'VARCHAR(50)': 'VARCHAR(50)',
   'VARCHAR(16777216)': 'LONGTEXT',  # MySQL's VARCHAR max is 65535; TEXT is safer
}

# SQL query to get all tables with metadata (defined after database/schema variables)



def insert_data_in_batches(table_name, column_names, data, batch_size):
   total_rows = len(data)
   if total_rows == 0:
       logger.info(f"No data to insert into {table_name}.")
       return

   total_batches = (total_rows // batch_size) + (1 if total_rows % batch_size != 0 else 0)
   logger.info(f"Inserting {total_rows} rows into {table_name} in {total_batches} batches.")


   # Correctly construct the INSERT query using %s placeholders (DO NOT format with .format())
   columns = ", ".join([f"`{col}`" for col in column_names])  # escape column names
   placeholders = ", ".join(["%s"] * len(column_names))
   insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"


   conn = mysql_engine.raw_connection()
   try:
       cursor = conn.cursor()


       for batch_number, start in enumerate(range(0, total_rows, batch_size), start=1):
           batch_tuples = data[start:start + batch_size]


           # Ensure each row is a tuple and escape % so PyMySQL executemany
           # does not treat it as a format placeholder.
           batch_tuples = [
               tuple(v.replace('%', '%%') if isinstance(v, str) else v for v in row)
               for row in batch_tuples
           ]


           logger.info(f"Inserting batch {batch_number}/{total_batches} into {table_name}...")
           cursor.executemany(insert_query, batch_tuples)

       conn.commit()
       logger.info(f"✅ All {total_rows} rows inserted successfully into {table_name}.")

   except Exception as e:
       logger.error(f"❌ Error inserting data into {table_name}: {e}")
       conn.rollback()
       logger.error("Transaction rolled back.")


   finally:
       cursor.close()
       conn.close()



def process_table(contextids, table_name, batch_size, from_date=None, to_date=None, database=None, schema=None):
    """Process a single table for incremental extraction"""
    logger.info(f"Starting processing of table: {table_name}")
    column_names = []

    # Fetching View Definition
    desc_query = f"DESC VIEW {database}.{schema}.{table_name};"
    try:
        with snowflake_engine.connect() as conn:
            result = conn.execute(text(desc_query))
            columns_df = pd.DataFrame(result.fetchall(), columns=result.keys())
        logger.info(f"Successfully retrieved schema for table: {table_name}")
    except Exception as e:
        logger.error(f"Error querying Snowflake for view definition of {table_name}: {e}")
        return


    # Construct CREATE TABLE statement for MySQL
    columns_sql = []
    for _, row in columns_df.iterrows():
        col_name = row["name"]
        snowflake_type = row["type"]
        mysql_type = data_type_mapping.get(snowflake_type, "TEXT")
        columns_sql.append(f"`{col_name}` {mysql_type}")
        column_names.append(col_name)


    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {', '.join(columns_sql)}
    );
    """


    try:
        with mysql_engine.connect() as conn:
            conn.execute(text(create_table_sql))
            logger.info(f"Table {table_name} created/verified in MySQL.")
    except Exception as e:
        logger.error(f"Error creating table {table_name} in MySQL: {e}")
        return


    # Construct SELECT query with date range
    where_clauses = [f"contextid IN {contextids}"]
    
    if from_date and to_date:
        # Date range extraction
        where_clauses.append(f"LASTUPDATED >= '{from_date}' AND LASTUPDATED <= '{to_date}'")
        logger.info(f"Extracting data from {from_date} to {to_date}")
    elif from_date:
        # Single date extraction
        where_clauses.append(f"LASTUPDATED >= '{from_date}'")
        logger.info(f"Extracting data from {from_date}")
    else:
        # Default to yesterday
        yesterday = datetime.now() - timedelta(days=1)
        where_clauses.append(f"LASTUPDATED > '{yesterday.strftime('%Y-%m-%d')}'")
        logger.info(f"Extracting data from yesterday: {yesterday.strftime('%Y-%m-%d')}")
    
    where_clause = " AND ".join(where_clauses)

    select_query = f"SELECT * FROM {database}.{schema}.{table_name} WHERE {where_clause};"
    logger.info(f"Executing query for {table_name}: {select_query}")

    try:
        with snowflake_engine.connect() as conn:
            result = conn.execute(text(select_query))
            data = result.fetchall()
        logger.info(f"Fetched {len(data)} rows from Snowflake for table {table_name}.")
    except Exception as e:
        logger.error(f"Error fetching data from Snowflake for table {table_name}: {e}")
        return


    insert_data_in_batches(table_name, column_names, data, batch_size)





def process_row(row):
   table_name = row['table_name']
   from_date, to_date = get_date_range()
   logger.info(f"Processing table {table_name} for date range: {from_date} to {to_date}")
   
   try:
       process_table(
           contextids=CONTEXT_IDS, 
           table_name=table_name, 
           batch_size=BATCH_SIZE, 
           from_date=from_date,
           to_date=to_date,
           database=database,
           schema=schema
       )
       return f"✅ Finished: {table_name}"
   except Exception as e:
       logger.error(f"❌ Failed to process table {table_name}: {e}")
       return f"❌ Failed: {table_name} - {str(e)}"




# Main execution
def main():
   """Main function to execute daily extraction"""
   from_date, to_date = get_date_range()
   if to_date:
       logger.info(f"Starting extraction for date range: {from_date} to {to_date}")
   else:
       logger.info(f"Starting extraction for date: {from_date}")
   logger.info(f"Configuration - Batch Size: {BATCH_SIZE}, Max Threads: {MAX_THREADS}, Context IDs: {CONTEXT_IDS}")
   
   # Get list of tables to process
   try:
       with snowflake_engine.connect() as connection:
           result = connection.execute(text(query))
           df = pd.DataFrame(result)
           logger.info(f"Found {len(df)} tables to process")
           
           # Filter tables if TEST_TABLES is specified
           if TEST_TABLES is not None:
               df = df[df['table_name'].isin(TEST_TABLES)]
               logger.info(f"Filtered to {len(df)} test tables: {TEST_TABLES}")
               
   except Exception as e:
       logger.error(f"Error querying Snowflake for table list: {e}")
       return
   
   # Track results
   successful_tables = []
   failed_tables = []
   
   # Process tables in parallel
   with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
       futures = [executor.submit(process_row, row) for _, row in df.iterrows()]
       
       for future in as_completed(futures):
           result = future.result()
           logger.info(result)
           
           if "✅ Finished" in result:
               successful_tables.append(result)
           else:
               failed_tables.append(result)
   
   # Summary report
   logger.info("="*50)
   logger.info("EXTRACTION SUMMARY")
   logger.info("="*50)
   if to_date:
       logger.info(f"Date Range: {from_date} to {to_date}")
   else:
       logger.info(f"Extraction Date: {from_date}")
   logger.info(f"Total Tables: {len(df)}")
   logger.info(f"Successful: {len(successful_tables)}")
   logger.info(f"Failed: {len(failed_tables)}")
   
   if failed_tables:
       logger.error("Failed Tables:")
       for failed in failed_tables:
           logger.error(f"  - {failed}")
   
   logger.info("Daily extraction completed.")
   return len(successful_tables), len(failed_tables)

if __name__ == "__main__":
   main()


