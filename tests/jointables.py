import os
import django
import sys

# Set up Django environment
sys.path.append(
    "/Users/rohit.chouhan/NEDI/CODE/Dump/Project/DeIdentification/deIdentification"
)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from core.dbPkg.dbhandler import NDDBHandler
from deIdentification.nd_logger import nd_logger
from typing import TypedDict
from sqlalchemy import select, join, Table
from sqlalchemy.orm import aliased


class Condition(TypedDict):
    source_column: str
    reference_table: str
    column_name: str

class JoinCondition(TypedDict):
    source_table: str
    conditions: list[Condition]
    destination_column: str

def create_temporary_table_after_join(sourcedb, join_condition: JoinCondition):
    source_table_name = join_condition["source_table"]
    source_column = join_condition["conditions"][0]["source_column"]  # Initial source column
    destination_column = join_condition["destination_column"]

    all_rows = sourcedb.get_rows(source_table_name, 1000, 0)
    mapping = {row[source_column]: None for row in all_rows}
    breakpoint()
    for idx, condition in enumerate(join_condition["conditions"]):
        source_column = condition["source_column"]
        reference_table = condition["reference_table"]
        reference_column = condition["column_name"]

        column_values = list(mapping.keys())

        reference_rows = sourcedb.get_rows_where_column_values_in(
            reference_table, reference_column, column_values
        )

        if idx + 1 < len(join_condition["conditions"]):
            next_column = join_condition["conditions"][idx + 1]["source_column"]
        else:
            next_column = destination_column

        new_mapping = {}
        for row in reference_rows:
            if row[reference_column] in mapping:
                new_mapping[row[reference_column]] = row.get(next_column, None)
        breakpoint()
        mapping = new_mapping
    breakpoint()
    return mapping

def create_temporary_table_after_join2(sourcedb, join_condition: JoinCondition):
    source_table_name = join_condition["source_table"]
    initial_source_column = join_condition["conditions"][0]["source_column"]
    destination_column = join_condition["destination_column"]

    # Get initial data
    all_rows = sourcedb.get_rows(source_table_name, 1000, 0)
    original_mapping = {row[initial_source_column]: row[initial_source_column] for row in all_rows}
    current_mapping = original_mapping.copy()

    for idx, condition in enumerate(join_condition["conditions"]):
        reference_table = condition["reference_table"]
        reference_column = condition["column_name"]
        next_column = (join_condition["conditions"][idx + 1]["source_column"] 
                      if idx + 1 < len(join_condition["conditions"]) 
                      else destination_column)
        
        column_values = list(current_mapping.values())
        
        reference_rows = sourcedb.get_rows_where_column_values_in(
            reference_table, reference_column, column_values
        )

        new_mapping = {}
        found_mapping_for = []
        for original_key, current_value in current_mapping.items():
            for row in reference_rows:
                if row[reference_column] == current_value:
                    new_mapping[original_key] = row[next_column]
                    found_mapping_for.append(original_key)
                    break
        for original_key, current_value in current_mapping.items():
            if original_key not in found_mapping_for:
                new_mapping[original_key] = None
        current_mapping = new_mapping
    return current_mapping


join_config: JoinCondition = {
    "source_table": "facilities",
    "destination_column": "uid",
    "destination_column_type": "patient_id",
    "conditions": [
        {
            "source_column": "nd_id",
            "reference_table": "enc_table",
            "column_name": "doctorID"
        },
        {
            "source_column": "patientID",
            "reference_table": "users",
            "column_name": "uid"
        },
    ],
}
source_conn_str = "mysql+pymysql://root:123456789@localhost:3306/nddenttest"
source_db = NDDBHandler(source_conn_str)

dest_conn_str = 'mysql+pymysql://root:123456789@localhost:3306/full_automation'
dest_db = NDDBHandler(dest_conn_str)

create_temporary_table_after_join2(source_db, join_config)

# create_temporary_table_after_join(source_db, dest_db, join_config)

# def create_temporary_table_after_join(source_db: NDDBHandler, dest_db: NDDBHandler, join_condition: JoinCondition):
#     # Extracting the source table and destination column
#     source_table_name = join_condition["source_table"]
#     destination_column = join_condition["destination_column"]
    
#     # Initialize the source table and column to select from
#     source_table = Table(source_table_name, source_db.metadata, autoload_with=source_db.engine)

#     # We need to build the join condition dynamically based on the provided join conditions
#     join_clauses = []
#     tables_to_join = [source_table]
    
#     # Create a list of tables to join and generate the join clauses
#     for condition in join_condition["conditions"]:
#         source_column = condition["source_column"]
#         reference_table_name = condition["reference_table"]
#         reference_column = condition["column_name"]
        
#         # Reference table alias to avoid name clashes
#         reference_table = Table(reference_table_name, source_db.metadata, autoload_with=source_db.engine)
        
#         # Define the join condition
#         join_clause = join(tables_to_join[-1], reference_table, getattr(tables_to_join[-1].c, source_column) == getattr(reference_table.c, reference_column))
#         tables_to_join.append(reference_table)
#         join_clauses.append(join_clause)

#     # Select the destination column after applying the joins
#     select_query = select(
#         *[getattr(tables_to_join[0].c, col.name) for col in source_table.columns],  # All columns from source_table
#         getattr(tables_to_join[-1].c, destination_column)  # Add the destination_column
#     ).select_from(tables_to_join[-1])
#     # Now, let's construct the query for data selection
#     try:
#         # Execute the SELECT query to join the tables and get the required data
#         result = source_db.session.execute(select_query)
#         rows = result.fetchall()
#         # If we have data, we insert it into the destination table
#         if rows:
#             # Convert rows to list of dictionaries, including only the relevant columns
#             data_to_insert = [
#                 {**{col.name: row[idx] for idx, col in enumerate(source_table.columns)},  # Include source_table columns
#                  destination_column: row[-1]}  # Add the destination_column
#                 for row in rows
#             ]
#             breakpoint()
#             # Assuming destination table exists, insert the data
#             dest_db.insert_to_db(data_to_insert, source_table_name)
#             nd_logger.info(f"Data from joined tables inserted into {source_table_name} in destination database.")
#         else:
#             nd_logger.info("No data to insert into the destination database from the join.")

#     except Exception as e:
#         nd_logger.error(f"Error during the join operation or data insertion: {str(e)}")
#         source_db.session.rollback()
#         dest_db.session.rollback()
