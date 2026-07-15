#!/usr/bin/env python3
"""
Update ND Auto Increment IDs - Pipeline Step 9
Assigns unique nd_auto_increment_id to all tables in incremental schema
Usage: Called from manual_pipeline_runner.py
"""

from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import traceback
import sys
import os
from datetime import datetime

# Add parent to path for config import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import configuration
try:
    #from config import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, INCREMENTAL_SCHEMA, HISTORICAL_SCHEMA, MAX_WORKERS
    from config_loader import (
        MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, INCREMENTAL_SCHEMA, HISTORICAL_SCHEMA, MAX_THREADS
    )
    # Use MAX_THREADS as MAX_WORKERS for compatibility
    MAX_WORKERS = MAX_THREADS
except ImportError as e:
    raise RuntimeError(
        f"Could not import config_loader — check ingest/config_loader.py and .env: {e}"
    ) from e

# Configuration
MYSQL_CONN = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/information_schema"
historical_schema = HISTORICAL_SCHEMA
incremental_schema = INCREMENTAL_SCHEMA

def process_table(table, engine, incremental_schema, historical_schema):
    """Process a single table to add nd_auto_increment_id"""
    try:
        with engine.connect() as conn:
            print(f"🧩 Processing table: {table}")

            # --- Step 1: Add column if missing
            try:
                conn.execute(text(f"""
                    ALTER TABLE `{incremental_schema}`.`{table}`
                    ADD COLUMN nd_auto_increment_id BIGINT UNIQUE;
                """))
                conn.commit()
                print(f"✅ Added column `nd_auto_increment_id` to `{table}`.")
            except Exception as e:
                if "Duplicate column name" in str(e):
                    print(f"ℹ️ Column already exists in `{table}` — skipping addition.")
                else:
                    print(f"⚠️ Could not add column to `{table}`: {e}")

            # --- Step 2: Get record counts
            hist_count = conn.execute(
                text(f"SELECT COUNT(*) FROM `{historical_schema}`.`{table}`")
            ).scalar() or 0
            inc_count = conn.execute(
                text(f"SELECT COUNT(*) FROM `{incremental_schema}`.`{table}`")
            ).scalar() or 0

            if inc_count == 0:
                return {
                    "table_name": table,
                    "status": "SKIPPED - No records",
                    "historical_records": hist_count,
                    "incremental_records": 0
                }

            # --- Step 3: Get max ID from historical
            max_id_result = conn.execute(
                text(f"SELECT COALESCE(MAX(nd_auto_increment_id), 0) FROM `{historical_schema}`.`{table}`")
            ).scalar() or 0

            # --- Step 4: Assign incremental IDs manually
            conn.execute(text("SET @row_num := 0;"))
            conn.execute(
                text(f"""
                    UPDATE `{incremental_schema}`.`{table}`
                    SET nd_auto_increment_id = (@row_num := @row_num + 1) + :max_id
                    WHERE nd_auto_increment_id IS NULL
                    ORDER BY (SELECT NULL);
                """), {"max_id": max_id_result}
            )
            conn.commit()

            # --- Step 5: Verify new IDs
            new_min_id = conn.execute(
                text(f"SELECT MIN(nd_auto_increment_id) FROM `{incremental_schema}`.`{table}`")
            ).scalar()
            new_max_id = conn.execute(
                text(f"SELECT MAX(nd_auto_increment_id) FROM `{incremental_schema}`.`{table}`")
            ).scalar()

            return {
                "table_name": table,
                "status": "UPDATED",
                "historical_records": hist_count,
                "incremental_records": inc_count,
                "max_id_in_historical": max_id_result,
                "min_new_id": new_min_id,
                "max_new_id": new_max_id
            }

    except Exception as e:
        traceback.print_exc()
        return {"table_name": table, "status": f"ERROR: {e}"}


def main():
    """Main function to update nd_auto_increment_id for all tables"""
    print(f"🔢 Starting ND Auto Increment ID update...")
    print(f"   Incremental Schema: {incremental_schema}")
    print(f"   Historical Schema: {historical_schema}")
    print(f"   Max Workers: {MAX_WORKERS}")
    
    engine = create_engine(MYSQL_CONN)
    summary = []
    
    # --- Step 1: Get common tables between schemas
    with engine.connect() as conn:
        tables_query = text(f"""
            SELECT table_name 
            FROM tables 
            WHERE table_schema = :historical
            AND table_name IN (
                SELECT table_name FROM tables WHERE table_schema = :incremental
            )
        """)
        tables = [r[0] for r in conn.execute(tables_query, {"historical": historical_schema, "incremental": incremental_schema})]

    print(f"✅ Found {len(tables)} common tables between `{historical_schema}` and `{incremental_schema}`.\n")

    # --- Step 2: Run updates in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_table = {executor.submit(process_table, t, engine, incremental_schema, historical_schema): t for t in tables}
        for future in as_completed(future_to_table):
            result = future.result()
            summary.append(result)
            print(f"🔹 {result['table_name']}: {result['status']}")

    # --- Step 3: Save report
    df_summary = pd.DataFrame(summary)
    csv_filename = f"nd_auto_increment_id_summary_{datetime.now().strftime('%Y%m%d')}.csv"
    df_summary.to_csv(csv_filename, index=False)
    print(f"\n✅ Summary saved as {csv_filename}")
    print(f"✅ Update nd_auto_increment_id completed for {len(tables)} tables")
    
    # Cleanup
    engine.dispose()


if __name__ == "__main__":
    main()
