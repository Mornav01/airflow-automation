#!/usr/bin/env python3
"""
Full schema sync: incremental -> historical
Handles reserved keywords by quoting all column names
nd_auto_increment_id is copied from incremental (assigned in Step 9)
Parallel execution, logging, and row counts
"""

import logging
import time
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

# Import configuration from Django model
from config_loader import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, INCREMENTAL_SCHEMA, HISTORICAL_SCHEMA
CONN_STR = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
INCR_SCHEMA = INCREMENTAL_SCHEMA
HIST_SCHEMA = HISTORICAL_SCHEMA


MAX_WORKERS = 10
ROW_COUNTS = True
ERROR_POLICY_CONTINUE = True
RETRY_ATTEMPTS = 2
RETRY_BACKOFF_SECONDS = 3

# Logging
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOG_FILE = os.path.join(LOG_DIR, f"full_schema_sync_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger("full_schema_sync")

# ---------- Helpers ----------
def q(name: str) -> str:
    """Quote a column or table name with backticks"""
    return f"`{name}`"

def get_columns(engine, schema, table):
    inspector = inspect(engine)
    return [col["name"] for col in inspector.get_columns(table, schema=schema)]

def process_table(table_name: str, engine):
    stats = {
        "table": table_name,
        "created": False,
        "src_count": None,
        "dst_before": None,
        "dst_after": None,
        "inserted": None,
        "duration": None,
        "error": None,
        "attempts": 0
    }

    src_fqn = f"{q(INCR_SCHEMA)}.{q(table_name)}"
    dst_fqn = f"{q(HIST_SCHEMA)}.{q(table_name)}"

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        stats["attempts"] = attempt
        start = time.time()
        try:
            with engine.begin() as conn:
                # Create missing table in historical if needed
                exists_sql = f"""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = :schema AND table_name = :table
                """
                exists = conn.execute(text(exists_sql), {"schema": HIST_SCHEMA, "table": table_name}).scalar() > 0
                if not exists:
                    create_sql = f"CREATE TABLE {dst_fqn} LIKE {src_fqn};"
                    conn.execute(text(create_sql))
                    stats["created"] = True
                    logger.info("Created table: %s", table_name)

                # Get column lists
                incr_cols = get_columns(engine, INCR_SCHEMA, table_name)
                hist_cols = get_columns(engine, HIST_SCHEMA, table_name)

                # Include nd_auto_increment_id in merge (assigned in step 9)
                # Don't exclude it - we want to copy the IDs assigned in update_nd_auto_inc_id.py

                # Determine common columns
                common_cols = sorted(list(set(incr_cols).intersection(hist_cols)))
                if not common_cols:
                    logger.warning("No matching columns for table %s. Skipping.", table_name)
                    return stats

                # Quote all column names to handle reserved keywords
                quoted_cols = [q(c) for c in common_cols]
                col_list_str = ", ".join(quoted_cols)

                # Row counts before insert
                if ROW_COUNTS:
                    stats["src_count"] = conn.execute(text(f"SELECT COUNT(*) FROM {src_fqn}")).scalar()
                    stats["dst_before"] = conn.execute(text(f"SELECT COUNT(*) FROM {dst_fqn}")).scalar()

                # Insert data
                insert_sql = f"INSERT INTO {dst_fqn} ({col_list_str}) SELECT {col_list_str} FROM {src_fqn}"
                conn.execute(text(insert_sql))

                # Row counts after insert
                if ROW_COUNTS:
                    stats["dst_after"] = conn.execute(text(f"SELECT COUNT(*) FROM {dst_fqn}")).scalar()
                    stats["inserted"] = stats["dst_after"] - (stats["dst_before"] or 0)

                stats["duration"] = round(time.time() - start, 3)
                stats["error"] = None

                logger.info(
                    "Table: %s | created=%s | src=%s | dst_before=%s | dst_after=%s | inserted=%s | time=%ss",
                    table_name, stats["created"], stats["src_count"],
                    stats["dst_before"], stats["dst_after"], stats["inserted"], stats["duration"]
                )
                return stats

        except SQLAlchemyError as e:
            stats["error"] = f"{type(e).__name__}: {str(e)}"
            logger.exception("Error processing table %s (attempt %d/%d)", table_name, attempt, RETRY_ATTEMPTS)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF_SECONDS)
                continue
            if ERROR_POLICY_CONTINUE:
                return stats
            else:
                raise

    return stats

# ---------- Main ----------
def main():
    logger.info("Starting full schema sync: %s -> %s", INCR_SCHEMA, HIST_SCHEMA)
    engine = create_engine(CONN_STR, pool_pre_ping=True)

    # Get list of tables
    insp = inspect(engine)
    incr_tables = insp.get_table_names(schema=INCR_SCHEMA)
    hist_tables = set(insp.get_table_names(schema=HIST_SCHEMA))

    logger.info("Incremental tables: %d | Historical tables: %d", len(incr_tables), len(hist_tables))

    results = []
    start_all = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_table = {executor.submit(process_table, tbl, engine): tbl for tbl in incr_tables}
        for future in as_completed(future_to_table):
            tbl = future_to_table[future]
            try:
                res = future.result()
                results.append(res)
            except Exception as exc:
                logger.exception("Unhandled exception for table %s: %s", tbl, exc)

    total_time = round(time.time() - start_all, 2)
    total_tables = len(results)
    created_tables = sum(1 for r in results if r.get("created"))
    succeeded = sum(1 for r in results if not r.get("error"))
    failed = total_tables - succeeded
    total_inserted = sum((r.get("inserted") or 0) for r in results)

    logger.info("----- Job Summary -----")
    logger.info("Total tables processed: %d", total_tables)
    logger.info("Tables created in historical: %d", created_tables)
    logger.info("Successful tables: %d | Failed: %d", succeeded, failed)
    logger.info("Total rows inserted (sum of per-table deltas): %d", total_inserted)
    logger.info("Total elapsed time: %ss", total_time)
    logger.info("Detailed per-table results saved in log: %s", LOG_FILE)

    # Optional CSV summary
    try:
        import csv
        summary_csv = LOG_FILE.replace(".log", "_summary.csv")
        with open(summary_csv, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=[
                "table", "created", "src_count", "dst_before", "dst_after", "inserted", "duration", "error", "attempts"
            ])
            writer.writeheader()
            for r in results:
                writer.writerow(r)
        logger.info("CSV summary written: %s", summary_csv)
    except Exception:
        logger.exception("Failed to write CSV summary")

if __name__ == "__main__":
    main()
