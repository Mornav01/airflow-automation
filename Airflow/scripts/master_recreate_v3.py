"""
V3 Master Recreate — config-driven master table builder.

Reads source data, joins to patient_mapping_table via N identifier columns
(ordered fallback: first matching column per row wins), and writes to destination
MySQL master tables with rotation (old → _old).

Uses the same YAML config file as incremental_mapping_update_v3.py.
Run AFTER the mapping script so patient_mapping_table is populated.

All DB connections are SQLAlchemy connection strings — the dialect prefix
(mssql+pymssql:// or mysql+pymysql://) determines the driver automatically.

Usage:
    python master_recreate_v3.py --config /path/to/mapping_master_config.yaml
    python master_recreate_v3.py --config /path/to/mapping_master_config.yaml --dry-run
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

IST = timezone(timedelta(hours=5, minutes=30))


# ─── Logging ──────────────────────────────────────────────────────────────────

def setup_logging(state_dir):
    os.makedirs(state_dir, exist_ok=True)
    runner_dir = os.path.join(state_dir, "v3_master_runner")
    os.makedirs(runner_dir, exist_ok=True)
    log_file = os.path.join(runner_dir, f"v3_master_{date.today().isoformat()}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ─── State ────────────────────────────────────────────────────────────────────

class MasterStateManager:

    def __init__(self, state_dir):
        self.state_file = os.path.join(state_dir, "v3_master_state.json")
        os.makedirs(state_dir, exist_ok=True)

    def load(self):
        if not os.path.exists(self.state_file):
            return {}
        with open(self.state_file) as f:
            return json.load(f)

    def save(self, state):
        with open(self.state_file, "w") as f:
            json.dump(state, f, indent=4)


# ─── Patient map loading ──────────────────────────────────────────────────────

def load_patient_maps_from_mysql(mapping_conn_str, identifier_columns):
    """
    Load patient_mapping_table into per-column dicts: {col → {val → nd_patient_id}}.
    Same structure as _load_patient_maps_from_db in the mapping script.
    """
    id_cols_csv = ", ".join(f"`{c}`" for c in identifier_columns)
    engine = create_engine(mapping_conn_str)
    with engine.connect() as conn:
        df = pd.read_sql(
            text(f"SELECT {id_cols_csv}, nd_patient_id FROM patient_mapping_table"),
            conn,
        )

    patient_maps_by_col = {col: {} for col in identifier_columns}
    for _, row in df.iterrows():
        nd_pid = row["nd_patient_id"]
        for col in identifier_columns:
            val = row.get(col)
            if val is not None and not pd.isna(val):
                patient_maps_by_col[col][val] = nd_pid

    logging.info(
        f"Loaded {len(next(iter(patient_maps_by_col.values()), {}))} patients "
        f"across {len(identifier_columns)} identifier columns."
    )
    return patient_maps_by_col


# ─── nd_patient_id resolution ─────────────────────────────────────────────────

def resolve_nd_patient_ids_df(df, patient_maps_by_col, join_cols):
    """
    Add nd_patient_id to each DataFrame row using an ordered-fallback across join_cols.
    For each row: try join_cols[0]; if no match, try join_cols[1]; etc.
    This is NOT a multi-column AND join — it is a prioritised single-column lookup per row.
    """
    nd_pids = pd.Series(pd.NA, index=df.index, dtype="Int64")
    for col in join_cols:
        if col not in df.columns:
            logging.warning(f"join_col '{col}' not in source SQL result — skipping.")
            continue
        col_map = patient_maps_by_col.get(col, {})
        mapped  = df[col].map(col_map)
        try:
            mapped = mapped.astype("Int64")
        except (TypeError, ValueError):
            pass
        nd_pids = nd_pids.combine_first(mapped)
    return nd_pids


# ─── Source query ─────────────────────────────────────────────────────────────

def execute_source_query_df(source_conn_str, sql_file_path):
    """
    Execute a SELECT query against the source DB and return results as a DataFrame.
    The SQLAlchemy dialect in source_conn_str determines the driver automatically:
      mssql+pymssql://...  → MSSQL
      mysql+pymysql://...  → MySQL
    """
    query  = text(Path(sql_file_path).read_text())
    engine = create_engine(source_conn_str)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


# ─── Table rotation ───────────────────────────────────────────────────────────

def rotate_table(mysql_engine, table_name):
    """Rename {table} → {table}_old (drop {table}_old first if it exists)."""
    logging.info(f"Rotating table: {table_name}")
    with mysql_engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}_old`"))
        result = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = :t"
            ),
            {"t": table_name},
        ).scalar()
        if result:
            conn.execute(text(f"RENAME TABLE `{table_name}` TO `{table_name}_old`"))
            logging.info(f"Renamed {table_name} → {table_name}_old")
        conn.commit()


# ─── Single master table transfer ─────────────────────────────────────────────

def transfer_master_table(
    source_conn_str, mysql_conn_str,
    table_cfg, patient_maps_by_col, dry_run=False,
):
    name         = table_cfg["name"]
    sql_file     = table_cfg["sql_file"]
    join_cols    = table_cfg["join_cols"]
    target_table = table_cfg["target_table"]

    logging.info(f"[{name}] Fetching source data from SQL file.")
    df = execute_source_query_df(source_conn_str, sql_file)
    logging.info(f"[{name}] Fetched {len(df)} rows.")

    df.drop_duplicates(inplace=True)
    logging.info(f"[{name}] {len(df)} rows after dedup.")

    present = [c for c in join_cols if c in df.columns]
    if not present:
        raise ValueError(
            f"[{name}] Source SQL must return at least one of join_cols {join_cols}. "
            f"Got columns: {list(df.columns)}"
        )

    df["nd_patient_id"] = resolve_nd_patient_ids_df(df, patient_maps_by_col, join_cols)

    unmapped = df["nd_patient_id"].isna().sum()
    if unmapped:
        logging.warning(
            f"[{name}] {unmapped} rows have no nd_patient_id match "
            f"across join_cols {join_cols}. Written with NULL nd_patient_id."
        )

    if dry_run:
        logging.info(f"[DRY RUN] [{name}] Would write {len(df)} rows to {target_table}.")
        return {"success": True, "row_count": len(df), "unmapped_rows": int(unmapped), "dry_run": True}

    mysql_engine = create_engine(mysql_conn_str)

    rotate_table(mysql_engine, target_table)

    df.to_sql(
        target_table,
        con=mysql_engine,
        index=False,
        if_exists="replace",
        chunksize=1000,
    )
    logging.info(f"[{name}] Written {len(df)} rows to {target_table}.")

    return {"success": True, "row_count": len(df), "unmapped_rows": int(unmapped)}


# ─── Top-level ETL ────────────────────────────────────────────────────────────

def run_master_etl(config, source_conn_str, mysql_conn_str, mapping_conn_str, dry_run=False):
    master_tables = config.get("master_tables", [])
    if not master_tables:
        logging.warning("No master_tables defined in config. Nothing to do.")
        return {}

    identifier_columns = config["patient"]["identifier_columns"]

    patient_maps_by_col = load_patient_maps_from_mysql(mapping_conn_str, identifier_columns)

    results = {}
    for table_cfg in master_tables:
        name = table_cfg["name"]
        logging.info(f"Processing master table: {name}")
        result = transfer_master_table(
            source_conn_str, mysql_conn_str,
            table_cfg, patient_maps_by_col, dry_run,
        )
        results[name] = result

    return results


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="V3 Master Recreate")
    parser.add_argument("--config", required=True, help="Path to YAML mapping+master config file")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without writing to DB")
    args = parser.parse_args()

    sys.path.insert(0, os.path.dirname(__file__))
    from incremental_mapping_update_v5 import load_config
    config = load_config(args.config)

    from db_config import (
        STATE_DIR,
        MASTER_SOURCE_CONN_STR,
        MASTER_DEST_CONN_STR,
        MASTER_MAPPING_CONN_STR,
    )

    state_dir = os.path.join(STATE_DIR, "mapping_master_v3")
    setup_logging(state_dir)

    state_manager = MasterStateManager(state_dir)
    state         = state_manager.load()
    today         = datetime.now(IST).date().isoformat()

    master_tables = config.get("master_tables", [])
    if not args.dry_run and master_tables:
        all_done = all(
            state.get(tbl["name"], {}).get("last_processed_date") == today
            and state.get(tbl["name"], {}).get("success")
            for tbl in master_tables
        )
        if all_done:
            logging.info("All master tables already processed successfully today. Skipping.")
            return

    logging.info("Starting V3 Master Recreate ETL")

    try:
        results = run_master_etl(
            config,
            MASTER_SOURCE_CONN_STR,
            MASTER_DEST_CONN_STR,
            MASTER_MAPPING_CONN_STR,
            args.dry_run,
        )
        for name, result in results.items():
            state[name] = {"last_processed_date": today, **result}
        state_manager.save(state)
        logging.info(f"V3 Master Recreate completed: {results}")

    except Exception as e:
        logging.exception("V3 Master Recreate failed")
        for tbl in master_tables:
            state[tbl["name"]] = {"last_processed_date": today, "success": False, "error": str(e)}
        state_manager.save(state)
        raise


if __name__ == "__main__":
    main()
