#!/usr/bin/env python
"""
Diff Decryption Script

Runs AFTER merge_engine. For each table in the CSV that has a registered
decryption function:

  1. Skip if table is NOT in DECRYPTION_REGISTRY
  2. Skip if diff + merge are not both successful (not ready)
  3. Skip if diff_decryption already succeeded today
  4. Run the table-specific decryption function
     → reads from DIFF_STAGING_CONN, writes decrypted rows to DIFF_STAGING_CONN
  5. Merge decrypted staging table into BASE_CONN via merge_engine
  6. Save state["diff_decryption"] with status + row counts

State written per table (state/<table>.json):
  {
    "diff": { ... },
    "merge": { ... },
    "diff_decryption": {
      "last_processed_date": "2026-03-06",
      "status": "success" | "failed",
      "rows_decrypted": 19481,
      "is_merged": true
    }
  }

To add decryption for a new table:
  1. Write a function: decrypt_<tablename>(source_engine, dest_engine, dest_table_name) -> int
  2. Register it in DECRYPTION_REGISTRY
  3. Add the source→dest table name mapping in TABLE_NAME_MAPPING
"""

import argparse
import csv
import json
import logging
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from datetime import datetime

from sqlalchemy import (
    Column, MetaData, Table, Text, create_engine, insert, select, text
)
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.dialects.mssql import NVARCHAR
from tqdm import tqdm

# ============================================================
# ===================== CONFIG ===============================
# ============================================================

STATE_DIRECTORY = "/Users/ndaicnsarizona/Desktop/production_state_incremental/state"

# Source: read encrypted rows from diff_staging
# Destination: write decrypted table back into the same diff_staging DB (new table name)
DIFF_STAGING_CONN = "mssql+pymssql://sa:ndADMIN2025@localhost:1437/mobiledoc_diff_staging"

# Merge step 1: merge decrypted table into base
BASE_CONN = "mssql+pymssql://sa:ndADMIN2025@localhost:1437/mobiledoc_base_2302"

# Merge step 2: accumulate merged snapshot into diff_merged
DIFF_MERGED_CONN = "mssql+pymssql://sa:ndADMIN2025@localhost:1437/mobiledoc_diff_merged"

PK_CSV = "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/input/arizona_primary_keys.csv"

HISTORICAL_DATA_DEFAULT_DATE = "2026-02-23 00:00:00"

DECRYPT_BATCH_SIZE = 1000
DECRYPT_MAX_WORKERS = 10

# ============================================================
# ============== TABLE REGISTRY & NAME MAPPING ===============
# ============================================================

# Maps: source_table_name → decrypted staging table name
TABLE_NAME_MAPPING = {
    "progressnotes": "progressnotes_decryptfinal",
    # "another_table": "another_table_decrypted",
}

# Maps: source_table_name → decryption function
# Each function signature: (source_engine, dest_engine, source_table, dest_table_name) -> int (rows written)
#
# Registered tables (populated via @register_decryptor below):
#   "progressnotes" → decrypt_progressnotes
#
# To add a new table:
#   1. Write the function and decorate it with @register_decryptor("your_table")
#   2. Add the name mapping to TABLE_NAME_MAPPING above
DECRYPTION_REGISTRY = {}

def register_decryptor(table_name):
    """Decorator to register a decryption function for a table."""
    def decorator(fn):
        DECRYPTION_REGISTRY[table_name] = fn
        return fn
    return decorator

# ============================================================
# ===================== LOGGING ==============================
# ============================================================

def setup_logging(state_dir):
    log_dir = os.path.join(state_dir, "diff_decryption")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f"decrypt_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )

# ============================================================
# ===================== STATE MANAGER ========================
# ============================================================

class StateManager:

    def __init__(self, state_dir):
        self.state_dir = state_dir
        os.makedirs(self.state_dir, exist_ok=True)

    def _file_path(self, table):
        return os.path.join(self.state_dir, f"{table}.json")

    def load(self, table):
        path = self._file_path(table)
        if not os.path.exists(path):
            return {"diff": {}, "merge": {}, "diff_decryption": {}}
        with open(path, "r") as f:
            state = json.load(f)
        state.setdefault("diff", {})
        state.setdefault("merge", {})
        state.setdefault("diff_decryption", {})
        return state

    def save(self, table, state):
        path = self._file_path(table)
        with open(path, "w") as f:
            json.dump(state, f, indent=4)

# ============================================================
# ===================== SKIP LOGIC ===========================
# ============================================================

def is_diff_merge_complete(state):
    """Both diff and merge must have succeeded (any date)."""
    return (
        state.get("diff", {}).get("success") is True
        and state.get("merge", {}).get("success") is True
    )

def is_decryption_done_today(state):
    """Decryption already succeeded today → skip."""
    dec = state.get("diff_decryption", {})
    return (
        dec.get("last_processed_date") == date.today().isoformat()
        and dec.get("status") == "success"
    )

# ============================================================
# ============== PK LOADER ===================================
# ============================================================

def load_pk_map(pk_csv):
    pk_map = {}
    with open(pk_csv, newline="") as f:
        for row in csv.DictReader(f):
            pk_value = row["primary_key"]
            pk_map[row["table_name"]] = (
                [col.strip() for col in pk_value.split(",") if col.strip()]
                if pk_value else []
            )
    return pk_map

# ============================================================
# ============== PROGRESS NOTES DECRYPTOR ====================
# ============================================================

class ProgressNoteDecryptor:

    def __init__(self, datakey="qdnmbf@##$"):
        import base64, re
        self._datakey = datakey
        self.TAG_RE = re.compile(r'<[^>]+>')

    def get_key(self, key_type, key_date):
        local_date = key_date.replace(' ', '_')
        if key_type == 1:
            key_val = self._datakey[::-1] + local_date[6:12]
        else:
            key_val = self._datakey + local_date[2:6]
        return bytes(key_val, 'utf-8')

    def decrypt_pnote(self, d, p):
        import base64, zlib, traceback
        from Crypto.Cipher import Blowfish

        _key = self.get_key(1, d)
        if p[:5] == "<?xml"[:5]:
            return self.cleanup_bytes(p.encode('utf-8'))

        msg = base64.b64decode(p[40:])
        pads_required = 16 - (len(msg) % 16)
        if pads_required:
            msg += b'\x00' * pads_required

        c3 = Blowfish.new(_key, Blowfish.MODE_ECB)
        m3 = c3.decrypt(msg)

        try:
            _plaintext = zlib.decompress(m3, zlib.MAX_WBITS | 32)
            return self.cleanup_bytes(_plaintext)
        except Exception:
            logging.warning("Blowfish decrypt failed for date=%s", d)
            return b""

    def cleanup_bytes(self, byte_data):
        replace_map = {
            b'\x0a': b'', b'\x0d': b'', b'\x0b': b' ',
            b'\xf8': b' ', b'\xe8': b' ', b'\xe9': b' ',
            b'\xe3': b' ', b'\x84': b'a', b'\x85': b'a',
            b'\x96': b'-', b'\x97': b'u', b'\xc2': b'', b'\x92': b'',
        }
        for k, v in replace_map.items():
            byte_data = byte_data.replace(k, v)
        return byte_data

    def process_text(self, dtmod, summary):
        result = self.decrypt_pnote(dtmod, summary)
        if isinstance(result, bytes):
            return result.decode("utf-8", errors="ignore")
        return str(result)


# ============================================================
# ========= DECRYPTION FUNCTION: progressnotes ===============
# ============================================================

def _batch(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]


@register_decryptor("progressnotes")
def decrypt_progressnotes(source_engine, dest_engine, source_table_name, dest_table_name):
    """
    Decrypt the 'summary' column of the progressnotes table.
    Reads from source_engine, writes decrypted rows to dest_engine.
    Returns the number of rows written.
    """
    # ── Read source ──────────────────────────────────────────
    logging.info("[progressnotes] reading source table")
    metadata = MetaData()
    with source_engine.connect() as src_conn:
        src_table = Table(source_table_name, metadata, autoload_with=src_conn)
        rows = src_conn.execute(select(src_table)).fetchall()

    logging.info("[progressnotes] read %d rows from source", len(rows))

    # ── Create dest table mirroring source schema ─────────────
    dest_metadata = MetaData()
    new_columns = []
    for col in src_table.columns:
        if col.name == 'summary':
            if dest_engine.dialect.name == 'mysql':
                col_type = LONGTEXT()
            elif dest_engine.dialect.name == 'mssql':
                col_type = NVARCHAR(None)
            else:
                col_type = Text()
            new_col = Column(col.name, col_type,
                             primary_key=col.primary_key,
                             nullable=col.nullable)
        else:
            new_col = Column(col.name, col.type,
                             primary_key=col.primary_key,
                             nullable=col.nullable,
                             autoincrement=col.autoincrement)
        new_columns.append(new_col)

    dest_table = Table(dest_table_name, dest_metadata, *new_columns)
    with dest_engine.begin() as dest_conn:
        dest_metadata.drop_all(dest_conn, tables=[dest_table], checkfirst=True)
        dest_metadata.create_all(dest_conn, tables=[dest_table])
    logging.info("[progressnotes] dest table '%s' created", dest_table_name)

    # ── Decrypt rows in parallel batches ─────────────────────
    decryptor = ProgressNoteDecryptor()

    insert_stmt = insert(dest_table)

    def process_batch(row_batch):
        processed = []
        for row in row_batch:
            if row is None:
                continue
            try:
                row_dict = dict(row._mapping)
                summary_raw = row_dict.get('summary', '') or ''
                if summary_raw:
                    decrypted = decryptor.process_text(
                        str(row_dict.get('ModifyDate', '')),
                        summary_raw,
                    )
                    row_dict['summary'] = decrypted
                else:
                    row_dict['summary'] = ''
                processed.append(row_dict)
            except Exception:
                logging.warning("[progressnotes] error on row, skipping: %s",
                                traceback.format_exc())
        return processed

    total_inserted = 0
    with dest_engine.begin() as dest_conn:
        with ThreadPoolExecutor(max_workers=DECRYPT_MAX_WORKERS) as executor:
            futures = [
                executor.submit(process_batch, chunk)
                for chunk in _batch(rows, DECRYPT_BATCH_SIZE)
            ]
            for future in tqdm(futures, desc="[progressnotes] inserting", unit="batch"):
                processed_rows = future.result()
                if processed_rows:
                    dest_conn.execute(insert_stmt, processed_rows)
                    total_inserted += len(processed_rows)

    logging.info("[progressnotes] inserted %d decrypted rows into '%s'",
                 total_inserted, dest_table_name)
    return total_inserted


# ============================================================
# ===================== MERGE STEP ===========================
# ============================================================

def run_merge_step(table, pk_cols, source_conn, dest_conn, diffmerged_conn):
    """Merge decrypted staging table into base, accumulate snapshot into diff_merged."""
    from merge_engine import MergeEngine

    engine = MergeEngine(
        db_type="mssql",
        main_conn=dest_conn,          # mobiledoc_base_2302
        diff_conn=source_conn,        # mobiledoc_diff_staging (decrypted table)
        diffmerged_conn=diffmerged_conn,  # mobiledoc_diff_merged (accumulation)
    )

    try:
        result = engine.run_table(
            table=table,
            pk_cols=pk_cols,
            default_date=HISTORICAL_DATA_DEFAULT_DATE,
        )
        return result
    except Exception as e:
        return {
            "success": False,
            "table": table,
            "error": str(e),
            "traceback": traceback.format_exc(),
        }

# ============================================================
# ===================== CORE LOGIC ===========================
# ============================================================

def process_table(table_name, state_manager, pk_map):

    # ── 1. Only process registered tables ────────────────────
    if table_name not in DECRYPTION_REGISTRY:
        logging.debug("[%s] not in DECRYPTION_REGISTRY, skipping", table_name)
        return

    state = state_manager.load(table_name)
    dec_state = state.get("diff_decryption", {})

    # ── 2. Diff + Merge must be complete ─────────────────────
    if not is_diff_merge_complete(state):
        logging.info("[%s] diff/merge not complete → skip decryption", table_name)
        return

    # ── 3. Skip if already done today ────────────────────────
    if is_decryption_done_today(state):
        logging.info("[%s] decryption already succeeded today → skip", table_name)
        return

    dest_table_name = TABLE_NAME_MAPPING.get(table_name, f"{table_name}_decrypted")
    decrypt_fn      = DECRYPTION_REGISTRY[table_name]
    pk_cols         = pk_map.get(table_name, [])

    today = date.today().isoformat()

    # ── 4. Run decryption ─────────────────────────────────────
    logging.info("[%s] starting decryption → dest table: %s", table_name, dest_table_name)

    source_engine = create_engine(DIFF_STAGING_CONN)
    dest_engine   = create_engine(DIFF_STAGING_CONN)

    try:
        rows_decrypted = decrypt_fn(source_engine, dest_engine, table_name, dest_table_name)

        dec_state = {
            "last_processed_date": today,
            "status":              "decrypted",
            "rows_decrypted":      rows_decrypted,
            "is_merged":           False,
        }
        state["diff_decryption"] = dec_state
        state_manager.save(table_name, state)

    except Exception:
        logging.exception("[%s] decryption failed", table_name)
        state["diff_decryption"] = {
            "last_processed_date": today,
            "status":              "failed",
            "error":               traceback.format_exc(),
        }
        state_manager.save(table_name, state)
        return

    # ── 5. Merge decrypted data into base ─────────────────────
    logging.info("[%s] merging '%s' into base", table_name, dest_table_name)

    merge_result = run_merge_step(
        table=dest_table_name,
        pk_cols=pk_cols,
        source_conn=DIFF_STAGING_CONN,
        dest_conn=BASE_CONN,
        diffmerged_conn=DIFF_MERGED_CONN,
    )

    if merge_result.get("success"):
        logging.info(
            "[%s] merge succeeded → updated=%d new=%d",
            table_name,
            merge_result.get("updated_rows", 0),
            merge_result.get("new_rows", 0),
        )
        dec_state["is_merged"] = True
        dec_state["status"]    = "success"
    else:
        logging.error("[%s] merge into base failed: %s", table_name, merge_result.get("error"))
        dec_state["status"]      = "failed"
        dec_state["merge_error"] = merge_result.get("error")

    state["diff_decryption"] = dec_state
    state_manager.save(table_name, state)


# ============================================================
# =========================== MAIN ===========================
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables_filepath", required=True,
                        help="CSV of tables to process (one per line)")
    parser.add_argument("--pk_csv",    default=PK_CSV)
    parser.add_argument("--state_dir", default=STATE_DIRECTORY)
    args = parser.parse_args()

    setup_logging(args.state_dir)

    pk_map = load_pk_map(args.pk_csv)

    with open(args.tables_filepath, newline="") as f:
        tables = [row[0].strip() for row in csv.reader(f)]

    logging.info(
        "Decryption registry has %d table(s): %s",
        len(DECRYPTION_REGISTRY),
        list(DECRYPTION_REGISTRY.keys()),
    )
    logging.info("Processing %d tables from CSV", len(tables))

    state_manager = StateManager(args.state_dir)

    failed_tables = []
    for table in tables:
        try:
            process_table(table, state_manager, pk_map)
        except Exception:
            logging.exception("[%s] unexpected error", table)
            failed_tables.append(table)

    if failed_tables:
        logging.error("Decryption failed for tables: %s", failed_tables)
        sys.exit(1)

    logging.info("Diff decryption and merge process exited")


if __name__ == "__main__":
    main()
