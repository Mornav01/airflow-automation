#!/usr/bin/env python
"""
Manual Decryption Script

Decrypts one table from SOURCE_CONN into DEST_CONN.
Decrypt only — no merge, no delete.

Config is loaded from Airflow/manual.env (section: manual_decryption.py).

Usage:
    python manual_decryption.py
    python manual_decryption.py --source-table progressnotes --dest-table progressnotes_decryptfinal

To add decryption for a new table:
  1. Write a class that implements decrypt_row(row_dict) -> row_dict
  2. Decorate it with @register_decryptor("your_table_name")
  3. Add the source→dest name mapping to TABLE_NAME_MAPPING
"""

import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import Column, MetaData, Table, create_engine, func, insert, select, text
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.dialects.mssql import NVARCHAR

# ── Load manual.env ───────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent.parent / "manual.env")

# ── Config ────────────────────────────────────────────────────────────────────
SOURCE_CONN   = os.environ["DECRYPT_SOURCE_CONN"]
DEST_CONN     = os.environ["DECRYPT_DEST_CONN"]
SOURCE_TABLE  = os.environ["DECRYPT_SOURCE_TABLE"]
DEST_TABLE    = os.environ["DECRYPT_DEST_TABLE"]
BATCH_SIZE    = int(os.environ["DECRYPT_BATCH_SIZE"])
MAX_WORKERS   = int(os.environ["DECRYPT_MAX_WORKERS"])

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ── Registry ──────────────────────────────────────────────────────────────────

# Maps: source_table_name → decryptor class
DECRYPTION_REGISTRY: dict[str, type] = {}

# Maps: source_table_name → dest_table_name (default: {table}_decryptfinal)
TABLE_NAME_MAPPING: dict[str, str] = {
    "progressnotes": "progressnotes_decryptfinal",
    # "another_table": "another_table_decryptfinal",
}


def register_decryptor(table_name: str):
    """Decorator to register a decryptor class for a source table name.

    Usage:
        @register_decryptor("progressnotes")
        class ProgressNoteDecryptor:
            def decrypt_row(self, row_dict: dict) -> dict:
                ...
    """
    def decorator(cls):
        DECRYPTION_REGISTRY[table_name] = cls
        return cls
    return decorator


# ── Index helper ──────────────────────────────────────────────────────────────

def _ensure_index(engine, table_name: str, column: str = "nd_auto_increment_id"):
    """Create an index on *column* in *table_name* if it does not already exist.

    Works for both MSSQL and MySQL. Skips silently if the index already exists.
    """
    dialect = engine.dialect.name
    index_name = f"ix_{table_name}_{column}"

    try:
        if dialect == "mssql":
            check_sql = text(f"""
                SELECT 1 FROM sys.indexes
                WHERE object_id = OBJECT_ID(N'{table_name}')
                  AND name = N'{index_name}'
            """)
        else:  # mysql / mariadb
            check_sql = text(f"""
                SELECT 1 FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME   = :tname
                  AND INDEX_NAME   = :iname
                LIMIT 1
            """)

        with engine.connect() as conn:
            if dialect == "mssql":
                exists = conn.execute(check_sql).fetchone() is not None
            else:
                exists = conn.execute(check_sql, {"tname": table_name, "iname": index_name}).fetchone() is not None

        if exists:
            logging.info("[index] %s already exists on %s.%s — skipping", index_name, table_name, column)
            return

        logging.info("[index] creating index %s on %s(%s)...", index_name, table_name, column)
        with engine.begin() as conn:
            if dialect == "mssql":
                conn.execute(text(
                    f"CREATE INDEX [{index_name}] ON [{table_name}] ([{column}])"
                ))
            else:
                conn.execute(text(
                    f"CREATE INDEX `{index_name}` ON `{table_name}` (`{column}`)"
                ))
        logging.info("[index] created %s", index_name)

    except Exception as e:
        logging.warning("[index] could not create index on %s.%s: %s", table_name, column, e)


# ── Offset-based batch fetcher ────────────────────────────────────────────────

def _iter_batches(engine, src_table: Table, batch_size: int):
    """Yield rows in batches using keyset pagination on nd_auto_increment_id.

    Each batch is an independent query — no open cursor is held between batches.
    This keeps MSSQL server-side state minimal and performance consistent
    regardless of how deep into the table we are.
    """
    id_col = src_table.c["nd_auto_increment_id"]
    last_id = 0

    with engine.connect() as conn:
        total = conn.execute(select(func.count()).select_from(src_table)).scalar() or 0

    logging.info("[fetch] total rows: %s", f"{total:,}")

    fetched = 0
    while True:
        with engine.connect() as conn:
            rows = conn.execute(
                select(src_table)
                .where(id_col > last_id)
                .order_by(id_col)
                .limit(batch_size)
            ).fetchall()

        if not rows:
            break

        yield rows, total
        last_id = rows[-1]._mapping["nd_auto_increment_id"]
        fetched += len(rows)


# ── Decryptors ────────────────────────────────────────────────────────────────

@register_decryptor("progressnotes")
class ProgressNoteDecryptor:
    """Decrypts the 'summary' column of the progressnotes table using Blowfish."""

    def __init__(self, datakey="qdnmbf@##$"):
        self._datakey = datakey

    def get_key(self, key_type, key_date):
        local_date = key_date.replace(' ', '_')
        if key_type == 1:
            key_val = self._datakey[::-1] + local_date[6:12]
        else:
            key_val = self._datakey + local_date[2:6]
        return bytes(key_val, 'utf-8')

    def decrypt_pnote(self, d, p):
        import base64, zlib
        from Crypto.Cipher import Blowfish

        _key = self.get_key(1, d)
        if p[:5] == "<?xml"[:5]:
            return self._cleanup_bytes(p.encode('utf-8'))

        msg = base64.b64decode(p[40:])
        pads_required = 16 - (len(msg) % 16)
        if pads_required:
            msg += b'\x00' * pads_required

        c3 = Blowfish.new(_key, Blowfish.MODE_ECB)
        m3 = c3.decrypt(msg)

        try:
            plaintext = zlib.decompress(m3, zlib.MAX_WBITS | 32)
            return self._cleanup_bytes(plaintext)
        except Exception:
            logging.warning("Blowfish decompress failed for date=%s", d)
            return b""

    def _cleanup_bytes(self, byte_data):
        replace_map = {
            b'\x0a': b'', b'\x0d': b'', b'\x0b': b' ',
            b'\xf8': b' ', b'\xe8': b' ', b'\xe9': b' ',
            b'\xe3': b' ', b'\x84': b'a', b'\x85': b'a',
            b'\x96': b'-', b'\x97': b'u', b'\xc2': b'', b'\x92': b'',
        }
        for k, v in replace_map.items():
            byte_data = byte_data.replace(k, v)
        return byte_data

    def decrypt_row(self, row_dict: dict) -> dict:
        summary_raw = row_dict.get("summary", "") or ""
        if summary_raw:
            result = self.decrypt_pnote(str(row_dict.get("ModifyDate", "")), summary_raw)
            row_dict["summary"] = (
                result.decode("utf-8", errors="ignore")
                if isinstance(result, bytes) else str(result)
            )
        else:
            row_dict["summary"] = ""
        return row_dict


# ── Core runner ───────────────────────────────────────────────────────────────

def run_decrypt(source_table_name: str, dest_table_name: str):
    source_engine = create_engine(SOURCE_CONN, pool_pre_ping=True)
    dest_engine   = create_engine(DEST_CONN,   pool_pre_ping=True)

    # ── Lookup decryptor class from registry ────────────────────────────────
    decryptor_cls = DECRYPTION_REGISTRY.get(source_table_name)
    if decryptor_cls is None:
        logging.error(
            "No decryptor registered for table '%s'. "
            "Registered tables: %s",
            source_table_name, list(DECRYPTION_REGISTRY.keys()),
        )
        sys.exit(1)

    decryptor = decryptor_cls()
    logging.info("Using decryptor: %s", decryptor_cls.__name__)

    # ── Reflect source schema ────────────────────────────────────────────────
    metadata = MetaData()
    with source_engine.connect() as conn:
        src_table = Table(source_table_name, metadata, autoload_with=conn)

    # ── Create dest table mirroring source schema ────────────────────────────
    dest_metadata = MetaData()
    new_columns = []
    for col in src_table.columns:
        if col.name == "summary":
            col_type = LONGTEXT() if dest_engine.dialect.name == "mysql" else NVARCHAR(None)
            new_col = Column(col.name, col_type, primary_key=col.primary_key, nullable=col.nullable)
        else:
            new_col = Column(col.name, col.type,
                             primary_key=col.primary_key, nullable=col.nullable,
                             autoincrement=col.autoincrement)
        new_columns.append(new_col)

    dest_tbl = Table(dest_table_name, dest_metadata, *new_columns)
    with dest_engine.begin() as conn:
        dest_metadata.drop_all(conn, tables=[dest_tbl], checkfirst=True)
        dest_metadata.create_all(conn, tables=[dest_tbl])
    logging.info("Dest table '%s' created", dest_table_name)

    # ── Ensure index on nd_auto_increment_id (source + dest) ────────────────
    _ensure_index(source_engine, source_table_name)
    _ensure_index(dest_engine,   dest_table_name)

    # ── Stream via keyset pagination → decrypt in parallel → write ───────────
    logging.info("source: %s", SOURCE_CONN.split("@")[-1])
    logging.info("dest  : %s", DEST_CONN.split("@")[-1])

    insert_stmt   = insert(dest_tbl)
    total_written = 0
    skipped       = 0
    start         = time.monotonic()

    def decrypt_batch(rows):
        results, failed = [], 0
        for row in rows:
            try:
                results.append(decryptor.decrypt_row(dict(row._mapping)))
            except Exception:
                failed += 1
        return results, failed

    batch_num = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for rows, total in _iter_batches(source_engine, src_table, BATCH_SIZE):
            batch_num += 1
            future = executor.submit(decrypt_batch, rows)
            processed, failed = future.result()
            skipped += failed

            if processed:
                with dest_engine.begin() as dest_conn:
                    dest_conn.execute(insert_stmt, processed)
                total_written += len(processed)

            if batch_num % 10 == 0 or total_written >= total:
                pct = (total_written / total * 100) if total else 0
                logging.info(
                    "progress: %s/%s rows (%.1f%%) — batch %d",
                    f"{total_written:,}", f"{total:,}", pct, batch_num,
                )

    elapsed = time.monotonic() - start
    logging.info("written : %s rows in %.1fs", f"{total_written:,}", elapsed)
    if skipped:
        logging.warning("skipped : %d rows (decrypt error)", skipped)
    return total_written


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Manual decryption runner")
    parser.add_argument("--source-table", default=SOURCE_TABLE,
                        help="Source table name (default: DECRYPT_SOURCE_TABLE from manual.env)")
    parser.add_argument("--dest-table",   default=DEST_TABLE,
                        help="Dest table name (default: DECRYPT_DEST_TABLE from manual.env)")
    args = parser.parse_args()

    dest = args.dest_table or TABLE_NAME_MAPPING.get(args.source_table, f"{args.source_table}_decryptfinal")

    logging.info("source table : %s", args.source_table)
    logging.info("dest table   : %s", dest)
    logging.info("registered   : %s", list(DECRYPTION_REGISTRY.keys()))

    run_decrypt(args.source_table, dest)
    logging.info("decryption complete")


if __name__ == "__main__":
    main()
