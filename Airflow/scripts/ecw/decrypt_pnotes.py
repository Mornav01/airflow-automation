"""
Decrypt progress notes from the progressnotes table and write to progressnotes_decryptfinal.
Reads from staging_schema and writes decrypted data to the same schema.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional, Generator, List, Dict, Any
from concurrent.futures import ProcessPoolExecutor, as_completed

import base64
import re
import zlib
from Crypto.Cipher import Blowfish
from sqlalchemy import create_engine, text, MetaData, Table, Column, select, Text, func
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.dialects.mssql import NVARCHAR
from tqdm import tqdm

logger = logging.getLogger(__name__)

# Built once at import time; reused in every worker process.
# Replaces 12 sequential .replace() calls with a single-pass translate().
_CLEANUP_TABLE = bytes.maketrans(
    bytes([0x0b, 0xf8, 0xe8, 0xe9, 0xe3, 0x84, 0x85, 0x96, 0x97]),
    bytes([0x20, 0x20, 0x20, 0x20, 0x20, 0x61, 0x61, 0x2d, 0x75]),
)
_CLEANUP_DELETE = bytes([0x0a, 0x0d, 0xc2, 0x92])


def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


class ProgressNoteDecryptor:
    def __init__(self, datakey=os.environ.get("DEID_DECRYPT_KEY", "")):
        self._datakey = datakey
        self.TAG_RE = re.compile(r'<[^>]+>')

    def get_key(self, key_type: int, key_date: str) -> bytes:
        local_date = key_date.replace(' ', '_')
        if key_type == 1:
            key_val = self._datakey[::-1] + local_date[6:12]
        else:
            key_val = self._datakey + local_date[2:6]
        return key_val.encode('utf-8')

    def remove_tags(self, text: str) -> str:
        return self.TAG_RE.sub('', text)

    def decrypt_pnote(self, d: str, p: str) -> bytes:
        _key = self.get_key(1, d)

        if p[:5] == "<?xml":
            return p.encode('utf-8').translate(_CLEANUP_TABLE, _CLEANUP_DELETE)

        msg = base64.b64decode(p[40:])
        pad = 16 - (len(msg) % 16)
        if pad:
            msg += b'\x00' * pad

        cipher = Blowfish.new(_key, Blowfish.MODE_ECB)
        decrypted = cipher.decrypt(msg)

        try:
            plaintext = zlib.decompress(decrypted, zlib.MAX_WBITS | 32)
        except Exception:
            return b""
        return plaintext.translate(_CLEANUP_TABLE, _CLEANUP_DELETE)

    def process_text(self, dtmod: str, summary: str) -> bytes:
        return self.decrypt_pnote(dtmod, summary)


# Top-level function required for ProcessPoolExecutor (must be picklable).
def process_row_batch(row_dicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    decryptor = ProgressNoteDecryptor()
    processed = []
    for row_dict in row_dicts:
        try:
            raw = row_dict.get('summary', '')
            if raw:
                result = decryptor.process_text(str(row_dict.get('ModifyDate', '')), raw)
                row_dict['summary'] = result.decode('utf-8', errors='ignore') if isinstance(result, bytes) else str(result)
            else:
                row_dict['summary'] = ''
            processed.append(row_dict)
        except Exception:
            pass  # worker processes can't easily log; malformed rows are skipped
    return processed


def build_engine(db_type: str, db_host: str, db_user: str, db_pass: str, schema: str, db_port: int = None):
    from urllib.parse import quote_plus
    if db_type == "mssql":
        port = db_port or int(os.environ.get("MSSQL_PORT", 1433))
        odbc_driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server").replace(" ", "+")
        url = (
            f"mssql+pyodbc://{quote_plus(db_user)}:{quote_plus(db_pass)}"
            f"@{db_host}:{port}/{schema}"
            f"?driver={odbc_driver}&TrustServerCertificate=yes"
        )
    else:
        port = db_port or int(os.environ.get("MYSQL_PORT", 3306))
        url = f"mysql+pymysql://{db_user}:{quote_plus(db_pass)}@{db_host}:{port}/{schema}"
    engine = create_engine(url, pool_size=5, max_overflow=10, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection successful (%s).", db_type)
        return engine
    except Exception as e:
        logger.error("Connection failed: %s", e)
        return None


def load_table(engine, table_name: str) -> Optional[Table]:
    try:
        meta = MetaData()
        table = Table(table_name, meta, autoload_with=engine)
        logger.info("Loaded schema for '%s'.", table_name)
        return table
    except Exception as e:
        logger.error("Failed to load table '%s': %s", table_name, e)
        return None


def stream_chunks(
    engine, table: Table, batch_size: int
) -> Generator[List[Dict[str, Any]], None, None]:
    """Yield row dicts in batches without loading the full table into memory."""
    with engine.connect().execution_options(stream_results=True) as conn:
        result = conn.execute(select(table))
        while True:
            chunk = result.fetchmany(batch_size)
            if not chunk:
                break
            # Convert to plain dicts here so they are picklable for subprocesses.
            yield [dict(row._mapping) for row in chunk]


def get_row_count(engine, table: Table) -> int:
    with engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(table)).scalar()


def create_dest_table(engine, source: Table, new_name: str) -> Optional[Table]:
    try:
        meta = MetaData()
        cols = []
        for col in source.columns:
            if col.name == 'summary':
                if engine.dialect.name == 'mysql':
                    dtype = LONGTEXT
                elif engine.dialect.name == 'mssql':
                    dtype = NVARCHAR(None)
                else:
                    dtype = Text
                cols.append(Column(
                    col.name, dtype,
                    **{k: getattr(col, k) for k in ['primary_key', 'nullable', 'unique', 'index', 'autoincrement']},
                ))
            else:
                cols.append(Column(
                    col.name, col.type,
                    **{k: getattr(col, k) for k in ['primary_key', 'nullable', 'unique', 'index', 'autoincrement']},
                ))
        dest = Table(new_name, meta, *cols)
        with engine.begin() as conn:
            if engine.dialect.name == "mssql":
                conn.execute(text(f"IF OBJECT_ID(N'{new_name}', N'U') IS NOT NULL DROP TABLE [{new_name}]"))
            else:
                conn.execute(text(f"DROP TABLE IF EXISTS `{new_name}`"))
        meta.create_all(engine, tables=[dest])
        logger.info("Destination table '%s' created.", new_name)
        return dest
    except Exception as e:
        logger.error("Failed to create '%s': %s", new_name, e)
        return None


def run_pipeline(
    engine, source: Table, dest: Table, max_workers: int, batch_size: int
) -> int:
    """
    Bounded pipeline: stream chunks → ProcessPool decrypt → bulk insert.
    At most (max_workers * 2) chunks are in-flight at once to cap memory usage.
    """
    total_rows = get_row_count(engine, source)
    logger.info("Total rows to process: %d", total_rows)

    insert_stmt = dest.insert()
    total_inserted = 0
    max_pending = max_workers * 2

    pbar = tqdm(total=total_rows, desc="Decrypting & inserting", unit="row")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        pending: dict = {}

        def flush_one():
            nonlocal total_inserted
            done = next(as_completed(pending))
            rows = done.result()
            del pending[done]
            if rows:
                with engine.begin() as conn:
                    conn.execute(insert_stmt, rows)
                total_inserted += len(rows)
                pbar.update(len(rows))

        for chunk in stream_chunks(engine, source, batch_size):
            pending[executor.submit(process_row_batch, chunk)] = True
            while len(pending) >= max_pending:
                flush_one()

        while pending:
            flush_one()

    pbar.close()
    logger.info("Done. Inserted %d / %d rows into '%s'.", total_inserted, total_rows, dest.name)
    return total_inserted


def main(
    staging_schema: str,
    db_type: str = os.environ.get("ECW_SOURCE_DB_TYPE", os.environ.get("PNOTES_DB_TYPE", "mysql")),
    db_host: Optional[str] = None,
    db_user: Optional[str] = None,
    db_pass: Optional[str] = None,
    table_name: str = "progressnotes",
    new_table_name: Optional[str] = None,
    max_workers: int = 8,
    batch_size: int = 10000,
):
    # Resolve credentials from environment based on db_type when not provided explicitly.
    if db_host is None:
        db_host = os.environ.get("DB_HOST", "localhost")
    if db_type == "mssql":
        if db_user is None:
            db_user = os.environ.get("SOURCE_USER", "")
        if db_pass is None:
            db_pass = os.environ.get("SOURCE_PW", "")
    else:
        if db_user is None:
            db_user = os.environ.get("DB_USER", "")
        if db_pass is None:
            db_pass = os.environ.get("DB_PASS", "")

    new_table_name = new_table_name or f"{table_name}_decryptfinal"
    logger.info("Starting decrypt_pnotes: db_type=%s, schema=%s, table=%s", db_type, staging_schema, table_name)

    engine = build_engine(db_type, db_host, db_user, db_pass, staging_schema)
    if not engine:
        raise RuntimeError("Failed to connect to database")

    source = load_table(engine, table_name)
    if source is None:
        raise RuntimeError(f"Failed to load source table '{table_name}'")

    dest = create_dest_table(engine, source, new_table_name)
    if dest is None:
        raise RuntimeError(f"Failed to create destination table '{new_table_name}'")

    run_pipeline(engine, source, dest, max_workers=max_workers, batch_size=batch_size)
    logger.info("Completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Decrypt progress notes from staging schema and write to progressnotes_decryptfinal."
    )
    parser.add_argument("--staging_schema", required=True, help="Source database/schema name")
    parser.add_argument(
        "--db_type", default=os.environ.get("ECW_SOURCE_DB_TYPE", os.environ.get("PNOTES_DB_TYPE", "mysql")),
        choices=["mysql", "mssql"],
        help="Database dialect: mysql (default) or mssql. "
             "mysql uses DB_USER/DB_PASS; mssql uses SOURCE_USER/SOURCE_PW.",
    )
    parser.add_argument("--db_host", default=None, help="Overrides DB_HOST env var")
    parser.add_argument("--db_user", default=None, help="Overrides DB_USER (mysql) or SOURCE_USER (mssql)")
    parser.add_argument("--db_pass", default=None, help="Overrides DB_PASS (mysql) or SOURCE_PW (mssql)")
    parser.add_argument("--table_name", default="progressnotes")
    parser.add_argument("--new_table_name", default=None)
    parser.add_argument("--max_workers", type=int, default=8, help="CPU worker processes for decryption")
    parser.add_argument("--batch_size", type=int, default=10000, help="Rows per batch")
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    try:
        main(
            staging_schema=args.staging_schema,
            db_type=args.db_type,
            db_host=args.db_host,
            db_user=args.db_user,
            db_pass=args.db_pass,
            table_name=args.table_name,
            new_table_name=args.new_table_name,
            max_workers=args.max_workers,
            batch_size=args.batch_size,
        )
    except Exception as e:
        logger.exception("Decrypt progress notes failed: %s", e)
        sys.exit(1)
