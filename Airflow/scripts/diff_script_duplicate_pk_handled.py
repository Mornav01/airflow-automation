"""
sp_run_diff.py
Equivalent of [dbo].[sp_run_diff] stored procedure in Python.

Usage:
    python sp_run_diff.py \
        --table  <table_name> \
        --old_conn  "mssql+pyodbc://...OldDB..." \
        --new_conn  "mssql+pyodbc://...NewDB..." \
        --diff_conn "mssql+pyodbc://...DiffDB..."

Connection string format (SQLAlchemy / pyodbc):
    mssql+pyodbc://<user>:<pass>@<server>/<db>?driver=ODBC+Driver+17+for+SQL+Server
    -- or --
    mssql+pyodbc:///?odbc_connect=Driver=...;Server=...;Database=...;Trusted_Connection=yes
"""

import argparse
import hashlib
import logging
import os
from typing import Optional

import pandas as pd
import pyodbc
import sqlalchemy as sa
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

CHUNK_SIZE = 50_000          # rows per batch when reading large tables
HASH_DELIM = "|"             # must match CONCAT_WS delimiter in the SP
EXCLUDE_TYPE_IDS = {34, 35, 99}  # text, ntext, image  (SQL system_type_id)


# ===========================================================================
# Engine helpers
# ===========================================================================

def make_engine(conn_str: str) -> sa.engine.Engine:
    """Return a SQLAlchemy engine from an ODBC / SQLAlchemy connection string."""
    return sa.create_engine(conn_str)


# ===========================================================================
# Metadata helpers  (mirror the SP's dynamic SQL blocks)
# ===========================================================================

def get_primary_key(engine: sa.engine.Engine, table: str, schema: str = "dbo") -> Optional[str]:
    """
    Return the FIRST column of the primary key (TOP 1 in SP).
    Returns None when no PK exists.
    """
    sql = text("""
        SELECT TOP 1 c.name
        FROM   sys.indexes        i
        JOIN   sys.index_columns  ic ON i.object_id  = ic.object_id
                                     AND i.index_id   = ic.index_id
        JOIN   sys.columns        c  ON ic.object_id  = c.object_id
                                     AND ic.column_id  = c.column_id
        JOIN   sys.tables         t  ON t.object_id   = i.object_id
        JOIN   sys.schemas        s  ON s.schema_id   = t.schema_id
        WHERE  i.is_primary_key = 1
          AND  t.name = :table
          AND  s.name = :schema
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"table": table, "schema": schema}).fetchone()
    return row[0] if row else None


def get_identity_col(engine: sa.engine.Engine, table: str, schema: str = "dbo") -> Optional[str]:
    sql = text("""
        SELECT c.name
        FROM   sys.columns c
        JOIN   sys.tables  t ON c.object_id = t.object_id
        JOIN   sys.schemas s ON s.schema_id = t.schema_id
        WHERE  t.name = :table
          AND  s.name = :schema
          AND  c.is_identity = 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"table": table, "schema": schema}).fetchone()
    return row[0] if row else None


def get_all_columns(engine: sa.engine.Engine, table: str, schema: str = "dbo") -> list[str]:
    """
    Return ALL column names in column_id order  (mirrors @AllNewCols in SP).
    """
    sql = text("""
        SELECT c.name
        FROM   sys.columns c
        JOIN   sys.tables  t ON c.object_id = t.object_id
        JOIN   sys.schemas s ON s.schema_id = t.schema_id
        WHERE  t.name = :table
          AND  s.name = :schema
        ORDER  BY c.column_id
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"table": table, "schema": schema}).fetchall()
    return [r[0] for r in rows]


def get_comparable_columns(
    new_engine: sa.engine.Engine,
    old_engine: sa.engine.Engine,
    table: str,
    schema: str = "dbo",
) -> list[str]:
    """
    INTERSECT of non-computed, non-LOB columns present in BOTH new and old DB.
    Mirrors @ComparableColsBase in the SP.
    """
    exclude_q = ",".join(str(x) for x in EXCLUDE_TYPE_IDS)

    col_sql = text(f"""
        SELECT c.name
        FROM   sys.columns c
        JOIN   sys.tables  t ON c.object_id = t.object_id
        JOIN   sys.schemas s ON s.schema_id = t.schema_id
        WHERE  t.name = :table
          AND  s.name = :schema
          AND  c.system_type_id NOT IN ({exclude_q})
          AND  c.is_computed   = 0
    """)

    def fetch_cols(engine):
        with engine.connect() as conn:
            rows = conn.execute(col_sql, {"table": table, "schema": schema}).fetchall()
        return {r[0] for r in rows}

    new_cols = fetch_cols(new_engine)
    old_cols = fetch_cols(old_engine)
    common   = new_cols & old_cols          # Python set INTERSECT

    # Preserve new-DB column_id ordering (same as SP's WITHIN GROUP (ORDER BY column_id))
    all_new  = get_all_columns(new_engine, table, schema)
    return [c for c in all_new if c in common]


def get_table_ddl_info(engine: sa.engine.Engine, table: str, schema: str = "dbo") -> list[dict]:
    """
    Return column metadata needed to recreate the table in DiffDB.
    Mirrors the CREATE TABLE dynamic SQL in the SP.
    """
    sql = text("""
        SELECT c.name,
               t.name            AS type_name,
               c.max_length,
               c.precision,
               c.scale,
               c.is_nullable,
               c.column_id
        FROM   sys.columns c
        JOIN   sys.tables  tb ON c.object_id    = tb.object_id
        JOIN   sys.schemas sc ON tb.schema_id   = sc.schema_id
        JOIN   sys.types   t  ON c.user_type_id = t.user_type_id
        WHERE  tb.name = :table
          AND  sc.name = :schema
        ORDER  BY c.column_id
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"table": table, "schema": schema}).fetchall()
    return [dict(r._mapping) for r in rows]


def has_column(engine: sa.engine.Engine, table: str, schema: str, col_name: str) -> bool:
    """Generic check if a column exists in a table."""
    sql = text("""
        SELECT COUNT(*)
        FROM   sys.columns c
        JOIN   sys.tables  t ON c.object_id = t.object_id
        JOIN   sys.schemas s ON s.schema_id = t.schema_id
        WHERE  t.name  = :table
          AND  s.name  = :schema
          AND  c.name  = :col_name
    """)
    with engine.connect() as conn:
        cnt = conn.execute(sql, {"table": table, "schema": schema, "col_name": col_name}).scalar()
    return cnt > 0


def has_active_flag(engine: sa.engine.Engine, table: str, schema: str = "dbo") -> bool:
    """Check if nd_ActiveFlag column exists in old DB (mirrors SP's @HasActiveFlag)."""
    sql = text("""
        SELECT COUNT(*)
        FROM   sys.columns c
        JOIN   sys.tables  t ON c.object_id = t.object_id
        JOIN   sys.schemas s ON s.schema_id = t.schema_id
        WHERE  t.name  = :table
          AND  s.name  = :schema
          AND  c.name  = 'nd_ActiveFlag'
    """)
    with engine.connect() as conn:
        cnt = conn.execute(sql, {"table": table, "schema": schema}).scalar()
    return cnt > 0


# ===========================================================================
# DiffDB table management
# ===========================================================================

def build_col_ddl(col: dict) -> str:
    """Turn a metadata row into a column DDL fragment."""
    name = f"[{col['name']}]"
    typ  = col["type_name"].lower()

    if typ in ("varchar", "char", "nvarchar", "nchar"):
        ml = col["max_length"]
        if ml == -1:
            size = "MAX"
        elif typ in ("nvarchar", "nchar"):
            size = str(ml // 2)
        else:
            size = str(ml)
        type_str = f"{typ}({size})"
    elif typ in ("decimal", "numeric"):
        type_str = f"{typ}({col['precision']},{col['scale']})"
    else:
        type_str = typ

    nullability = "NULL" if col["is_nullable"] else "NOT NULL"
    return f"{name} {type_str} {nullability}"


def ensure_diff_table(
    diff_engine: sa.engine.Engine,
    table: str,
    schema: str,
    col_meta: list[dict],
) -> None:
    """
    Drop-and-recreate the diff table with extra columns diff_type, nd_extracted_date.
    Mirrors steps 3 and the DROP in the SP.
    """
    col_defs = ",\n    ".join(build_col_ddl(c) for c in col_meta)
    full_name = f"[{schema}].[{table}]"

    ddl = f"""
        IF OBJECT_ID(N'{full_name}') IS NOT NULL
            DROP TABLE {full_name};

        CREATE TABLE {full_name} (
            {col_defs},
            [diff_type]          VARCHAR(20)  NULL,
            [nd_extracted_date]  DATETIME2    NULL DEFAULT CAST(CAST(SYSUTCDATETIME() AS DATE) AS DATETIME2)
        );
    """
    with diff_engine.begin() as conn:
        conn.execute(text(ddl))
    log.info("Diff table [%s].[%s] created.", schema, table)


# ===========================================================================
# Row hashing  (mirrors HASHBYTES('SHA2_256', CONCAT_WS('|', ...)))
# ===========================================================================

def hash_row(row: pd.Series, cols: list[str]) -> bytes:
    """
    Reproduce T-SQL:  HASHBYTES('SHA2_256', CONCAT_WS('|', col1, col2, ...))
    NULL  → empty string  (CONCAT_WS silently skips NULLs between delimiters,
                            but for a single value it still contributes nothing;
                            we match that by converting NaN/None → '')
    """
    parts = []
    for c in cols:
        v = row[c]
        if pd.isna(v) if not isinstance(v, str) else False:
            parts.append("")
        else:
            parts.append(str(v))
    raw = HASH_DELIM.join(parts).encode("utf-8")
    return hashlib.sha256(raw).digest()


def hash_df(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    return df.apply(lambda row: hash_row(row, cols), axis=1)


# ===========================================================================
# Data read helpers
# ===========================================================================

def read_table_chunked(engine: sa.engine.Engine, table: str, schema: str,
                        cols: list[str], where: str = "") -> pd.DataFrame:
    """
    Read the full table in CHUNK_SIZE batches and concatenate.
    Chunking is done via OFFSET/FETCH on column_id order so every row is
    read exactly once and is always paired with itself across calls.
    """
    col_list = ", ".join(f"[{c}]" for c in cols)
    base_q   = f"SELECT {col_list} FROM [{schema}].[{table}]"
    if where:
        base_q += f" WHERE {where}"

    chunks = []
    offset = 0
    # We need a deterministic ORDER BY for OFFSET … FETCH.
    # Use the first column (usually PK or first column in table).
    order_col = f"[{cols[0]}]"

    while True:
        q = f"{base_q} ORDER BY {order_col} OFFSET {offset} ROWS FETCH NEXT {CHUNK_SIZE} ROWS ONLY"
        chunk = pd.read_sql(q, engine)
        if chunk.empty:
            break
        chunks.append(chunk)
        if len(chunk) < CHUNK_SIZE:
            break
        offset += CHUNK_SIZE

    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=cols)


def read_table_full(engine: sa.engine.Engine, table: str, schema: str,
                     cols: list[str], where: str = "") -> pd.DataFrame:
    col_list = ", ".join(f"[{c}]" for c in cols)
    q = f"SELECT {col_list} FROM [{schema}].[{table}]"
    if where:
        q += f" WHERE {where}"
    return pd.read_sql(q, engine)


# ===========================================================================
# Insert helper
# ===========================================================================

def insert_to_diff(diff_engine: sa.engine.Engine,
                   df: pd.DataFrame,
                   table: str,
                   schema: str,
                   diff_type: str) -> None:
    """
    Insert rows into the diff table in safe batches.
    SQL Server hard-limits bound parameters to 2100 per statement.
    We auto-calculate batch_size from the actual column count so we
    always stay safely under that limit regardless of table width.
      safe_batch = floor(2000 / num_cols)   (2000 gives a small headroom)
      minimum is 1 row per batch (extreme edge case: 2000+ columns)
    """
    if df.empty:
        log.info("  Nothing to insert for diff_type='%s'.", diff_type)
        return

    df = df.copy()
    df["diff_type"] = diff_type
    df["nd_extracted_date"] = pd.Timestamp.utcnow().normalize()

    num_cols   = len(df.columns)
    batch_size = max(1, 2000 // num_cols)
    total      = len(df)

    log.info(
        "  Inserting %d rows  diff_type='%s'  cols=%d  batch_size=%d",
        total, diff_type, num_cols, batch_size,
    )

    inserted = 0
    for start in range(0, total, batch_size):
        chunk = df.iloc[start : start + batch_size]
        chunk.to_sql(
            name=table,
            con=diff_engine,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=batch_size,
        )
        inserted += len(chunk)
        log.info("  ... %d / %d rows inserted", inserted, total)

    log.info("  ✅ Done inserting %d rows  diff_type='%s'.", total, diff_type)


# ===========================================================================
# Core diff logic
# ===========================================================================

def dedup_old_df(
    old_df: pd.DataFrame,
    pk: str,
    has_extracted_date: bool,
) -> pd.DataFrame:
    """
    Deduplicate OldDB rows per PK so each PK appears exactly once.

    If nd_extracted_date is present:
        - Keep the row with the MAX nd_extracted_date per PK.
        - On ties, keep the first one found (arbitrary but deterministic).
    Else:
        - Keep the first row found per PK.
    """
    if has_extracted_date and "nd_extracted_date" in old_df.columns:
        # Sort descending by date so the latest comes first, then drop dupes
        old_df = (
            old_df
            .sort_values("nd_extracted_date", ascending=False, na_position="last")
            .drop_duplicates(subset=[pk], keep="first")
        )
        log.info("  Deduped OLD rows by nd_extracted_date (latest kept). Rows after dedup: %d", len(old_df))
    else:
        before = len(old_df)
        old_df = old_df.drop_duplicates(subset=[pk], keep="first")
        log.info("  Deduped OLD rows arbitrarily (no nd_extracted_date). %d → %d rows", before, len(old_df))

    return old_df


def diff_with_pk(
    new_engine:  sa.engine.Engine,
    old_engine:  sa.engine.Engine,
    diff_engine: sa.engine.Engine,
    table:       str,
    schema:      str,
    pk:          str,
    all_new_cols:       list[str],
    comparable_cols:    list[str],
    active_flag_filter: bool,
) -> None:
    """
    PK-based diff  (mirrors the IF @HasPK = 1 branch).

    Logic:
      NEW_ONLY  – PK exists in new but NOT in old (active rows only)
      CHANGED   – PK exists in both but hash differs

    OldDB dedup: if OldDB has duplicate PKs with nd_ActiveFlag='Y',
    we keep only the latest row per PK (by nd_extracted_date if available,
    else first found) before comparing — so each new row is always compared
    against exactly one old row.
    """
    log.info("  Reading NEW table …")
    new_df = read_table_full(new_engine, table, schema, all_new_cols)

    # Columns to fetch from old — comparable cols + PK + nd_extracted_date if present
    old_has_extracted_date = has_column(old_engine, table, schema, "nd_extracted_date")
    old_fetch_cols = list({pk} | set(comparable_cols))
    if old_has_extracted_date and "nd_extracted_date" not in old_fetch_cols:
        old_fetch_cols.append("nd_extracted_date")

    # Build old-DB WHERE clause
    old_where = "nd_ActiveFlag = 'Y'" if active_flag_filter else ""
    log.info("  Reading OLD table (filter='%s') …", old_where or "none")
    old_df = read_table_full(old_engine, table, schema, old_fetch_cols, where=old_where)

    # ── Deduplicate OldDB so each PK appears exactly once ───────────────────
    old_df = dedup_old_df(old_df, pk, has_extracted_date=old_has_extracted_date)

    # ── NEW_ONLY ────────────────────────────────────────────────────────────
    old_pks = set(old_df[pk].dropna())
    new_only = new_df[~new_df[pk].isin(old_pks)]
    insert_to_diff(diff_engine, new_only[all_new_cols], table, schema, "NEW_ONLY")

    # ── CHANGED ─────────────────────────────────────────────────────────────
    # Merge on PK — now guaranteed 1:1 after dedup
    old_compare_cols = [pk] + [c for c in comparable_cols if c != pk]
    merged = new_df[new_df[pk].isin(old_pks)].merge(
        old_df[old_compare_cols],
        on=pk,
        suffixes=("_new", "_old"),
    )

    # Resolve column names after merge (suffixes applied only on conflicts)
    new_comp_cols = [c + "_new" if c + "_new" in merged.columns else c
                     for c in comparable_cols]
    old_comp_cols = [c + "_old" if c + "_old" in merged.columns else c
                     for c in comparable_cols]

    merged["_hash_new"] = merged.apply(lambda row: hash_row(row, new_comp_cols), axis=1)
    merged["_hash_old"] = merged.apply(lambda row: hash_row(row, old_comp_cols), axis=1)

    changed_pks = merged.loc[merged["_hash_new"] != merged["_hash_old"], pk]
    changed = new_df[new_df[pk].isin(changed_pks)]
    insert_to_diff(diff_engine, changed[all_new_cols], table, schema, "CHANGED")


def diff_without_pk(
    new_engine:       sa.engine.Engine,
    old_engine:       sa.engine.Engine,
    diff_engine:      sa.engine.Engine,
    table:            str,
    schema:           str,
    all_new_cols:     list[str],
    comparable_cols:  list[str],
) -> None:
    """
    HASH-based diff  (mirrors the ELSE branch / #old_hash temp table logic).

    Logic:
      Build a multiset of SHA256 hashes for every OLD row.
      Any NEW row whose hash is NOT in that multiset  →  NEW_OR_CHANGED.

    Point 5: old DB may have duplicates all with nd_ActiveFlag='Y';
             we read ALL old rows (no active-flag filter) because the SP
             also reads all rows for the hash table in the no-PK branch.
    """
    log.info("  [No-PK] Building OLD hash set …")
    old_df = read_table_full(old_engine, table, schema, comparable_cols)

    # Build a Counter so that duplicate rows in old are counted correctly.
    # A new row is "already known" only if old has AT LEAST that many copies.
    from collections import Counter
    old_hashes = Counter(
        old_df.apply(lambda row: hash_row(row, comparable_cols), axis=1)
    )

    log.info("  [No-PK] Reading NEW table …")
    new_df = read_table_full(new_engine, table, schema, all_new_cols)

    new_comp_available = [c for c in comparable_cols if c in new_df.columns]
    new_df["_hash"] = new_df.apply(
        lambda row: hash_row(row, new_comp_available), axis=1)

    # For each new row check against the counter (consume matches)
    remaining = dict(old_hashes)
    mask = []
    for h in new_df["_hash"]:
        if remaining.get(h, 0) > 0:
            remaining[h] -= 1
            mask.append(False)   # found in old → not new/changed
        else:
            mask.append(True)    # not found → NEW_OR_CHANGED

    new_or_changed = new_df[mask][all_new_cols]
    insert_to_diff(diff_engine, new_or_changed, table, schema, "NEW_OR_CHANGED")


# ===========================================================================
# Main entry point  (mirrors the stored procedure body)
# ===========================================================================

def run_diff(
    table_name: str,
    old_conn:   str,
    new_conn:   str,
    diff_conn:  str,
    schema:     str = "dbo",
) -> None:
    log.info("🔍 Processing table: %s", table_name)

    old_engine  = make_engine(old_conn)
    new_engine  = make_engine(new_conn)
    diff_engine = make_engine(diff_conn)

    table = table_name

    # ── 1. Primary key (from NEW DB) ─────────────────────────────────────────
    pk     = get_primary_key(new_engine, table, schema)
    has_pk = pk is not None
    log.info("  PK=%s", pk if has_pk else "(none)")

    # ── 2. Column lists ───────────────────────────────────────────────────────
    all_new_cols     = get_all_columns(new_engine, table, schema)
    comparable_cols  = get_comparable_columns(new_engine, old_engine, table, schema)
    col_meta         = get_table_ddl_info(new_engine, table, schema)

    log.info("  All new cols     : %s", all_new_cols)
    log.info("  Comparable cols  : %s", comparable_cols)

    # ── 3. Create diff table ─────────────────────────────────────────────────
    ensure_diff_table(diff_engine, table, schema, col_meta)

    # ── 4/5. Diff logic ───────────────────────────────────────────────────────
    if has_pk:
        log.info("🔑 Using PK-based diff on '%s'", pk)

        # Check nd_ActiveFlag in OLD DB
        af_filter = has_active_flag(old_engine, table, schema)
        log.info("  nd_ActiveFlag filter: %s", af_filter)

        diff_with_pk(
            new_engine, old_engine, diff_engine,
            table, schema, pk,
            all_new_cols, comparable_cols,
            active_flag_filter=af_filter,
        )
    else:
        log.info("⚠️  No PK – using HASH-based diff")
        diff_without_pk(
            new_engine, old_engine, diff_engine,
            table, schema,
            all_new_cols, comparable_cols,
        )

    log.info("✅ Completed table: %s", table)
    log.info("🎯 ALL TABLE DIFFERENCES COMPLETED")


# ===========================================================================
# CLI
# ===========================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Python port of sp_run_diff")
    parser.add_argument("--table",     required=True,  help="Table name")
    parser.add_argument("--old_conn",  default=os.environ.get("MSSQL_OLD_CONN", ""))
    parser.add_argument("--new_conn",  default=os.environ.get("MSSQL_NEW_CONN", ""))
    parser.add_argument("--diff_conn", default=os.environ.get("MSSQL_DIFF_CONN", ""))
    parser.add_argument("--schema",    default="dbo",  help="Schema name (default: dbo)")
    args = parser.parse_args()

    run_diff(
        table_name = args.table,
        old_conn   = args.old_conn,
        new_conn   = args.new_conn,
        diff_conn  = args.diff_conn,
        schema     = args.schema,
    )