"""
diff_script_optimised.py
Optimised Python port of [dbo].[sp_run_diff].

Optimisation over diff_script_duplicate_pk_handled.py
------------------------------------------------------
Old approach:  read_table_full() pulls ALL columns x ALL rows from both DBs
               into pandas, then hashes every row in Python (df.apply axis=1).

New approach (3-phase for PK tables, 2-phase for no-PK tables):
  Phase 1  Push HASHBYTES to SQL Server — pull only (pk + 32-byte hash) per row.
  Phase 2  Python set/merge ops on the tiny hash DataFrames.
  Phase 3  Fetch full rows from NewDB for delta rows only.

Composite PK support: get_primary_key_cols() returns ALL key columns.
  Dedup, comparison and Phase-3 fetch all operate on the full composite key.
  Phase-3 uses a VALUES-derived-table JOIN to avoid separator/type issues.

Hash normalisation: _row_hash is always normalised to bytes() so that
  bytes vs memoryview from different pyodbc connections always compare equal.

Constraint: OldDB and NewDB are on different SQL Server instances — no
            cross-database or linked-server queries are possible.

Usage:
    python diff_script_optimised.py \
        --table  <table_name> \
        --old_conn  "mssql+pyodbc://...OldDB..." \
        --new_conn  "mssql+pyodbc://...NewDB..." \
        --diff_conn "mssql+pyodbc://...DiffDB..."
"""

import argparse
import logging
from collections import Counter
from typing import Optional

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HASH_DELIM       = "|"            # must match CONCAT_WS delimiter in the original SP
EXCLUDE_TYPE_IDS = {34, 35, 99}   # text, ntext, image
PK_BATCH_SIZE    = 500            # composite-key rows per VALUES JOIN in Phase 3


# ===========================================================================
# Engine helpers
# ===========================================================================

def make_engine(conn_str: str, fast_executemany: bool = False) -> sa.engine.Engine:
    if fast_executemany:
        return sa.create_engine(conn_str, fast_executemany=True)
    return sa.create_engine(conn_str)


# ===========================================================================
# Metadata helpers
# ===========================================================================

def get_primary_key_cols(engine: sa.engine.Engine, table: str, schema: str = "dbo") -> list[str]:
    """
    Return ALL primary key columns sorted alphabetically.
    Returns [] when no PK exists.

    Sorted alphabetically (not by key_ordinal) so that tuple comparisons in
    diff_with_pk are order-independent — the same column set always produces
    the same tuple shape regardless of how the PK was defined in each DB.
    """
    sql = text("""
        SELECT c.name
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
        rows = conn.execute(sql, {"table": table, "schema": schema}).fetchall()
    return sorted(r[0] for r in rows)  # alphabetical — immune to key_ordinal differences


def get_all_columns(engine: sa.engine.Engine, table: str, schema: str = "dbo") -> list[str]:
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

    common  = fetch_cols(new_engine) & fetch_cols(old_engine)
    all_new = get_all_columns(new_engine, table, schema)
    return [c for c in all_new if c in common]


def get_table_ddl_info(engine: sa.engine.Engine, table: str, schema: str = "dbo") -> list[dict]:
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
    return has_column(engine, table, schema, "nd_ActiveFlag")


# ===========================================================================
# DiffDB table management
# ===========================================================================

def build_col_ddl(col: dict) -> str:
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
    col_defs  = ",\n    ".join(build_col_ddl(c) for c in col_meta)
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
# SQL hash builder
# ===========================================================================

def build_hash_sql(
    table: str,
    schema: str,
    comparable_cols: list[str],
    select_cols: Optional[list[str]] = None,
    where: str = "",
) -> str:
    """
    SELECT [select_cols], HASHBYTES('SHA2_256', CONCAT_WS('|', ...)) AS _row_hash
    comparable_cols  — columns fed into HASHBYTES.
    select_cols      — extra columns returned alongside the hash (e.g. PK cols).
                       Pass None for old-DB no-PK path (hash only).
    """
    cast_exprs = ", ".join(f"CAST([{c}] AS NVARCHAR(MAX))" for c in comparable_cols)
    hash_expr  = f"HASHBYTES('SHA2_256', CONCAT_WS('{HASH_DELIM}', {cast_exprs}))"

    col_part = (", ".join(f"[{c}]" for c in select_cols) + ", ") if select_cols else ""
    q = f"SELECT {col_part}{hash_expr} AS _row_hash FROM [{schema}].[{table}]"
    if where:
        q += f" WHERE {where}"
    return q


def _norm_hash(v) -> Optional[bytes]:
    """Normalise pyodbc VARBINARY result to bytes (bytes/bytearray/memoryview all unified)."""
    if v is None:
        return None
    return bytes(v)


def fetch_hashes(
    engine: sa.engine.Engine,
    table: str,
    schema: str,
    comparable_cols: list[str],
    select_cols: Optional[list[str]] = None,
    where: str = "",
) -> pd.DataFrame:
    """
    Execute build_hash_sql and return a DataFrame.
    _row_hash is always normalised to bytes so cross-connection comparisons work.
    """
    q  = build_hash_sql(table, schema, comparable_cols, select_cols=select_cols, where=where)
    df = pd.read_sql(q, engine)
    df["_row_hash"] = df["_row_hash"].apply(_norm_hash)
    return df


# ===========================================================================
# Phase-3 targeted row fetch — supports composite PK via VALUES JOIN
# ===========================================================================

def _fmt_pk_val(v) -> str:
    """Format a single PK column value for embedding in SQL."""
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if v is None:
        return "NULL"
    return str(v)


def fetch_rows_by_pk(
    engine: sa.engine.Engine,
    table: str,
    schema: str,
    cols: list[str],
    pk_cols: list[str],
    pk_tuples,
    batch_size: int = PK_BATCH_SIZE,
) -> pd.DataFrame:
    """
    Fetch full rows for a set of (composite) PK tuples.

    For a single PK col: WHERE [pk] IN (...)
    For composite PK:    JOIN (VALUES (v1a,v1b), (v2a,v2b), ...) AS keys(k1,k2)
                             ON t.[pk1]=keys.k1 AND t.[pk2]=keys.k2
    PK values are embedded as literals (they originate from the DB, not user input).
    """
    col_list = ", ".join(f"t.[{c}]" for c in cols)
    pk_list  = list(pk_tuples)

    if not pk_list:
        return pd.DataFrame(columns=cols)

    chunks = []
    total  = len(pk_list)

    for i in range(0, total, batch_size):
        batch = pk_list[i : i + batch_size]

        if len(pk_cols) == 1:
            in_clause = ", ".join(_fmt_pk_val(v[0]) for v in batch)
            q = (f"SELECT {col_list} FROM [{schema}].[{table}] t "
                 f"WHERE t.[{pk_cols[0]}] IN ({in_clause})")
        else:
            key_aliases  = ", ".join(f"k{j}" for j in range(len(pk_cols)))
            join_cond    = " AND ".join(f"t.[{pk_cols[j]}] = keys.k{j}" for j in range(len(pk_cols)))
            values_rows  = [
                "(" + ", ".join(_fmt_pk_val(v[j]) for j in range(len(pk_cols))) + ")"
                for v in batch
            ]
            values_clause = ", ".join(values_rows)
            q = (f"SELECT {col_list} "
                 f"FROM [{schema}].[{table}] t "
                 f"JOIN (VALUES {values_clause}) AS keys({key_aliases}) "
                 f"ON {join_cond}")

        chunks.append(pd.read_sql(q, engine))
        log.info("  Phase 3 fetch: %d / %d PKs retrieved", min(i + batch_size, total), total)

    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=cols)


# ===========================================================================
# Insert helper
# ===========================================================================

def insert_to_diff(
    diff_engine: sa.engine.Engine,
    df: pd.DataFrame,
    table: str,
    schema: str,
    diff_type: str,
) -> None:
    if df.empty:
        log.info("  Nothing to insert for diff_type='%s'.", diff_type)
        return

    df = df.copy()
    df["diff_type"]         = diff_type
    df["nd_extracted_date"] = pd.Timestamp("today")  # naive midnight — avoids pyodbc wchar truncation on DATETIME2

    log.info("  Inserting %d rows  diff_type='%s'", len(df), diff_type)
    df.to_sql(
        name=table,
        con=diff_engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=500,  # small batches — pyodbc fast_executemany preallocates max-width buffers
    )
    log.info("  Done inserting %d rows  diff_type='%s'.", len(df), diff_type)


# ===========================================================================
# OldDB dedup — now operates on composite PK cols
# ===========================================================================

def dedup_old_df(old_df: pd.DataFrame, pk_cols: list[str], has_extracted_date: bool) -> pd.DataFrame:
    """
    Keep one row per composite PK in OldDB hash frame.
    If nd_extracted_date is present: keep the latest. Otherwise: keep first.
    """
    if has_extracted_date and "nd_extracted_date" in old_df.columns:
        old_df = (
            old_df
            .sort_values("nd_extracted_date", ascending=False, na_position="last")
            .drop_duplicates(subset=pk_cols, keep="first")
        )
        log.info("  Deduped OLD hashes by nd_extracted_date (composite PK=%s). Rows after dedup: %d",
                 pk_cols, len(old_df))
    else:
        before = len(old_df)
        old_df = old_df.drop_duplicates(subset=pk_cols, keep="first")
        log.info("  Deduped OLD hashes arbitrarily (composite PK=%s). %d → %d rows",
                 pk_cols, before, len(old_df))
    return old_df


# ===========================================================================
# Core diff logic
# ===========================================================================

def diff_with_pk(
    new_engine:         sa.engine.Engine,
    old_engine:         sa.engine.Engine,
    diff_engine:        sa.engine.Engine,
    table:              str,
    schema:             str,
    pk_cols:            list[str],
    all_new_cols:       list[str],
    comparable_cols:    list[str],
    active_flag_filter: bool,
) -> None:
    """
    3-phase composite-PK diff.

    Phase 1  Fetch (pk_cols, _row_hash) from both DBs — SQL does the hashing.
    Phase 2  Set ops on composite PK tuples → NEW_ONLY and CHANGED sets.
    Phase 3  Fetch full rows from NewDB via VALUES JOIN for delta PKs only.
    """
    old_has_extracted_date = has_column(old_engine, table, schema, "nd_extracted_date")
    old_select = list(pk_cols) + (["nd_extracted_date"] if old_has_extracted_date else [])
    old_where  = "nd_ActiveFlag = 'Y'" if active_flag_filter else ""

    # ── Phase 1 ──────────────────────────────────────────────────────────────
    log.info("  Phase 1: Fetching hashes from NewDB ...")
    new_hashes = fetch_hashes(new_engine, table, schema, comparable_cols, select_cols=list(pk_cols))

    log.info("  Phase 1: Fetching hashes from OldDB (filter='%s') ...", old_where or "none")
    old_hashes = fetch_hashes(old_engine, table, schema, comparable_cols,
                               select_cols=old_select, where=old_where)

    log.info("  NewDB hash rows: %d  |  OldDB hash rows: %d", len(new_hashes), len(old_hashes))

    # Dedup old on composite PK (keeps latest snapshot per unique key)
    old_hashes = dedup_old_df(old_hashes, pk_cols, has_extracted_date=old_has_extracted_date)

    # ── Phase 2 — set ops on composite PK tuples ─────────────────────────────
    def to_tuple_set(df):
        return set(map(tuple, df[pk_cols].values.tolist()))

    new_pk_set = to_tuple_set(new_hashes)
    old_pk_set = to_tuple_set(old_hashes)

    new_only_pks = new_pk_set - old_pk_set
    log.info("  NEW_ONLY PKs: %d", len(new_only_pks))

    # CHANGED: in both, but hash differs — merge on composite PK, compare hashes
    old_for_merge = old_hashes[pk_cols + ["_row_hash"]].rename(columns={"_row_hash": "_hash_old"})
    new_for_merge = new_hashes[new_hashes[pk_cols[0]].isin({t[0] for t in old_pk_set})]
    # Narrow to PKs in both before merging (perf — avoids merging NEW_ONLY rows)
    new_for_merge = new_hashes.merge(
        old_hashes[pk_cols], on=pk_cols, how="inner"
    )[pk_cols + ["_row_hash"]].rename(columns={"_row_hash": "_hash_new"})

    merged = new_for_merge.merge(old_for_merge, on=pk_cols)
    changed_mask = merged["_hash_new"] != merged["_hash_old"]
    changed_pks  = set(map(tuple, merged.loc[changed_mask, pk_cols].values.tolist()))
    log.info("  CHANGED PKs: %d", len(changed_pks))

    delta_pks = new_only_pks | changed_pks
    if not delta_pks:
        log.info("  No differences found.")
        return

    # ── Phase 3 ──────────────────────────────────────────────────────────────
    log.info("  Phase 3: Fetching %d full rows from NewDB ...", len(delta_pks))
    delta_df = fetch_rows_by_pk(new_engine, table, schema, all_new_cols, pk_cols, delta_pks)

    new_only_mask = delta_df[pk_cols].apply(tuple, axis=1).isin(new_only_pks)
    changed_mask  = delta_df[pk_cols].apply(tuple, axis=1).isin(changed_pks)

    insert_to_diff(diff_engine, delta_df[new_only_mask][all_new_cols], table, schema, "NEW_ONLY")
    insert_to_diff(diff_engine, delta_df[changed_mask][all_new_cols],  table, schema, "CHANGED")


def diff_without_pk(
    new_engine:      sa.engine.Engine,
    old_engine:      sa.engine.Engine,
    diff_engine:     sa.engine.Engine,
    table:           str,
    schema:          str,
    all_new_cols:    list[str],
    comparable_cols: list[str],
) -> None:
    """
    2-phase hash-based diff (no PK).

    Phase 1a  OldDB → hashes only (32 bytes/row).
    Phase 1b  NewDB → all columns + hash in one query.
    Phase 2   Counter comparison; filter unmatched new rows → NEW_OR_CHANGED.
    """
    log.info("  [No-PK] Phase 1: Fetching OLD hashes ...")
    old_hash_df = fetch_hashes(old_engine, table, schema, comparable_cols)
    old_counter = Counter(old_hash_df["_row_hash"])
    log.info("  [No-PK] OLD distinct hashes: %d", len(old_counter))

    log.info("  [No-PK] Phase 1: Fetching NEW rows + hashes ...")
    new_df = fetch_hashes(new_engine, table, schema, comparable_cols, select_cols=all_new_cols)
    log.info("  [No-PK] NEW rows: %d", len(new_df))

    remaining = dict(old_counter)
    mask = []
    for h in new_df["_row_hash"]:
        if remaining.get(h, 0) > 0:
            remaining[h] -= 1
            mask.append(False)
        else:
            mask.append(True)

    new_or_changed = new_df[mask][all_new_cols]
    insert_to_diff(diff_engine, new_or_changed, table, schema, "NEW_OR_CHANGED")


# ===========================================================================
# Main entry point
# ===========================================================================

def run_diff(
    table_name: str,
    old_conn:   str,
    new_conn:   str,
    diff_conn:  str,
    schema:     str = "dbo",
) -> None:
    log.info("Processing table: %s", table_name)

    old_engine  = make_engine(old_conn)
    new_engine  = make_engine(new_conn)
    diff_engine = make_engine(diff_conn, fast_executemany=True)

    table = table_name

    pk_cols = get_primary_key_cols(new_engine, table, schema)
    has_pk  = len(pk_cols) > 0
    log.info("  PK cols=%s", pk_cols if has_pk else "(none)")

    all_new_cols    = get_all_columns(new_engine, table, schema)
    comparable_cols = get_comparable_columns(new_engine, old_engine, table, schema)
    col_meta        = get_table_ddl_info(new_engine, table, schema)

    log.info("  All new cols    : %s", all_new_cols)
    log.info("  Comparable cols : %s", comparable_cols)

    ensure_diff_table(diff_engine, table, schema, col_meta)

    if has_pk:
        log.info("Using PK-based diff on %s", pk_cols)
        af_filter = has_active_flag(old_engine, table, schema)
        log.info("  nd_ActiveFlag filter: %s", af_filter)
        diff_with_pk(
            new_engine, old_engine, diff_engine,
            table, schema, pk_cols,
            all_new_cols, comparable_cols,
            active_flag_filter=af_filter,
        )
    else:
        log.info("No PK – using hash-based diff")
        diff_without_pk(
            new_engine, old_engine, diff_engine,
            table, schema,
            all_new_cols, comparable_cols,
        )

    log.info("Completed table: %s", table_name)


# ===========================================================================
# CLI
# ===========================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Optimised Python port of sp_run_diff")
    parser.add_argument("--table",     required=True)
    parser.add_argument("--old_conn",  default="mssql+pyodbc://sa:ndADMIN2025@localhost:1437/mobiledoc_base_0601?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes")
    parser.add_argument("--new_conn",  default="mssql+pyodbc://sa:ndADMIN2025@localhost:1437/mobiledoc_latest?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes")
    parser.add_argument("--diff_conn", default="mssql+pyodbc://sa:ndADMIN2025@localhost:1437/mobiledoc_diff_staging?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes")
    parser.add_argument("--schema",    default="dbo")
    args = parser.parse_args()

    run_diff(
        table_name = args.table,
        old_conn   = args.old_conn,
        new_conn   = args.new_conn,
        diff_conn  = args.diff_conn,
        schema     = args.schema,
    )
