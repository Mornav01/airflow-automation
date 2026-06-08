"""
MSSQL → MySQL incremental sync engine (Airflow service).

- Auto-detects date columns for incremental sync.
- Tracks last sync per table in a tracking table.
- Truncates incremental table before each sync so it only has today's data.
- Pulls only new/changed rows from MSSQL, inserts into MySQL.
- No credentials here: engines are passed in (from Airflow connections).
"""

from datetime import datetime
import traceback

import pandas as pd
from sqlalchemy import text

# -----------------------------------------------------
# CONFIG
# -----------------------------------------------------

DATE_CANDIDATES = [
    "Created",
    "ModifiedDate",
    "DTS_Export_Date",
    "DB_Create_Date",
    "Mshdate",
    "OrderRequestedDate",
    "DB_Updated_Date",
    "LastModified",
]

GET_TABLES_EXTRA_FILTER = (
    "AND name NOT LIKE '%%AUDIT%%' AND name NOT LIKE '%%Log%%' "
    "AND name NOT LIKE '%%Backup%%' AND name NOT LIKE '%%bkp%%'"
)


# -----------------------------------------------------
# TRACKING TABLE
# -----------------------------------------------------


def ensure_tracking_table(eng_mysql, tracking_schema=None, tracking_table=None):
    schema = tracking_schema
    tbl = tracking_table
    with eng_mysql.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{schema}`"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{schema}`.`{tbl}` (
                table_name VARCHAR(200) PRIMARY KEY,
                last_sync DATETIME NULL
            )
        """))
        conn.commit()


def get_last_sync(eng_mysql, table_name, tracking_schema=None, tracking_table=None):
    schema = tracking_schema
    tbl = tracking_table
    lookup_name = "DOCUEMNT" if table_name == "DOCDATA" else table_name
    sql = text(f"""
        SELECT last_sync
        FROM `{schema}`.`{tbl}`
        WHERE table_name = :t
    """)
    with eng_mysql.connect() as conn:
        row = conn.execute(sql, {"t": lookup_name}).fetchone()
        return row[0] if row else None


def update_last_sync(eng_mysql, table_name, dt, tracking_schema=None, tracking_table=None):
    schema = tracking_schema
    tbl = tracking_table
    sql = text(f"""
        INSERT INTO `{schema}`.`{tbl}` (table_name, last_sync)
        VALUES (:t, :dt)
        ON DUPLICATE KEY UPDATE last_sync = VALUES(last_sync)
    """)
    with eng_mysql.begin() as conn:
        conn.execute(sql, {"t": table_name, "dt": dt})


# -----------------------------------------------------
# METADATA HELPERS
# -----------------------------------------------------


def get_tables(eng_mssql, extra_filter=None):
    filter_clause = extra_filter or GET_TABLES_EXTRA_FILTER
    sql = text(f"""
        SELECT name
        FROM sys.tables
        WHERE is_ms_shipped = 0 {filter_clause}
        ORDER BY name
    """)
    with eng_mssql.connect() as conn:
        rows = conn.execute(sql).fetchall()
        return [r[0] for r in rows]


def get_columns(eng_mssql, table):
    sql = text("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :t
    """)
    with eng_mssql.connect() as conn:
        df = pd.read_sql(sql, conn, params={"t": table})
        return df["COLUMN_NAME"].tolist()


def detect_date_column(cols, candidates=None):
    candidates = candidates or DATE_CANDIDATES
    normalized_cols = [c.strip().lower() for c in cols]
    for col in candidates:
        if col.lower() in normalized_cols:
            idx = normalized_cols.index(col.lower())
            return cols[idx]
    return None


def get_pk_columns(eng_mssql, table):
    """Return list of PK column names for a table in MSSQL (empty list if no PK)."""
    sql = text("""
        SELECT c.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
          ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
         AND tc.TABLE_NAME     = c.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
          AND tc.TABLE_NAME = :t
        ORDER BY c.ORDINAL_POSITION
    """)
    with eng_mssql.connect() as conn:
        rows = conn.execute(sql, {"t": table}).fetchall()
        return [r[0] for r in rows]


# -----------------------------------------------------
# SCHEMA / TABLE CREATION
# -----------------------------------------------------


def ensure_incremental_db(eng_mysql, incremental_schema=None):
    schema = incremental_schema
    with eng_mysql.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{schema}`"))
        conn.commit()


def ensure_incremental_table(eng_mysql, table, df, incremental_schema=None):
    """Create MySQL table if missing with types inferred from DataFrame."""
    if df.empty:
        return
    schema = incremental_schema
    columns = []
    for c, dtype in zip(df.columns, df.dtypes):
        dtype_str = str(dtype).lower()
        if "int" in dtype_str:
            t = "BIGINT"
        elif "float" in dtype_str or "decimal" in dtype_str or "numeric" in dtype_str:
            series = df[c].dropna()
            if not series.empty and series.apply(lambda x: float(x).is_integer()).all():
                max_val = series.astype(int).abs().max()
                t = "BIGINT" if max_val <= 9223372036854775807 else "DECIMAL(38,0)"
            else:
                t = "DECIMAL(38,10)"
        elif "datetime" in dtype_str:
            t = "DATETIME"
        elif "object" in dtype_str:
            non_null = df[c].dropna()
            if non_null.empty:
                t = "LONGTEXT"
            else:
                sample_size = min(20, len(non_null))
                samples = non_null.iloc[:sample_size]
                if samples.apply(lambda x: isinstance(x, (bytes, bytearray))).all():
                    t = "LONGBLOB"
                elif samples.apply(lambda x: isinstance(x, str)).all():
                    t = "LONGTEXT"
                else:
                    t = "LONGBLOB"
        else:
            t = "LONGTEXT"
        columns.append(f"`{c}` {t}")
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` (
        {', '.join(columns)}
    )
    """
    with eng_mysql.connect() as conn:
        conn.execute(text(sql))
        conn.commit()


# -----------------------------------------------------
# TRUNCATE INCREMENTAL TABLE
# -----------------------------------------------------


def truncate_incremental_table(eng_mysql, table, incremental_schema=None):
    """
    Truncates the incremental table before each sync so it only contains
    today's fresh data from MSSQL — not accumulated rows from previous runs.
    """
    schema = incremental_schema
    with eng_mysql.connect() as conn:
        exists = conn.execute(text(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_name = '{table}'
        """)).scalar()

        if exists:
            conn.execute(text(f"TRUNCATE TABLE `{schema}`.`{table}`"))
            conn.commit()
            print(f"Truncated incremental table: {schema}.{table}")
        else:
            print(f"Incremental table does not exist yet, skipping truncate: {schema}.{table}")


# -----------------------------------------------------
# SYNC LOGIC
# -----------------------------------------------------


def fetch_incremental(eng_mssql, table, date_col, last_sync, pk_col=None, max_pk=None):
    """Pull incremental (or full) rows from MSSQL."""
    with eng_mssql.connect() as conn:
        if date_col is None:
            if table in ("DOCDATA", "DOCDATA2", "DOCCONTB"):
                sql = text(
                    "SELECT * FROM [{}] WHERE sdid IN (SELECT sdid FROM DOCUMENT WHERE DTS_EXPORT_DATE >= '2025-12-26')"
                    .format(table)
                )
            elif table in ("REL_OBS_EXT_CODE"):
                sql = text(
                    "SELECT * FROM [{}] WHERE OBSID IN (SELECT OBSID FROM OBS WHERE DTS_EXPORT_DATE >= '2025-12-26')"
                    .format(table)
                )
            elif pk_col is not None and max_pk is not None:
                # No date column but PK exists: fetch only rows newer than what's already in MySQL
                sql = text(f"SELECT * FROM [{table}] WHERE [{pk_col}] > :pk")
                return pd.read_sql(sql, conn, params={"pk": max_pk})
            else:
                # No date column, no PK (or original schema unavailable) — full load
                sql = text(f"SELECT * FROM [{table}]")
            return pd.read_sql(sql, conn)
        if last_sync:
            sql = text(f"""
                SELECT * FROM [{table}]
                WHERE [{date_col}] > :dt
            """)
            return pd.read_sql(sql, conn, params={"dt": last_sync})
        sql = text(f"SELECT * FROM [{table}]")
        return pd.read_sql(sql, conn)


def insert_incremental(eng_mysql, table, df, incremental_schema=None):
    if df.empty:
        return 0
    schema = incremental_schema
    ensure_incremental_table(eng_mysql, table, df, incremental_schema=schema)
    conn = None
    trans = None
    try:
        conn = eng_mysql.connect()
        trans = conn.begin()
        df.to_sql(
            table,
            conn,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=2000,
            method=None,
        )
        trans.commit()
        return len(df)
    except Exception as e:
        if trans is not None:
            try:
                trans.rollback()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        eng_mysql.dispose()
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def process_table(
    eng_mssql,
    eng_mysql,
    table,
    incremental_schema=None,
    tracking_schema=None,
    tracking_table=None,
    date_candidates=None,
    log_fn=None,
    original_schema=None,
):
    """Sync a single table (incremental or full). Returns number of rows inserted."""
    if not incremental_schema:
        raise ValueError("incremental_schema is required")
    if not tracking_schema:
        raise ValueError("tracking_schema is required")
    if not tracking_table:
        raise ValueError("tracking_table is required")
    inc_schema = incremental_schema
    tr_schema  = tracking_schema
    tr_table   = tracking_table
    log        = log_fn or print

    cols      = get_columns(eng_mssql, table)
    date_col  = detect_date_column(cols, candidates=date_candidates)
    last_sync = get_last_sync(
        eng_mysql, table,
        tracking_schema=tr_schema,
        tracking_table=tr_table
    )
    log(f"{table}: last_sync={last_sync}, date_col={date_col}")

    # ── PK-based incremental for tables with no date column ───────────────
    # Mirrors add_nd_auto_increment_id_step: uses original_schema as the
    # source of truth for what's already been synced. Fetches only rows
    # whose PK exceeds the current max in the original (historical) table.
    pk_col = None
    max_pk = None
    if date_col is None and original_schema:
        pk_cols = get_pk_columns(eng_mssql, table)
        if len(pk_cols) == 1:
            pk_col = pk_cols[0]
            try:
                with eng_mysql.connect() as conn:
                    max_pk = conn.execute(text(
                        f"SELECT COALESCE(MAX(`{pk_col}`), 0) FROM `{original_schema}`.`{table}`"
                    )).scalar()
                log(f"{table}: No date col — PK-based on [{pk_col}] > {max_pk}")
            except Exception:
                pk_col = None
                max_pk = None
                log(f"{table}: No date col, original table missing — FULL LOAD")
        else:
            log(f"{table}: No date col, no single PK — FULL LOAD")

    # ── TRUNCATE incremental table before every sync ──────────────────────
    # Ensures incremental only has TODAY's fresh data, not accumulated rows
    truncate_incremental_table(eng_mysql, table, incremental_schema=inc_schema)

    df = fetch_incremental(eng_mssql, table, date_col, last_sync, pk_col=pk_col, max_pk=max_pk)

    inserted = insert_incremental(eng_mysql, table, df, incremental_schema=inc_schema)

    if date_col and not df.empty:
        max_dt = df[date_col].max()
    else:
        max_dt = datetime.now()

    update_last_sync(
        eng_mysql, table, max_dt,
        tracking_schema=tr_schema,
        tracking_table=tr_table
    )
    log(f"{table}: Inserted {inserted} rows, last_sync -> {max_dt}")
    return inserted


def run_sync(
    eng_mssql,
    eng_mysql,
    tables=None,
    incremental_schema=None,
    tracking_schema=None,
    tracking_table=None,
    date_candidates=None,
    get_tables_extra_filter=None,
    log_fn=None,
    original_schema=None,
):
    """
    Run full incremental sync: all tables (or given list) from MSSQL to MySQL.
    Each table's incremental schema is truncated before sync so only
    today's data is present — no accumulation across runs.
    """
    if not incremental_schema:
        raise ValueError("incremental_schema is required")
    if not tracking_schema:
        raise ValueError("tracking_schema is required")
    if not tracking_table:
        raise ValueError("tracking_table is required")
    inc_schema = incremental_schema
    tr_schema  = tracking_schema
    tr_table   = tracking_table
    log        = log_fn or print

    ensure_incremental_db(eng_mysql, incremental_schema=inc_schema)
    ensure_tracking_table(eng_mysql, tracking_schema=tr_schema, tracking_table=tr_table)

    if tables is None:
        tables = get_tables(eng_mssql, extra_filter=get_tables_extra_filter)

    total_rows = 0
    for t in tables:
        try:
            total_rows += process_table(
                eng_mssql,
                eng_mysql,
                t,
                incremental_schema=inc_schema,
                tracking_schema=tr_schema,
                tracking_table=tr_table,
                date_candidates=date_candidates,
                log_fn=log,
                original_schema=original_schema,
            )
        except Exception as e:
            log(f"ERROR in {t}: {e}")
            traceback.print_exc()
            raise
    return total_rows


# -----------------------------------------------------
# ND_AUTO_INCREMENT_ID STEP (post-extraction)
# -----------------------------------------------------


def add_nd_auto_increment_id_step(
    eng_mysql,
    tables,
    original_schema=None,
    incremental_schema=None,
    log_fn=None,
):
    """
    For each table in the incremental schema: ensure column nd_auto_increment_id exists,
    then set it to a running sequence starting at (original table row count + 1).
    """
    orig = original_schema
    inc  = incremental_schema
    log  = log_fn or print
    report = []

    with eng_mysql.connect() as conn:
        for table in tables:
            table  = table.strip()
            status = "Success"
            original_count = 0
            try:
                # STEP 0: Skip if incremental table doesn't exist (0 rows synced this run)
                table_exists = conn.execute(text("""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = :schema AND table_name = :tbl
                """), {"schema": inc, "tbl": table}).scalar()
                if not table_exists:
                    log(f"{table}: incremental table does not exist — skipping nd_auto_increment_id step")
                    report.append({"table_name": table, "original_count": 0, "status": "Skipped (no table)"})
                    continue

                # STEP 1: Get count from original schema
                try:
                    count_sql = text(f"SELECT COUNT(*) FROM `{orig}`.`{table}`")
                    original_count = conn.execute(count_sql).scalar()
                except Exception:
                    original_count = 0

                # STEP 2: Check if column exists
                check_sql = text("""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = :schema
                      AND TABLE_NAME = :tbl
                      AND COLUMN_NAME = 'nd_auto_increment_id'
                """)
                col_exists = conn.execute(
                    check_sql, {"schema": inc, "tbl": table}
                ).scalar()

                # STEP 3: Add column if missing
                if not col_exists:
                    log(f"Adding column nd_auto_increment_id to {inc}.{table}...")
                    conn.execute(text(f"""
                        ALTER TABLE `{inc}`.`{table}`
                        ADD COLUMN nd_auto_increment_id BIGINT
                    """))
                    conn.commit()

                # STEP 4: Backfill sequence starting at original_count + 1
                conn.execute(text("SET @row_num = :start"), {"start": original_count})
                update_sql = text(f"""
                    UPDATE `{inc}`.`{table}`
                    SET nd_auto_increment_id = (@row_num := @row_num + 1)
                    ORDER BY (SELECT NULL)
                """)
                conn.execute(update_sql)
                conn.commit()
                log(f"{table}: nd_auto_increment_id set (starting at {original_count + 1})")
            except Exception as e:
                status = f"Failed: {str(e)}"
                log(f"Error on {table}: {e}")
                try:
                    conn.rollback()
                except Exception:
                    pass

            report.append({
                "table_name":     table,
                "original_count": original_count,
                "status":         status,
            })

    return report
