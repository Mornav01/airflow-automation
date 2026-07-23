# merge_engine.py
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import logging
import traceback
from typing import List, Dict
import pandas as pd


BATCH_INSERT_CHUNK = 1000
DEACTIVATE_CHUNK   = 5000

_NULL_LIKE_STRINGS = ["0000-00-00 00:00:00", "0000-00-00", "NaT", "nat", "NaN"]


# ──────────────────────────────────────────────────────────────────────────────
# SANITIZE
# ──────────────────────────────────────────────────────────────────────────────

def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.astype(object)
    df = df.where(pd.notna(df), None)
    df = df.replace(_NULL_LIKE_STRINGS, None)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# UNIFIED RAW-CURSOR BULK INSERT
#   MSSQL (pyodbc):  placeholder = ?    cur.fast_executemany = True
#   MySQL (pymysql): placeholder = %s   fast_executemany not set (not supported)
# ──────────────────────────────────────────────────────────────────────────────

def _raw_bulk_insert(engine: Engine, table: str, df: pd.DataFrame,
                     db_type: str, db: str = None,
                     chunk: int = BATCH_INSERT_CHUNK):
    if df.empty:
        logging.info("[_raw_bulk_insert] [%s] Empty DataFrame — skipping", table)
        return

    df   = sanitize_df(df)
    cols = list(df.columns)

    if db_type == "mssql":
        col_sql      = ",".join(f"[{c}]" for c in cols)
        placeholders = ",".join(["?" for _ in cols])
        sql          = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"
    else:
        col_sql      = ",".join([f"`{c}`" for c in cols])
        placeholders = ",".join(["%s" for _ in cols])
        sql          = f"INSERT INTO `{db}`.`{table}` ({col_sql}) VALUES ({placeholders})"

    rows      = [tuple(v.to_pytimedelta() if isinstance(v, pd.Timedelta) else v
                       for v in r)
                 for r in df.itertuples(index=False, name=None)]
    total     = len(rows)
    n_batches = (total + chunk - 1) // chunk

    logging.info("[_raw_bulk_insert] [%s] Inserting %d rows — %d batch(es) of %d | db_type=%s",
                 table, total, n_batches, chunk, db_type)

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        if db_type == "mssql":
            cur.fast_executemany = True     # pyodbc only — not available on pymysql

        for i in range(0, total, chunk):
            batch_num  = i // chunk + 1
            batch_rows = rows[i:i + chunk]
            cur.executemany(sql, batch_rows)
            raw.commit()
        cur.close()
    except Exception as e:
        logging.error("[_raw_bulk_insert] [%s] ERROR: %s — rolling back", table, e)
        raw.rollback()
        raise
    finally:
        raw.close()


# ──────────────────────────────────────────────────────────────────────────────
# UNIFIED SET-BASED DEACTIVATE
#   MSSQL: #tmp_deact + single UPDATE JOIN  (one index-seek, no row-by-row)
#   MySQL: MEMORY temp table + single UPDATE JOIN  (same pattern)
# ──────────────────────────────────────────────────────────────────────────────

def _raw_deactivate(engine: Engine, table: str, pk_df: pd.DataFrame,
                    pk_cols: List[str], db_type: str, db: str = None,
                    chunk: int = DEACTIVATE_CHUNK):
    if pk_df.empty:
        logging.info("[_raw_deactivate] [%s] No PKs to deactivate — skipping", table)
        return

    pk_only = pk_df[pk_cols].drop_duplicates().astype(str)
    total   = len(pk_only)

    logging.info("[_raw_deactivate] [%s] Starting deactivate — %d unique PK rows | db_type=%s | chunk=%d",
                 table, total, db_type, chunk)

    if db_type == "mssql":
        _deactivate_mssql(engine, table, pk_only, pk_cols, chunk)
    else:
        _deactivate_mysql(engine, db, table, pk_only, pk_cols, chunk)



def _deactivate_mssql(engine: Engine, table: str, pk_df: pd.DataFrame,
                      pk_cols: List[str], chunk: int):
    col_defs      = ", ".join([f"[{c}] NVARCHAR(255) NOT NULL" for c in pk_cols])
    pk_constraint = ", ".join(f"[{c}]" for c in pk_cols)
    ins_ph        = ", ".join(["?" for _ in pk_cols])
    ins_sql       = f"INSERT INTO #tmp_deact ({pk_constraint}) VALUES ({ins_ph})"
    join_cond     = " AND ".join([f"m.[{c}] = t.[{c}]" for c in pk_cols])

    rows  = [tuple(r) for r in pk_df.itertuples(index=False, name=None)]
    total = len(rows)

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.fast_executemany = True     # pyodbc only

        cur.execute(f"""
            CREATE TABLE #tmp_deact (
                {col_defs},
                PRIMARY KEY ({pk_constraint})
            )
        """)

        for i in range(0, total, chunk):
            batch_rows = rows[i:i + chunk]
            cur.executemany(ins_sql, batch_rows)

        cur.execute(f"""
            UPDATE m
            SET m.nd_ActiveFlag = 'N'
            FROM {table} m
            JOIN #tmp_deact t ON {join_cond}
            WHERE m.nd_ActiveFlag = 'Y'
        """)
        logging.info("[_deactivate_mssql] [%s] deactivated %d rows", table, cur.rowcount)

        cur.execute("DROP TABLE #tmp_deact")
        raw.commit()
        cur.close()
    except Exception as e:
        logging.error("[_deactivate_mssql] [%s] ERROR: %s — rolling back", table, e)
        raw.rollback()
        raise
    finally:
        raw.close()


def _deactivate_mysql(engine: Engine, db: str, table: str,
                      pk_df: pd.DataFrame, pk_cols: List[str], chunk: int):
    col_defs      = ", ".join([f"`{c}` VARCHAR(255) NOT NULL" for c in pk_cols])
    pk_constraint = ", ".join([f"`{c}`" for c in pk_cols])
    tmp           = f"_tmp_deact_{table}"
    ins_ph        = ", ".join(["%s" for _ in pk_cols])
    ins_sql       = f"INSERT INTO `{tmp}` ({pk_constraint}) VALUES ({ins_ph})"
    join_cond     = " AND ".join([f"m.`{c}` = t.`{c}`" for c in pk_cols])

    rows  = [tuple(r) for r in pk_df.itertuples(index=False, name=None)]
    total = len(rows)

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()

        cur.execute(f"""
            CREATE TEMPORARY TABLE `{tmp}` (
                {col_defs},
                PRIMARY KEY ({pk_constraint})
            ) ENGINE=MEMORY
        """)

        for i in range(0, total, chunk):
            batch_rows = rows[i:i + chunk]
            cur.executemany(ins_sql, batch_rows)

        cur.execute(f"""
            UPDATE `{db}`.`{table}` m
            JOIN `{tmp}` t ON {join_cond}
            SET m.nd_ActiveFlag = 'N'
            WHERE m.nd_ActiveFlag = 'Y'
        """)
        logging.info("[_deactivate_mysql] [%s] deactivated %d rows", table, cur.rowcount)

        cur.execute(f"DROP TEMPORARY TABLE `{tmp}`")
        raw.commit()
        cur.close()
    except Exception as e:
        logging.error("[_deactivate_mysql] [%s] ERROR: %s — rolling back", table, e)
        raw.rollback()
        raise
    finally:
        raw.close()


# ──────────────────────────────────────────────────────────────────────────────
# ENSURE INDEX ON (nd_ActiveFlag, pk_cols)
#   MSSQL: filtered non-clustered index WHERE nd_ActiveFlag='Y'
#   MySQL:  composite index (filtered indexes not supported)
# ──────────────────────────────────────────────────────────────────────────────

def _ensure_active_index(engine: Engine, db: str, table: str,
                         pk_cols: List[str], db_type: str):
    index_name  = f"IX_{table}_active_pk"
    index_cols  = ["nd_ActiveFlag"] + pk_cols

    if db_type == "mssql":
        with engine.connect() as conn:
            exists = conn.execute(text("""
                SELECT COUNT(*) FROM sys.indexes
                WHERE object_id = OBJECT_ID(:tbl) AND name = :idx
            """), {"tbl": table, "idx": index_name}).scalar()

        if exists:
            logging.info("[_ensure_active_index] [%s] Index %s already exists — skipping", table, index_name)
            return

        col_list = ", ".join(f"[{c}]" for c in index_cols)
        with engine.begin() as conn:
            conn.execute(text(f"""
                CREATE NONCLUSTERED INDEX {index_name}
                ON {table} ({col_list})
                WHERE nd_ActiveFlag = 'Y'
            """))
        logging.info("[_ensure_active_index] [%s] Created filtered MSSQL index %s ON (%s) WHERE nd_ActiveFlag='Y'",
                     table, index_name, col_list)

    else:  # mysql
        with engine.connect() as conn:
            exists = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.statistics
                WHERE table_schema = :db AND table_name = :tbl AND index_name = :idx
            """), {"db": db, "tbl": table, "idx": index_name}).scalar()

        if exists:
            logging.info("[_ensure_active_index] [%s] Index %s already exists — skipping", table, index_name)
            return

        col_list = ", ".join([f"`{c}`" for c in index_cols])
        with engine.begin() as conn:
            conn.execute(text(f"""
                ALTER TABLE `{db}`.`{table}`
                ADD INDEX {index_name} ({col_list})
            """))
        logging.info("[_ensure_active_index] [%s] Created MySQL index %s ON (%s)",
                     table, index_name, col_list)


# ──────────────────────────────────────────────────────────────────────────────
# MERGE ENGINE
# ──────────────────────────────────────────────────────────────────────────────

class MergeEngine:
    """
    Generic incremental merge engine for MySQL (pymysql) / MSSQL (pyodbc).
    Airflow should ONLY call run_table().

    Connection string format:
        MySQL:  mysql+pymysql://user:pass@host/dbname
        MSSQL:  mssql+pyodbc://user:pass@host/dbname?driver=ODBC+Driver+17+for+SQL+Server
    """

    def __init__(self, db_type: str, main_conn: str, diff_conn: str, diffmerged_conn: str,
                 pool_size: int = 8):
        self.db_type = db_type.lower()
        if self.db_type not in ("mysql", "mssql"):
            raise ValueError("db_type must be mysql or mssql")

        # fast_executemany is set directly on raw cursors inside helpers —
        # not at engine level. pymysql does not support the engine-level flag.
        _pool_kw = dict(pool_pre_ping=True, pool_size=pool_size, max_overflow=2, pool_recycle=1800)
        self.main_engine       = create_engine(main_conn,       **_pool_kw)
        self.diff_engine       = create_engine(diff_conn,       **_pool_kw)
        self.diffmerged_engine = create_engine(diffmerged_conn, **_pool_kw)

        self.main_db       = self._extract_db(main_conn)
        self.diff_db       = self._extract_db(diff_conn)
        self.diffmerged_db = self._extract_db(diffmerged_conn)

        logging.info("MAIN DB = %s", self.main_db)
        logging.info("DIFF DB = %s", self.diff_db)
        logging.info("DIFFMERGED DB = %s", self.diffmerged_db)

    def dispose(self):
        self.main_engine.dispose()
        self.diff_engine.dispose()
        self.diffmerged_engine.dispose()

    # ──────────────────────────────────────────────────────────────────────────
    # PUBLIC ENTRY POINT
    # ──────────────────────────────────────────────────────────────────────────

    def run_table(self, table: str, pk_cols: List[str], default_date: str) -> Dict:
        """This is the ONLY method Airflow should call."""
        try:
            self._log(f"START MERGE: {table}")

            self._step_0_skip_if_already_processed(table)
            self._step_1_validate_main_table(table)
            self._step_2_prepare_main_table(table, pk_cols, default_date)
            self._step_3_validate_diff_table(table)
            self._step_4_prepare_diff_table(table, pk_cols)

            updated_df, new_df = self._step_5_compare_and_classify(table, pk_cols)

            updated_insert_df, insert_df_new = self._step_6_merge_into_main(
                table, pk_cols, updated_df, new_df
            )
            diff_df_inserted_snapshot = self._step_7_merge_into_diffmergedDB(
                table, pk_cols, updated_insert_df, insert_df_new
            )

            self._log(f"SUCCESS MERGE: {table}")
            return {
                "success": True,
                "table": table,
                "updated_rows": len(updated_df),
                "new_rows": len(new_df),
                "diffMerged_inserted_rows": len(diff_df_inserted_snapshot),
                "message": "Merge completed successfully"
            }

        except Exception as e:
            return {
                "success": False,
                "table": table,
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 0 – Skip if already processed
    # ──────────────────────────────────────────────────────────────────────────

    def _step_0_skip_if_already_processed(self, table: str):
        self._log("Step 0: skip check (no-op)")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 1 – Validate MAIN table
    # ──────────────────────────────────────────────────────────────────────────

    def _step_1_validate_main_table(self, table: str):
        self._log("Step 1: validate MAIN table")
        with self.main_engine.connect() as conn:
            if self.db_type == "mysql":
                q = text("""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = :db AND table_name = :tbl
                """)
                exists = conn.execute(q, {"db": self.main_db, "tbl": table}).scalar()
            else:
                q = text("""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = :tbl
                """)
                exists = conn.execute(q, {"tbl": table}).scalar()
            if exists == 0:
                raise Exception(f"MAIN table not found: {table}")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 2 – Prepare MAIN
    # ──────────────────────────────────────────────────────────────────────────

    def _step_2_prepare_main_table(self, table: str, pk_cols: List[str], default_date: str):
        self._log("Step 2: prepare MAIN table")
        self._drop_primary_key(table)
        self._ensure_nd_auto_increment_main(table, pk_cols)
        self._ensure_nd_extracted_date(table, default_date)
        self._ensure_active_flag(self.main_engine, self.main_db, table)
        if pk_cols:
            _ensure_active_index(self.main_engine, self.main_db, table, pk_cols, self.db_type)
            self._normalize_active_by_pk(self.main_engine, self.main_db, table, pk_cols)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 3 – Validate DIFF table
    # ──────────────────────────────────────────────────────────────────────────

    def _step_3_validate_diff_table(self, table: str):
        self._log("Step 3: validate DIFF table")
        with self.diff_engine.connect() as conn:
            if self.db_type == "mysql":
                q = text("""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = :db AND table_name = :tbl
                """)
                exists = conn.execute(q, {"db": self.diff_db, "tbl": table}).scalar()
            else:
                q = text("""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = :tbl
                """)
                exists = conn.execute(q, {"tbl": table}).scalar()
            if exists == 0:
                raise Exception(f"DIFF table not found: {table}")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 4 – Prepare DIFF
    # ──────────────────────────────────────────────────────────────────────────

    def _step_4_prepare_diff_table(self, table: str, pk_cols: List[str]):
        self._log("Step 4: prepare DIFF table")
        self._ensure_nd_auto_increment_diff(table)
        self._populate_diff_nd_from_main(table, pk_cols)
        self._ensure_active_flag(self.diff_engine, self.diff_db, table)
        if pk_cols:
            _ensure_active_index(self.diff_engine, self.diff_db, table, pk_cols, self.db_type)
            self._normalize_active_by_pk(self.diff_engine, self.diff_db, table, pk_cols)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 5 – Compare & classify
    # ──────────────────────────────────────────────────────────────────────────

    def _step_5_compare_and_classify(self, table: str, pk_cols: List[str]):
        self._log("Step 5: compare MAIN vs DIFF")

        if self.db_type == "mysql":
            diff_df = pd.read_sql(
                f"SELECT * FROM `{self.diff_db}`.`{table}`",
                self.diff_engine
            )
            if not pk_cols:
                return pd.DataFrame(), diff_df
            main_keys = pd.read_sql(
                f"SELECT {','.join(pk_cols)} FROM `{self.main_db}`.`{table}` WHERE nd_ActiveFlag='Y'",
                self.main_engine
            )
        else:
            with self.diff_engine.connect() as conn:
                diff_df = pd.read_sql(f"SELECT * FROM {self.diff_db}.dbo.{table}", conn)
            if not pk_cols:
                return pd.DataFrame(), diff_df
            main_keys = pd.read_sql(
                f"SELECT {','.join(f'[{c}]' for c in pk_cols)} FROM {self.main_db}.dbo.{table} WHERE nd_ActiveFlag='Y'",
                self.main_engine
            )

        self._log(f"Step 5: diff_df={len(diff_df)} rows | main active keys={len(main_keys)} rows")

        def make_key(df: pd.DataFrame) -> pd.Series:
            result = df[pk_cols[0]].astype(str)
            for col in pk_cols[1:]:
                result = result + "||" + df[col].astype(str)
            return result

        main_set        = set(make_key(main_keys))
        diff_df["_key"] = make_key(diff_df)

        updated_df = diff_df[diff_df["_key"].isin(main_set)].copy()
        new_df     = diff_df[~diff_df["_key"].isin(main_set)].copy()

        self._log(f"Step 5: classified → updated={len(updated_df)} | new={len(new_df)}")
        return updated_df, new_df

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 6 – Merge into MAIN
    # ──────────────────────────────────────────────────────────────────────────

    def _step_6_merge_into_main(self, table: str, pk_cols: list,
                                 updated_rows_df: pd.DataFrame,
                                 new_rows_df: pd.DataFrame):
        """
        TX1 — deactivate old rows  (temp table + single set-based UPDATE JOIN)
        TX2 — insert updated rows  (raw cursor bulk insert)
        TX3 — insert new rows      (raw cursor bulk insert)
        """
        self._log(f"Step 6: merge into MAIN ({table})")

        # fetch main column order
        with self.main_engine.connect() as conn:
            if self.db_type == "mysql":
                sql = text("""
                    SELECT COLUMN_NAME FROM information_schema.columns
                    WHERE table_schema = :db AND table_name = :tbl
                    ORDER BY ordinal_position
                """)
                result = conn.execute(sql, {"db": self.main_db, "tbl": table}).fetchall()
            else:
                sql = text("""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = :tbl
                    ORDER BY ORDINAL_POSITION
                """)
                result = conn.execute(sql, {"tbl": table}).fetchall()

        main_columns = [r[0] for r in result]
        if not main_columns:
            raise Exception("FATAL: main_columns is empty.")

        self._log(f"Step 6: main_columns count={len(main_columns)}")

        # fill any missing columns with None
        for df in [updated_rows_df, new_rows_df]:
            for c in main_columns:
                if c not in df.columns:
                    df[c] = None

        updated_insert_df = (
            updated_rows_df[main_columns].copy() if not updated_rows_df.empty else pd.DataFrame()
        )
        insert_df_new = (
            new_rows_df[main_columns].copy() if not new_rows_df.empty else pd.DataFrame()
        )

        # TX1 — deactivate
        if not updated_insert_df.empty:
            _raw_deactivate(
                self.main_engine, table, updated_insert_df,
                pk_cols, self.db_type, self.main_db
            )
            self._log(f"Step 6 TX1: deactivated {len(updated_insert_df)} rows")
        else:
            self._log("Step 6 TX1: no rows to deactivate — skipping")

        # TX2 — insert updated rows
        if not updated_insert_df.empty:
            _raw_bulk_insert(
                self.main_engine, table, updated_insert_df,
                self.db_type, self.main_db
            )
            self._log(f"Step 6 TX2: inserted {len(updated_insert_df)} updated rows")
        else:
            self._log("Step 6 TX2: no updated rows to insert — skipping")

        # TX3 — insert new rows
        if not insert_df_new.empty:
            _raw_bulk_insert(
                self.main_engine, table, insert_df_new,
                self.db_type, self.main_db
            )
            self._log(f"Step 6 TX3: inserted {len(insert_df_new)} new rows")
        else:
            self._log("Step 6 TX3: no new rows to insert — skipping")

        return updated_insert_df, insert_df_new

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 7 – Persist into DiffMergedDB
    # ──────────────────────────────────────────────────────────────────────────

    def _step_7_merge_into_diffmergedDB(self, table: str, pk_cols: list,
                                         updated_insert_df: pd.DataFrame,
                                         insert_df_new: pd.DataFrame):
        self._log(f"STEP 7: Persisting diff into DiffMergedDB for {table}")

        dfs = [df for df in [updated_insert_df, insert_df_new]
               if df is not None and not df.empty]
        if not dfs:
            self._log("STEP 7: No rows to persist into DiffMergedDB — skipping")
            return pd.DataFrame()

        final_df = pd.concat(dfs, ignore_index=True)
        self._log(f"STEP 7: {len(final_df)} rows to insert into DiffMergedDB.{table}")

        # create table if not exists
        with self.diffmerged_engine.begin() as conn:
            if self.db_type == "mysql":
                conn.execute(text(
                    f"CREATE TABLE IF NOT EXISTS `{self.diffmerged_db}`.`{table}` "
                    f"LIKE `{self.main_db}`.`{table}`"
                ))
            else:
                conn.execute(text(f"""
                    IF OBJECT_ID('{table}', 'U') IS NULL
                    BEGIN
                        SELECT TOP 0 * INTO {table}
                        FROM {self.main_db}.dbo.{table}
                    END
                """))

        _raw_bulk_insert(
            self.diffmerged_engine, table, final_df,
            self.db_type, self.diffmerged_db
        )

        self._log(f"STEP 7 COMPLETE: Inserted {len(final_df)} rows into DiffMergedDB.{table}")
        return final_df

    # ──────────────────────────────────────────────────────────────────────────
    # LOW-LEVEL HELPERS
    # ──────────────────────────────────────────────────────────────────────────

    def _extract_db(self, conn_str: str) -> str:
        return conn_str.split("/")[-1].split("?")[0]

    def _log(self, msg: str):
        logging.info("[MergeEngine] %s", msg)

    def _column_exists(self, engine: Engine, db: str, table: str, column: str) -> bool:
        with engine.connect() as conn:
            if self.db_type == "mysql":
                q = text("""
                    SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_schema = :db AND table_name = :tbl AND column_name = :col
                """)
                return conn.execute(q, {"db": db, "tbl": table, "col": column}).scalar() > 0
            else:
                q = text("""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = :tbl AND COLUMN_NAME = :col
                """)
                return conn.execute(q, {"tbl": table, "col": column}).scalar() > 0

    def _drop_primary_key(self, table: str):
        if self.db_type != "mysql":
            return
        with self.main_engine.begin() as conn:
            has_pk = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
                WHERE table_schema = :db AND table_name = :tbl
                AND constraint_type = 'PRIMARY KEY'
            """), {"db": self.main_db, "tbl": table}).scalar()
            if not has_pk:
                self._log(f"No PRIMARY KEY on {table}, skipping DROP")
                return
            conn.execute(text(f"ALTER TABLE `{self.main_db}`.`{table}` DROP PRIMARY KEY"))
            self._log(f"Dropped PRIMARY KEY on {table}")

    def _ensure_nd_auto_increment_main(self, table: str, pk_cols: List[str]):
        if self._column_exists(self.main_engine, self.main_db, table, "nd_auto_increment_id"):
            self._log(f"nd_auto_increment_id already exists on MAIN.{table} — skipping")
            return
        self._log(f"Adding nd_auto_increment_id to MAIN.{table}")
        with self.main_engine.begin() as conn:
            if self.db_type == "mysql":
                conn.execute(text(
                    f"ALTER TABLE `{self.main_db}`.`{table}` ADD COLUMN nd_auto_increment_id BIGINT"
                ))
                conn.execute(text("SET @r := 0"))
                conn.execute(text(
                    f"UPDATE `{self.main_db}`.`{table}` SET nd_auto_increment_id = (@r := @r + 1)"
                ))
            else:
                conn.execute(text(f"ALTER TABLE {table} ADD nd_auto_increment_id BIGINT"))
                conn.execute(text(f"""
                    WITH cte AS (
                        SELECT nd_auto_increment_id,
                               ROW_NUMBER() OVER (ORDER BY (SELECT 1)) rn
                        FROM {table}
                    )
                    UPDATE cte SET nd_auto_increment_id = rn
                """))
        self._log(f"nd_auto_increment_id populated on MAIN.{table}")

    def _ensure_nd_extracted_date(self, table: str, default_date: str):
        if self._column_exists(self.main_engine, self.main_db, table, "nd_extracted_date"):
            self._log(f"nd_extracted_date already exists on MAIN.{table} — skipping")
            return
        self._log(f"Adding nd_extracted_date to MAIN.{table} with default={default_date}")
        with self.main_engine.begin() as conn:
            if self.db_type == "mysql":
                conn.execute(text(
                    f"ALTER TABLE `{self.main_db}`.`{table}` ADD COLUMN nd_extracted_date DATETIME"
                ))
                conn.execute(text(
                    f"UPDATE `{self.main_db}`.`{table}` SET nd_extracted_date = :d "
                    f"WHERE nd_extracted_date IS NULL"
                ), {"d": default_date})
            else:
                conn.execute(text(f"ALTER TABLE {table} ADD nd_extracted_date DATETIME"))
                conn.execute(text(
                    f"UPDATE {table} SET nd_extracted_date = :d WHERE nd_extracted_date IS NULL"
                ), {"d": default_date})
        self._log(f"nd_extracted_date populated on MAIN.{table}")

    def _ensure_nd_auto_increment_diff(self, table: str):
        if self._column_exists(self.diff_engine, self.diff_db, table, "nd_auto_increment_id"):
            self._log(f"nd_auto_increment_id already exists on DIFF.{table} — skipping")
            return
        self._log(f"Adding nd_auto_increment_id to DIFF.{table}")
        with self.diff_engine.begin() as conn:
            if self.db_type == "mysql":
                conn.execute(text(
                    f"ALTER TABLE `{self.diff_db}`.`{table}` ADD COLUMN nd_auto_increment_id BIGINT"
                ))
            else:
                conn.execute(text(f"ALTER TABLE {table} ADD nd_auto_increment_id BIGINT"))
        self._log(f"nd_auto_increment_id added to DIFF.{table}")

    def _populate_diff_nd_from_main(self, table: str, pk_cols: List[str]):
        # ── check how many NULL nd_auto_increment_id rows exist in DIFF ──
        with self.diff_engine.connect() as conn:
            if self.db_type == "mysql":
                null_count = conn.execute(text(
                    f"SELECT COUNT(*) FROM `{self.diff_db}`.`{table}` "
                    f"WHERE nd_auto_increment_id IS NULL"
                )).scalar()
            else:
                null_count = conn.execute(text(
                    f"SELECT COUNT(*) FROM {table} "
                    f"WHERE nd_auto_increment_id IS NULL"
                )).scalar()

        if null_count == 0:
            self._log(f"DIFF.{table} — all nd_auto_increment_id already populated ({null_count} NULLs) — skipping")
            return

        self._log(f"DIFF.{table} — {null_count} rows have NULL nd_auto_increment_id — will populate")

        # ── get max from MAIN to continue sequence from there ──
        with self.main_engine.connect() as conn:
            if self.db_type == "mysql":
                max_nd = conn.execute(text(
                    f"SELECT COALESCE(MAX(nd_auto_increment_id), 0) FROM `{self.main_db}`.`{table}`"
                )).scalar()
            else:
                max_nd = conn.execute(text(
                    f"SELECT COALESCE(MAX(nd_auto_increment_id), 0) FROM {table}"
                )).scalar()

        self._log(f"DIFF.{table} — max nd_auto_increment_id in MAIN={max_nd}, assigning from {max_nd + 1}")

        with self.diff_engine.begin() as conn:
            if self.db_type == "mysql":
                conn.execute(text("SET @r := :x"), {"x": max_nd})
                conn.execute(text(
                    f"UPDATE `{self.diff_db}`.`{table}` "
                    f"SET nd_auto_increment_id = (@r := @r + 1) "
                    f"WHERE nd_auto_increment_id IS NULL"
                ))
            else:
                conn.execute(text(f"""
                    WITH cte AS (
                        SELECT nd_auto_increment_id,
                               ROW_NUMBER() OVER (ORDER BY (SELECT 1)) rn
                        FROM {table}
                        WHERE nd_auto_increment_id IS NULL
                    )
                    UPDATE cte SET nd_auto_increment_id = rn + :x
                """), {"x": max_nd})

        self._log(f"DIFF.{table} — nd_auto_increment_id populated for {null_count} rows starting from {max_nd + 1}")


    def _ensure_active_flag(self, engine: Engine, db: str, table: str):
        if self._column_exists(engine, db, table, "nd_ActiveFlag"):
            self._log(f"nd_ActiveFlag already exists on {db}.{table} — skipping")
            return
        self._log(f"Adding nd_ActiveFlag to {db}.{table}")
        with engine.begin() as conn:
            if self.db_type == "mysql":
                conn.execute(text(f"ALTER TABLE `{db}`.`{table}` ADD nd_ActiveFlag CHAR(1)"))
                conn.execute(text(f"UPDATE `{db}`.`{table}` SET nd_ActiveFlag = 'Y'"))
            else:
                conn.execute(text(f"ALTER TABLE {table} ADD nd_ActiveFlag CHAR(1)"))
                conn.execute(text(f"UPDATE {table} SET nd_ActiveFlag = 'Y'"))
        self._log(f"nd_ActiveFlag added and set to Y on {db}.{table}")

    def _normalize_active_by_pk(self, engine: Engine, db: str, table: str, pk_cols: List[str]):
        if not pk_cols:
            return
        self._log(f"Normalizing nd_ActiveFlag by PK on {db}.{table} (pk_cols={pk_cols})")
        if self.db_type == "mysql":
            pk_expr = ",".join([f"`{c}`" for c in pk_cols])
            sql = f"""
                UPDATE `{db}`.`{table}` t
                JOIN (
                    SELECT nd_auto_increment_id FROM (
                        SELECT nd_auto_increment_id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY {pk_expr}
                                   ORDER BY nd_auto_increment_id DESC
                               ) rn
                        FROM `{db}`.`{table}`
                        WHERE nd_ActiveFlag = 'Y'
                    ) x WHERE rn > 1
                ) d ON t.nd_auto_increment_id = d.nd_auto_increment_id
                SET t.nd_ActiveFlag = 'N'
            """
        else:
            pk_expr = ",".join(f"[{c}]" for c in pk_cols)
            sql = f"""
                UPDATE t SET t.nd_ActiveFlag = 'N'
                FROM {table} t
                JOIN (
                    SELECT nd_auto_increment_id FROM (
                        SELECT nd_auto_increment_id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY {pk_expr}
                                   ORDER BY nd_auto_increment_id DESC
                               ) rn
                        FROM {table}
                        WHERE nd_ActiveFlag = 'Y'
                    ) x WHERE rn > 1
                ) d ON t.nd_auto_increment_id = d.nd_auto_increment_id
            """
        with engine.begin() as conn:
            conn.execute(text(sql))
        self._log(f"Normalization complete on {db}.{table}")

    def _deactivate_main_by_pk(self, table: str, df: pd.DataFrame, pk_cols: list):
        if df.empty:
            self._log(f"_deactivate_main_by_pk: empty df for {table} — skipping")
            return
        _raw_deactivate(
            self.main_engine, table, df,
            pk_cols, self.db_type, self.main_db
        )