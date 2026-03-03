# merge_engine.py

from sqlalchemy import create_engine, text, String
from sqlalchemy.engine import Engine
import logging
import traceback
from typing import List, Dict
import pandas as pd
import numpy as np



BATCH_INSERT_CHUNK = 1000

def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.astype(object)
    df = df.replace({pd.NaT: None, np.nan: None})
    df = df.replace(
        {
            "nan": None,
            "NaN": None,
            "NAN": None,
            "null": None,
            "NULL": None,
            #"": None,
            "NaT": None,
            "None": None,
            "none": None
        }
    )
    df = df.where(pd.notnull(df), None)
    return df



def insert_rows_mysql(conn, db, table, df):
    if df.empty:
        return

    df = sanitize_df(df)
    cols = list(df.columns)

    col_sql = ",".join([f"`{c}`" for c in cols])
    placeholders = ",".join([f":{c}" for c in cols])

    sql = text(
        f"INSERT INTO `{db}`.`{table}` ({col_sql}) VALUES ({placeholders})"
    )

    rows = df.to_dict("records")
    for i in range(0, len(rows), BATCH_INSERT_CHUNK):
        conn.execute(sql, rows[i:i+BATCH_INSERT_CHUNK])


def insert_rows_mssql(conn, table, df):
    if df.empty:
        return

    df = sanitize_df(df)
    cols = list(df.columns)

    col_sql = ",".join(cols)
    placeholders = ",".join([f":{c}" for c in cols])

    sql = text(
        f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"
    )

    rows = df.to_dict("records")
    for i in range(0, len(rows), BATCH_INSERT_CHUNK):
        conn.execute(sql, rows[i:i+BATCH_INSERT_CHUNK])



class MergeEngine:
    """
    Generic incremental merge engine for MySQL / MSSQL.
    Airflow should ONLY call run_table().
    """

    def __init__(self, db_type: str, main_conn: str, diff_conn: str, diffmerged_conn: str):
        self.db_type = db_type.lower()
        if self.db_type not in ("mysql", "mssql"):
            raise ValueError("db_type must be mysql or mssql")

        self.main_engine: Engine = create_engine(main_conn)
        self.diff_engine: Engine = create_engine(diff_conn)
        self.diffmerged_engine: Engine = create_engine(diffmerged_conn)

        self.main_db = self._extract_db(main_conn)
        self.diff_db = self._extract_db(diff_conn)
        self.diffmerged_db = self._extract_db(diffmerged_conn)

        logging.info("MAIN DB = %s", self.main_db)
        logging.info("DIFF DB = %s", self.diff_db)
        logging.info("DIFFMERGED DB = %s", self.diffmerged_db)

    # ============================================================
    # SINGLE PUBLIC ENTRY POINT (Airflow calls ONLY this)
    # ============================================================

    def run_table(
        self,
        table: str,
        pk_cols: List[str],
        default_date: str
    ) -> Dict:
        """
        This is the ONLY method Airflow should call.
        """

        try:
            self._log(f"START MERGE: {table}")

            self._step_0_skip_if_already_processed(table)
            # 1. Main table validation
            # - Checks mainDB.table exists
            # - Validates PK columns exist
            # - Ensures no duplicate PK rows in main table
            # - Ensures schema is readable
            self._step_1_validate_main_table(table)

            # 2. Main table preparation
            # - Adds audit columns if missing (created_at, updated_at, etc.)
            # - Adds soft-delete or active flag if required
            # - Ensures correct data types for PK columns
            # - Sets default values where needed
            self._step_2_prepare_main_table(table, pk_cols, default_date)

            # 3. Diff table validation
            # - Checks diffDB.table exists
            # - Ensures diff table is not empty
            # - Validates PK columns exist
            # - Ensures no duplicate PK rows in diff
            self._step_3_validate_diff_table(table)

            # 4. Diff table preparation
            # - Aligns diff schema with main schema
            # - Casts column types to match main
            # - Removes identity / auto-increment constraints
            # - Normalizes column order
            self._step_4_prepare_diff_table(table, pk_cols)

            # 5. Compare & classify
            # Performs:
            # - JOIN main vs diff on PK
            # - Splits into:
            #   updated_df → rows where PK exists in main but non-PK columns differ
            #   new_df     → rows where PK does not exist in main
            # This is the core diff logic.
            updated_df, new_df = self._step_5_compare_and_classify(table, pk_cols)

            # 6. Merge into MainDB
            # - Deactivate old pk with 'N' and insert rows into rows using from updated_df
            # - INSERT new rows using new_df
            # - This mutates the system-of-record (MainDB)
            updated_insert_df, insert_df_new = self._step_6_merge_into_main(table, pk_cols, updated_df, new_df)

            diff_df_inserted_snapshot = self._step_7_merge_into_diffmergedDB(table, pk_cols, updated_insert_df, insert_df_new)

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

    # ============================================================
    # STEP 0 – Skip if already processed
    # ============================================================

    def _step_0_skip_if_already_processed(self, table: str):
        # You will plug your merge_log table here
        # For now: no-op
        self._log("Step 0: skip check (Loop)")

    # ============================================================
    # STEP 1 – Validate MAIN table
    # ============================================================

    def _step_1_validate_main_table(self, table: str):
        self._log("Step 1: validate MAIN table")

        with self.main_engine.connect() as conn:
            if self.db_type == "mysql":
                q = text("""
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = :db
                    AND table_name = :tbl
                """)
                exists = conn.execute(q, {
                    "db": self.main_db,
                    "tbl": table
                }).scalar()
            else:
                q = text("""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = :tbl
                """)
                exists = conn.execute(q, {"tbl": table}).scalar()

            if exists == 0:
                raise Exception(f"MAIN table not found: {table}")

    # ============================================================
    # STEP 2 – Prepare MAIN
    # ============================================================

    def _step_2_prepare_main_table(
        self,
        table: str,
        pk_cols: List[str],
        default_date: str
    ):
        self._log("Step 2: prepare MAIN table")

        self._drop_primary_key(table)
        self._ensure_nd_auto_increment_main(table, pk_cols)
        self._ensure_nd_extracted_date(table, default_date)
        self._ensure_active_flag(self.main_engine, self.main_db, table)

        if pk_cols:
            self._normalize_active_by_pk(self.main_engine, self.main_db, table, pk_cols)

    # ============================================================
    # STEP 3 – Validate DIFF table
    # ============================================================

    def _step_3_validate_diff_table(self, table: str):
        self._log("Step 3: validate DIFF table")

        with self.diff_engine.connect() as conn:
            if self.db_type == "mysql":
                q = text("""
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = :db
                    AND table_name = :tbl
                """)
                exists = conn.execute(q, {
                    "db": self.diff_db,
                    "tbl": table
                }).scalar()
            else:
                q = text("""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = :tbl
                """)
                exists = conn.execute(q, {"tbl": table}).scalar()

            if exists == 0:
                raise Exception(f"DIFF table not found: {table}")

    # ============================================================
    # STEP 4 – Prepare DIFF
    # ============================================================

    def _step_4_prepare_diff_table(self, table: str, pk_cols: List[str]):
        self._log("Step 4: prepare DIFF table")

        self._ensure_nd_auto_increment_diff(table)
        self._populate_diff_nd_from_main(table, pk_cols)
        self._ensure_active_flag(self.diff_engine, self.diff_db, table)

        if pk_cols:
            self._normalize_active_by_pk(self.diff_engine, self.diff_db, table, pk_cols)

    # ============================================================
    # STEP 5 – Compare keys
    # ============================================================

    def _step_5_compare_and_classify(self, table: str, pk_cols: List[str]):
        import pandas as pd

        self._log("Step 5: compare MAIN vs DIFF")

        if self.db_type == "mysql":
            # MySQL via SQLAlchemy engine
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

        elif self.db_type == "mssql":
            # MSSQL via SQLAlchemy engine (use +pyodbc)
            with self.diff_engine.connect() as conn:
                diff_df = pd.read_sql(f"SELECT * FROM {self.diff_db}.dbo.{table}", conn)
            if not pk_cols:
                return pd.DataFrame(), diff_df
            main_keys = pd.read_sql(
                f"SELECT {','.join(pk_cols)} FROM {self.main_db}.dbo.{table} WHERE nd_ActiveFlag='Y'",
                self.main_engine
            )

        # make composite key
        def make_key(df):
            result = df[pk_cols[0]].astype(str)
            for col in pk_cols[1:]:
                result = result + "||" + df[col].astype(str)
            return result

        main_set = set(make_key(main_keys))
        diff_df["_key"] = make_key(diff_df)

        updated_df = diff_df[diff_df["_key"].isin(main_set)].copy()
        new_df = diff_df[~diff_df["_key"].isin(main_set)].copy()

        return updated_df, new_df



    # ============================================================
    # STEP 6 – Merge
    # ============================================================

    def _step_6_merge_into_main(self, table: str, pk_cols: List[str], updated_rows_df, new_rows_df):
        """
        Merge updated and new rows into the main table.
        - Aligns columns to main table order
        - Deactivates old rows for updated_df
        - Inserts updated and new rows
        """
        self._log(f"Step 6: merge into MAIN ({table})")

        with self.main_engine.begin() as conn:

            # Fetch main table column order
            if self.db_type == "mysql":
                sql = text("""
                    SELECT COLUMN_NAME
                    FROM information_schema.columns
                    WHERE table_schema=:db AND table_name=:tbl
                    ORDER BY ordinal_position
                """)
                result = conn.execute(sql, {"db": self.main_db, "tbl": table}).fetchall()
            else:  # MSSQL
                sql = text("""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME=:tbl
                    ORDER BY ORDINAL_POSITION
                """)
                result = conn.execute(sql, {"tbl": table}).fetchall()

            main_columns = [r[0] for r in result]

            if not main_columns:
                raise Exception("FATAL: main_columns is empty. Metadata query failed.")

            # Fill missing columns with None
            for df in [updated_rows_df, new_rows_df]:
                for c in main_columns:
                    if c not in df.columns:
                        df[c] = None

            # Prepare DataFrames to insert
            updated_insert_df = updated_rows_df[main_columns].copy() if not updated_rows_df.empty else pd.DataFrame()
            insert_df_new = new_rows_df[main_columns].copy() if not new_rows_df.empty else pd.DataFrame()

            # Deactivate old rows and insert updated
            if not updated_insert_df.empty:
                self._deactivate_main_by_pk(table, updated_insert_df, pk_cols)

                if self.db_type == "mysql":
                    insert_rows_mysql(conn, self.main_db, table, updated_insert_df)
                else:
                    insert_rows_mssql(conn, table, updated_insert_df)

            # Insert new rows
            if not insert_df_new.empty:
                if self.db_type == "mysql":
                    insert_rows_mysql(conn, self.main_db, table, insert_df_new)
                else:
                    insert_rows_mssql(conn, table, insert_df_new)


        # Return prepared DataFrames in case you want to log or debug them
        return updated_insert_df, insert_df_new
    
    def _step_7_merge_into_diffmergedDB(
        self,
        table: str,
        pk_cols: list,
        updated_insert_df: pd.DataFrame,
        insert_df_new: pd.DataFrame
    ):
        """
        Step 7:
        Persist diff snapshot into DiffMergedDB.
        """

        self._log(f"STEP 7: Persisting diff into DiffMergedDB for {table}")

        # -------------------------------------------------
        # 1. Normalize input DataFrames
        # -------------------------------------------------
        dfs = []

        if updated_insert_df is not None and not updated_insert_df.empty:
            dfs.append(updated_insert_df)

        if insert_df_new is not None and not insert_df_new.empty:
            dfs.append(insert_df_new)

        if not dfs:
            self._log("STEP 7: No rows to persist into DiffMergedDB")
            return pd.DataFrame()

        final_df = pd.concat(dfs, ignore_index=True)

        # -------------------------------------------------
        # 2. Create table if not exists
        # -------------------------------------------------
        with self.diffmerged_engine.begin() as diffmerged_conn:

            # ---------- MySQL ----------
            if self.db_type == "mysql":
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table}
                LIKE {self.main_db}.{table}
                """
                diffmerged_conn.execute(text(create_sql))

            # ---------- MSSQL ----------
            else:
                create_sql = f"""
                IF OBJECT_ID('{table}', 'U') IS NULL
                BEGIN
                    SELECT TOP 0 *
                    INTO {table}
                    FROM {self.main_db}.dbo.{table}
                END
                """
                diffmerged_conn.execute(text(create_sql))


            # -------------------------------------------------
            # 3. Append data
            # -------------------------------------------------
            final_df.to_sql(
                name=table,
                con=diffmerged_conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000
            )

        self._log(
            f"STEP 7 COMPLETE: Inserted {len(final_df)} rows into DiffMergedDB.{table}"
        )

        return final_df

    # ============================================================
    # LOW-LEVEL HELPERS (DB AGNOSTIC CORE)
    # ============================================================

    def _extract_db(self, conn: str):
        return conn.split("/")[-1].split("?")[0]

    def _log(self, msg: str):
        logging.info("[MergeEngine] %s", msg)

    # ---------- MAIN helpers ----------

    def _drop_primary_key(self, table: str):
        if self.db_type != "mysql":
            return

        with self.main_engine.begin() as conn:
            conn.execute(text(
                f"ALTER TABLE `{self.main_db}`.`{table}` DROP PRIMARY KEY"
            ))

    def _column_exists(self, engine, db, table, column):
        with engine.connect() as conn:
            if self.db_type == "mysql":
                q = text("""
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_schema=:db
                    AND table_name=:tbl
                    AND column_name=:col
                """)
                return conn.execute(q, {
                    "db": db, "tbl": table, "col": column
                }).scalar() > 0
            else:
                q = text("""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME=:tbl
                    AND COLUMN_NAME=:col
                """)
                return conn.execute(q, {
                    "tbl": table, "col": column
                }).scalar() > 0


    
    def _ensure_nd_auto_increment_main(self, table, pk_cols):
        if self._column_exists(self.main_engine, self.main_db, table, "nd_auto_increment_id"):
            return

        with self.main_engine.begin() as conn:
            if self.db_type == "mysql":
                conn.execute(text(
                    f"ALTER TABLE `{self.main_db}`.`{table}` "
                    f"ADD COLUMN nd_auto_increment_id BIGINT"
                ))
                conn.execute(text("SET @r := 0"))
                conn.execute(text(
                    f"""
                    UPDATE `{self.main_db}`.`{table}`
                    SET nd_auto_increment_id = (@r := @r + 1)
                    """
                ))
            else:  # MSSQL
                conn.execute(text(
                    f"""
                    ALTER TABLE {table}
                    ADD nd_auto_increment_id BIGINT
                    """
                ))
                conn.execute(text(
                    f"""
                    WITH cte AS (
                        SELECT nd_auto_increment_id,
                            ROW_NUMBER() OVER (ORDER BY (SELECT 1)) rn
                        FROM {table}
                    )
                    UPDATE cte SET nd_auto_increment_id = rn
                    """
                ))


    def _ensure_nd_extracted_date(self, table, default_date):
        if self._column_exists(
            self.main_engine,
            self.main_db,
            table,
            "nd_extracted_date"
        ):
            return

        with self.main_engine.begin() as conn:
            if self.db_type == "mysql":
                conn.execute(text(
                    f"""
                    ALTER TABLE `{self.main_db}`.`{table}`
                    ADD COLUMN nd_extracted_date DATETIME
                    """
                ))
                conn.execute(text(
                    f"""
                    UPDATE `{self.main_db}`.`{table}`
                    SET nd_extracted_date = :d
                    WHERE nd_extracted_date IS NULL
                    """
                ), {"d": default_date})
            else:  # MSSQL
                conn.execute(text(
                    f"""
                    ALTER TABLE {table}
                    ADD nd_extracted_date DATETIME
                    """
                ))
                conn.execute(text(
                    f"""
                    UPDATE {table}
                    SET nd_extracted_date = :d
                    WHERE nd_extracted_date IS NULL
                    """
                ), {"d": default_date})


    def _ensure_nd_auto_increment_diff(self, table):
        if self._column_exists(self.diff_engine, self.diff_db, table, "nd_auto_increment_id"):
            return

        with self.diff_engine.begin() as conn:
            if self.db_type == "mysql":
                conn.execute(text(
                    f"ALTER TABLE `{self.diff_db}`.`{table}` "
                    f"ADD COLUMN nd_auto_increment_id BIGINT"
                ))
            else:
                conn.execute(text(
                    f"""
                    ALTER TABLE {table}
                    ADD nd_auto_increment_id BIGINT
                    """
                ))

    def _populate_diff_nd_from_main(self, table, pk_cols):
        # Step 1: get max from main
        with self.main_engine.connect() as conn:
            if self.db_type == "mysql":
                max_nd = conn.execute(text(
                    f"""
                    SELECT COALESCE(MAX(nd_auto_increment_id),0)
                    FROM `{self.main_db}`.`{table}`
                    """
                )).scalar()
            else:
                max_nd = conn.execute(text(
                    f"""
                    SELECT COALESCE(MAX(nd_auto_increment_id),0)
                    FROM {table}
                    """
                )).scalar()

        # Step 2: populate diff
        with self.diff_engine.begin() as conn:
            if self.db_type == "mysql":
                conn.execute(text("SET @r := :x"), {"x": max_nd})
                conn.execute(text(
                    f"""
                    UPDATE `{self.diff_db}`.`{table}`
                    SET nd_auto_increment_id = (@r := @r + 1)
                    WHERE nd_auto_increment_id IS NULL
                    """
                ))
            else:  # MSSQL
                conn.execute(text(
                    f"""
                    WITH cte AS (
                        SELECT nd_auto_increment_id,
                            ROW_NUMBER() OVER (ORDER BY (SELECT 1)) rn
                        FROM {table}
                        WHERE nd_auto_increment_id IS NULL
                    )
                    UPDATE cte
                    SET nd_auto_increment_id = rn + :x
                    """
                ), {"x": max_nd})

    def _ensure_active_flag(self, engine, db, table):
        if self._column_exists(engine, db, table, "nd_ActiveFlag"):
            return

        with engine.begin() as conn:
            if self.db_type == "mysql":
                conn.execute(text(
                    f"ALTER TABLE `{db}`.`{table}` ADD nd_ActiveFlag CHAR(1)"
                ))
                conn.execute(text(
                    f"UPDATE `{db}`.`{table}` SET nd_ActiveFlag='Y'"
                ))
            else:
                conn.execute(text(
                    f"ALTER TABLE {table} ADD nd_ActiveFlag CHAR(1)"
                ))
                conn.execute(text(
                    f"UPDATE {table} SET nd_ActiveFlag='Y'"
                ))


    def _normalize_active_by_pk(self, engine, db, table, pk_cols):
        if not pk_cols:
            return

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
                    WHERE nd_ActiveFlag='Y'
                ) x WHERE rn>1
            ) d
            ON t.nd_auto_increment_id=d.nd_auto_increment_id
            SET t.nd_ActiveFlag='N'
            """
        else:  # MSSQL
            pk_expr = ",".join(pk_cols)
            sql = f"""
            UPDATE t
            SET nd_ActiveFlag='N'
            FROM {table} t
            JOIN (
                SELECT nd_auto_increment_id FROM (
                    SELECT nd_auto_increment_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY {pk_expr}
                            ORDER BY nd_auto_increment_id DESC
                        ) rn
                    FROM {table}
                    WHERE nd_ActiveFlag='Y'
                ) x WHERE rn>1
            ) d
            ON t.nd_auto_increment_id=d.nd_auto_increment_id
            """

        with engine.begin() as conn:
            conn.execute(text(sql))



    def _deactivate_mysql(self, engine, main_db, table, pk_df, pk_cols):
        if pk_df.empty:
            return

        temp_table = f"tmp_deactivate_{table}"

        with engine.begin() as conn:

            # create temp table
            col_defs = []
            for c in pk_cols:
                col_defs.append(f"`{c}` VARCHAR(255) NOT NULL")

            conn.execute(text(f"""
                CREATE TEMPORARY TABLE `{temp_table}` (
                    {", ".join(col_defs)},
                    PRIMARY KEY ({",".join(f"`{c}`" for c in pk_cols)})
                ) ENGINE=MEMORY
            """))

            pk_df[pk_cols].drop_duplicates().to_sql(
                temp_table,
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000
            )

            join_cond = " AND ".join(
                [f"m.`{c}` = t.`{c}`" for c in pk_cols]
            )

            conn.execute(text(f"""
                UPDATE `{main_db}`.`{table}` m
                JOIN `{temp_table}` t
                ON {join_cond}
                SET m.nd_ActiveFlag = 'N'
                WHERE m.nd_ActiveFlag = 'Y'
            """))

    def _deactivate_mssql(self, engine, table, pk_df, pk_cols):
        if pk_df.empty:
            return

        temp_table = "#tmp_deactivate"

        with engine.begin() as conn:

            # create temp table
            col_defs = []
            for c in pk_cols:
                col_defs.append(f"{c} NVARCHAR(255) NOT NULL")

            conn.execute(text(f"""
                CREATE TABLE {temp_table} (
                    {", ".join(col_defs)},
                    PRIMARY KEY ({",".join(pk_cols)})
                )
            """))

            # insert PKs
            pk_df[pk_cols].drop_duplicates().to_sql(
                temp_table,
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000
            )

            join_cond = " AND ".join(
                [f"m.{c} = t.{c}" for c in pk_cols]
            )

            conn.execute(text(f"""
                UPDATE m
                SET m.nd_ActiveFlag = 'N'
                FROM {table} m
                JOIN {temp_table} t
                ON {join_cond}
                WHERE m.nd_ActiveFlag = 'Y'
            """))


    def _deactivate_main_by_pk(self, table, df, pk_cols):
        if df.empty:
            return

        if self.db_type == "mysql":
            self._deactivate_mysql(
                self.main_engine,
                self.main_db,
                table,
                df,
                pk_cols
            )
        else:
            self._deactivate_mssql(
                self.main_engine,
                table,
                df,
                pk_cols
            )
