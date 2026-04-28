"""
Airflow DAG: SQL Query Runner with CSV Logging
================================================
Runs all SQL queries from the provided script file and logs
success/failure of each query to a CSV file.

Configurable Paths (via Airflow Variables or DAG params):
- SCRIPT_PATH   : Path to the Python runner script (sql_runner.py)
- SQL_FILE_PATH : Path to the SQL queries file (.sql)
- CSV_LOG_PATH  : Path where the output CSV log will be saved

CSV columns include:
  run_timestamp, query_number, query_label, target_table,
  status, rows_before, rows_after, rows_delta, rows_affected, error_message
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import importlib.util
import logging
import csv
import os
import re

# ─────────────────────────────────────────────
# DEFAULT PATHS — override via Airflow Variables
# ─────────────────────────────────────────────
DEFAULT_SCRIPT_PATH  = "/Users/ndaicnsarizona/Documents/udm/de_udm/dashboards/mssql_queries_ecw_ndactiveflag_24apr.sql"
DEFAULT_SQL_FILE     = "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/Airflow/scripts/sql/ecw_providers_dashboard_queries.sql"
DEFAULT_CSV_LOG_PATH = "/Users/ndaicnsarizona/Desktop/production_state_incremental/state/dashboard/dashboard.csv"

# ─────────────────────────────────────────────
# DAG DEFAULT ARGS
# ─────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ─────────────────────────────────────────────
# HELPER: Load config from Airflow Variables
# ─────────────────────────────────────────────
def get_paths():
    return {
        "script_path":   Variable.get("SQL_RUNNER_SCRIPT_PATH",  default_var=DEFAULT_SCRIPT_PATH),
        "sql_file_path": Variable.get("SQL_RUNNER_SQL_FILE",     default_var=DEFAULT_SQL_FILE),
        "csv_log_path":  Variable.get("SQL_RUNNER_CSV_LOG_PATH", default_var=DEFAULT_CSV_LOG_PATH),
    }

# ─────────────────────────────────────────────
# TASK 1: Validate that all files/paths exist
# ─────────────────────────────────────────────
def validate_paths(**context):
    paths = get_paths()
    logging.info(f"Script path : {paths['script_path']}")
    logging.info(f"SQL file    : {paths['sql_file_path']}")
    logging.info(f"CSV log path: {paths['csv_log_path']}")

    errors = []
    if not os.path.exists(paths["script_path"]):
        errors.append(f"Script not found: {paths['script_path']}")
    if not os.path.exists(paths["sql_file_path"]):
        errors.append(f"SQL file not found: {paths['sql_file_path']}")

    csv_dir = os.path.dirname(paths["csv_log_path"])
    if csv_dir and not os.path.exists(csv_dir):
        try:
            os.makedirs(csv_dir, exist_ok=True)
            logging.info(f"Created CSV log directory: {csv_dir}")
        except Exception as e:
            errors.append(f"Cannot create CSV log dir: {e}")

    if errors:
        raise FileNotFoundError("\n".join(errors))

    logging.info("✅ All paths validated successfully.")
    # Push paths to XCom for downstream tasks
    context["ti"].xcom_push(key="paths", value=paths)


# ─────────────────────────────────────────────
# TASK 2: Load the SQL file & dynamically import
#          config from the runner script
# ─────────────────────────────────────────────
def load_and_prepare(**context):
    paths = context["ti"].xcom_pull(task_ids="validate_paths", key="paths")

    # Dynamically load the runner script to extract DB_CONFIG & SCHEMA_MAPPING
    spec = importlib.util.spec_from_file_location("sql_runner", paths["script_path"])
    runner_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner_module)

    db_config      = runner_module.DB_CONFIG
    schema_mapping = runner_module.SCHEMA_MAPPING

    # Read and substitute placeholders in SQL
    with open(paths["sql_file_path"], "r", encoding="utf-8") as f:
        raw_sql = f.read()

    for placeholder, actual in schema_mapping.items():
        raw_sql = raw_sql.replace(f"{{{{{placeholder}}}}}", actual)

    # Split into individual queries
    queries = []
    for q in raw_sql.split(";"):
        stripped = q.strip()
        if stripped:
            queries.append(stripped)

    logging.info(f"✅ Loaded {len(queries)} queries from SQL file.")

    context["ti"].xcom_push(key="queries",    value=queries)
    context["ti"].xcom_push(key="db_config",  value=db_config)
    context["ti"].xcom_push(key="paths",      value=paths)


# ─────────────────────────────────────────────
# HELPER: Extract table metadata from a query
# ─────────────────────────────────────────────
def extract_table_info(query):
    q_upper = query.upper()
    db_name = schema_name = table_name = None
    try:
        if "SELECT" in q_upper and "INTO" in q_upper:
            into_part = re.split(r"\bINTO\b", query, flags=re.IGNORECASE)[1].strip()
            raw_name  = into_part.split()[0].replace("[", "").replace("]", "")
            parts = raw_name.split(".")
            if len(parts) == 3:
                db_name, schema_name, table_name = parts
            elif len(parts) == 2:
                schema_name, table_name = parts
            else:
                schema_name, table_name = "dbo", parts[0]

        elif q_upper.lstrip().startswith("CREATE TABLE"):
            after = re.split(r"\bCREATE\s+TABLE\b", query, flags=re.IGNORECASE)[1].strip()
            raw_name = after.split("(")[0].strip().replace("[", "").replace("]", "")
            parts = raw_name.split(".")
            if len(parts) == 3:
                db_name, schema_name, table_name = parts
            elif len(parts) == 2:
                schema_name, table_name = parts
            else:
                schema_name, table_name = "dbo", parts[0]

    except Exception as e:
        logging.warning(f"⚠️ Could not parse table info: {e}")
    return db_name, schema_name, table_name


# ─────────────────────────────────────────────
# HELPER: Get current row count for a table
# Returns None if table doesn't exist or on error
# ─────────────────────────────────────────────
def get_row_count(engine, db_name, schema_name, table_name):
    from sqlalchemy import text
    try:
        fully_qualified = f"[{db_name}].[{schema_name}].[{table_name}]"
        with engine.connect() as conn:
            # First check if table exists
            exists = conn.execute(
                text("""
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_CATALOG = :db
                      AND TABLE_SCHEMA  = :schema
                      AND TABLE_NAME    = :table
                """),
                {"db": db_name, "schema": schema_name or "dbo", "table": table_name}
            ).fetchone()
            if not exists:
                return None
            result = conn.execute(text(f"SELECT COUNT(*) FROM {fully_qualified}"))
            return result.scalar()
    except Exception as e:
        logging.warning(f"⚠️ Could not get row count for {db_name}.{schema_name}.{table_name}: {e}")
        return None



# ─────────────────────────────────────────────
# TASK 3: Check all databases are online
# ─────────────────────────────────────────────
def check_databases(**context):
    from sqlalchemy import create_engine, text

    queries = context["ti"].xcom_pull(task_ids="load_and_prepare", key="queries")
    db_conf = context["ti"].xcom_pull(task_ids="load_and_prepare", key="db_config")

    def get_conn_str(db_name):
        return (
            f"mssql+pyodbc://{db_conf['user']}:{db_conf['password']}"
            f"@{db_conf['host']}:{db_conf['port']}/{db_name}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
        )

    # Collect all unique DB names referenced across queries + primary DB
    dbs_to_check = {db_conf["database"]}
    for q in queries:
        db_name, _, _ = extract_table_info(q)
        if db_name:
            dbs_to_check.add(db_name)

    failed_dbs = []
    for db_name in dbs_to_check:
        try:
            engine = create_engine(
                get_conn_str(db_name),
                connect_args={"timeout": 10},   # login timeout in seconds
            )
            with engine.connect() as conn:
                # Check MSSQL database state — must be ONLINE
                row = conn.execute(
                    text("SELECT state_desc FROM sys.databases WHERE name = :db"),
                    {"db": db_name},
                ).fetchone()
                if row is None:
                    raise RuntimeError(f"Database `{db_name}` not found in sys.databases")
                if row[0] != "ONLINE":
                    raise RuntimeError(f"Database `{db_name}` state is {row[0]}, expected ONLINE")
                # Quick connectivity ping
                conn.execute(text("SELECT 1"))
            engine.dispose()
            logging.info("✅ DB `%s` is ONLINE and reachable", db_name)
        except Exception as e:
            logging.error("❌ DB `%s` is DOWN or blocked: %s", db_name, e)
            failed_dbs.append(db_name)

    all_dbs_up = len(failed_dbs) == 0
    context["ti"].xcom_push(key="all_dbs_up",  value=all_dbs_up)
    context["ti"].xcom_push(key="failed_dbs",  value=failed_dbs)

    if not all_dbs_up:
        logging.warning(
            "⚠️  %d DB(s) are down or blocked: %s — DROP TABLE queries will be skipped.",
            len(failed_dbs), failed_dbs,
        )
    else:
        logging.info("✅ All %d database(s) are up.", len(dbs_to_check))


# ─────────────────────────────────────────────
# TASK 4: Execute all queries & log to CSV
# ─────────────────────────────────────────────
def execute_queries(**context):
    from sqlalchemy import create_engine, text

    queries     = context["ti"].xcom_pull(task_ids="load_and_prepare",  key="queries")
    db_conf     = context["ti"].xcom_pull(task_ids="load_and_prepare",  key="db_config")
    paths       = context["ti"].xcom_pull(task_ids="load_and_prepare",  key="paths")
    all_dbs_up  = context["ti"].xcom_pull(task_ids="check_databases",   key="all_dbs_up")
    failed_dbs  = context["ti"].xcom_pull(task_ids="check_databases",   key="failed_dbs") or []
    csv_path    = paths["csv_log_path"]
    run_time    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def get_conn_str(db_name):
        return (
            f"mssql+pyodbc://{db_conf['user']}:{db_conf['password']}"
            f"@{db_conf['host']}:{db_conf['port']}/{db_name}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
        )

    csv_rows     = []
    success_cnt  = 0
    failure_cnt  = 0
    skipped_cnt  = 0

    # ── Abort all queries if any DB is down or blocked ───────────────────
    if not all_dbs_up:
        logging.warning(
            "⚠️  Skipping ALL queries — DB(s) down or blocked: %s", failed_dbs
        )
        for i, query in enumerate(queries, start=1):
            csv_rows.append({
                "run_timestamp":  run_time,
                "query_number":   i,
                "query_label":    query.strip().splitlines()[0][:120],
                "target_table":   "",
                "status":         "SKIPPED",
                "rows_before":    "",
                "rows_after":     "",
                "rows_delta":     "",
                "rows_affected":  "",
                "error_message":  f"Skipped — DB(s) down or blocked: {failed_dbs}",
            })
            skipped_cnt += 1

        fieldnames = [
            "run_timestamp", "query_number", "query_label", "target_table",
            "status", "rows_before", "rows_after", "rows_delta",
            "rows_affected", "error_message",
        ]
        file_exists = os.path.exists(csv_path)
        with open(csv_path, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists or os.path.getsize(csv_path) == 0:
                writer.writeheader()
            writer.writerows(csv_rows)

        raise Exception(f"Execution aborted — DB(s) down or blocked: {failed_dbs}")

    current_db = db_conf["database"]
    engine     = create_engine(get_conn_str(current_db), pool_recycle=3600)

    for i, query in enumerate(queries, start=1):
        q_upper       = query.upper().strip()
        status        = None
        error_msg     = ""
        rows_affected = None
        rows_before   = None
        rows_after    = None
        rows_delta    = None
        skip_flag     = False

        db_name, schema_name, table_name = extract_table_info(query)

        # Resolve effective DB for row-count queries
        effective_db = db_name if db_name else current_db

        # Switch DB connection if needed
        if db_name and db_name.upper() != current_db.upper():
            logging.info(f"🔄 Switching to DB `{db_name}`")
            engine.dispose()
            engine     = create_engine(get_conn_str(db_name), pool_recycle=3600)
            current_db = db_name

        # ── Row count BEFORE ────────────────────────────────────────────
        if table_name:
            rows_before = get_row_count(engine, effective_db, schema_name or "dbo", table_name)
            logging.info(
                f"📋 Query {i} | Table `{effective_db}.{schema_name}.{table_name}` "
                f"| rows BEFORE: {rows_before if rows_before is not None else 'table absent'}"
            )

        # ── Skip CREATE/SELECT INTO if table already exists ──────────────
        if (
            table_name
            and not any(k in q_upper for k in ("UPDATE", "INSERT", "DROP"))
        ):
            try:
                with engine.connect() as conn:
                    res = conn.execute(
                        text("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t"),
                        {"s": schema_name or "dbo", "t": table_name}
                    ).fetchone()
                    if res:
                        status      = "SKIPPED"
                        error_msg   = f"Table `{effective_db}.{schema_name}.{table_name}` already exists"
                        skip_flag   = True
                        skipped_cnt += 1
                        rows_after  = rows_before   # no change
                        rows_delta  = 0
                        logging.info(f"⚠️  Query {i} SKIPPED — {error_msg}")
            except Exception as e:
                logging.warning(f"⚠️  Could not check table existence for query {i}: {e}")

        # ── Execute ──────────────────────────────────────────────────────
        if not skip_flag:
            try:
                with engine.begin() as conn:
                    result        = conn.exec_driver_sql(query, execution_options={"autocommit": True})
                    rows_affected = result.rowcount if result.rowcount != -1 else None
                    status        = "SUCCESS"
                    success_cnt  += 1
                    logging.info(f"✅ Query {i} SUCCESS — rows_affected: {rows_affected}")
            except Exception as e:
                status       = "FAILED"
                error_msg    = str(e)
                failure_cnt += 1
                logging.error(f"❌ Query {i} FAILED — {e}")

        # ── Row count AFTER ──────────────────────────────────────────────
        if table_name and not skip_flag:
            rows_after = get_row_count(engine, effective_db, schema_name or "dbo", table_name)
            if rows_before is not None and rows_after is not None:
                rows_delta = rows_after - rows_before           # existing table updated
            elif rows_before is None and rows_after is not None:
                rows_delta = rows_after                         # table newly created
            else:
                rows_delta = None
            logging.info(
                f"📋 Query {i} | Table `{effective_db}.{schema_name}.{table_name}` "
                f"| rows AFTER: {rows_after if rows_after is not None else 'N/A'} "
                f"| DELTA: {rows_delta if rows_delta is not None else 'N/A'}"
            )

        # ── Build CSV row ────────────────────────────────────────────────
        first_line = query.strip().splitlines()[0][:120]

        csv_rows.append({
            "run_timestamp":  run_time,
            "query_number":   i,
            "query_label":    first_line,
            "target_table":   f"{effective_db}.{schema_name}.{table_name}" if table_name else "",
            "status":         status,
            "rows_before":    rows_before   if rows_before  is not None else "",
            "rows_after":     rows_after    if rows_after   is not None else "",
            "rows_delta":     rows_delta    if rows_delta   is not None else "",
            "rows_affected":  rows_affected if rows_affected is not None else "",
            "error_message":  error_msg,
        })

    engine.dispose()

    # Write CSV
    fieldnames = [
        "run_timestamp", "query_number", "query_label", "target_table",
        "status", "rows_before", "rows_after", "rows_delta",
        "rows_affected", "error_message",
    ]

    file_exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists or os.path.getsize(csv_path) == 0:
            writer.writeheader()
        writer.writerows(csv_rows)

    logging.info(
        f"\n📊 Execution Summary:\n"
        f"   ✅ Success : {success_cnt}\n"
        f"   ⚠️  Skipped : {skipped_cnt}\n"
        f"   ❌ Failed  : {failure_cnt}\n"
        f"   📄 CSV log : {csv_path}"
    )

    # Fail the task if any queries failed (optional — remove if you want soft fails)
    if failure_cnt > 0:
        raise Exception(f"{failure_cnt} queries failed. Check CSV log at: {csv_path}")


# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────
with DAG(
    dag_id="provider_dashboard_refresh",
    description="Runs SQL queries from a script and logs results to CSV",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 22 * * *",          # Trigger manually or set a cron e.g. "0 2 * * *"
    catchup=False,
    tags=["sql", "etl", "reporting"],
    params={
        # These can also be overridden at trigger time via the Airflow UI
        "script_path":   DEFAULT_SCRIPT_PATH,
        "sql_file_path": DEFAULT_SQL_FILE,
        "csv_log_path":  DEFAULT_CSV_LOG_PATH,
    },
) as dag:

    t1_validate = PythonOperator(
        task_id="validate_paths",
        python_callable=validate_paths,
    )

    t2_load = PythonOperator(
        task_id="load_and_prepare",
        python_callable=load_and_prepare,
    )

    t3_check_dbs = PythonOperator(
        task_id="check_databases",
        python_callable=check_databases,
    )

    t4_execute = PythonOperator(
        task_id="execute_queries",
        python_callable=execute_queries,
    )

    t1_validate >> t2_load >> t3_check_dbs >> t4_execute



















