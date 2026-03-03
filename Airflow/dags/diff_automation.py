from __future__ import annotations
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from typing import Any, Optional

import pendulum
import requests

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
try:
    from airflow.sdk import Variable
except ImportError:
    from airflow.models import Variable  # Fallback for Airflow < 3.x

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 🧭 Configuration
# ──────────────────────────────────────────────
local_tz = pendulum.timezone("Asia/Kolkata")

# Base path for the deidentification repo on the Airflow host
BASE_REPO_DIR = "/Users/ndaicnsarizona/Desktop/deidentification/"

# Script directory (Airflow DAGs location)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# CDC directory lives inside the deidentification repo
CDC_DIR = os.path.join(BASE_REPO_DIR, "CDC", "MySQL")

# Python executable path (optional - if None, uses 'python' from PATH)
# Set this if you need to use a specific Python interpreter
# Example: PYTHON_EXECUTABLE = "/opt/miniconda3/envs/airflow_deid/bin/python"
# If Airflow runs from the conda environment, 'python' should work fine
PYTHON_EXECUTABLE = os.environ.get("AIRFLOW_PYTHON", "python")

# Default configuration (can be overridden via Airflow Variables or DAG params)
DEFAULT_CONFIG = {
    "staging_schema": "mobiledoc_staging",
    "prod_schema": "mobiledoc_oct",
    "mysql_user": "ndadmin",
    "mysql_pass": "ndADMIN@2025",
    "mysql_host": "localhost",
}

# Webhook URL for Google Chat notifications
# You can set this in Airflow Variables: Variable.set("gchat_webhook_url", "your_url")
# Accessing Variable at module level can cause issues if DB isn't initialized, so we'll get it lazily
WEBHOOK_URL = None

def get_webhook_url():
    """Get webhook URL lazily to avoid issues during DAG import."""
    global WEBHOOK_URL
    if WEBHOOK_URL is None:
        try:
            WEBHOOK_URL = Variable.get("gchat_webhook_url") or None
        except Exception:
            WEBHOOK_URL = None
    return WEBHOOK_URL


# ──────────────────────────────────────────────
# 🔧 Helper Functions
# ──────────────────────────────────────────────
def get_config(**context) -> dict[str, Any]:
    """Get configuration from DAG params, Airflow Variables, or use defaults."""
    config = DEFAULT_CONFIG.copy()
    
    # First, try to get from DAG run params
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf:
        params = dag_run.conf
        if params.get("staging_schema"):
            config["staging_schema"] = params["staging_schema"]
        if params.get("prod_schema"):
            config["prod_schema"] = params["prod_schema"]
        if params.get("mysql_user"):
            config["mysql_user"] = params["mysql_user"]
        if params.get("mysql_pass"):
            config["mysql_pass"] = params["mysql_pass"]
        if params.get("mysql_host"):
            config["mysql_host"] = params["mysql_host"]
    
    # Then, try to get from Airflow Variables (if not in params)
    try:
        if not (dag_run and dag_run.conf and dag_run.conf.get("staging_schema")):
            config["staging_schema"] = Variable.get("cdc_staging_schema", default_var=config["staging_schema"])
        if not (dag_run and dag_run.conf and dag_run.conf.get("prod_schema")):
            config["prod_schema"] = Variable.get("cdc_prod_schema", default_var=config["prod_schema"])
        if not (dag_run and dag_run.conf and dag_run.conf.get("mysql_user")):
            config["mysql_user"] = Variable.get("mysql_user", default_var=config["mysql_user"])
        if not (dag_run and dag_run.conf and dag_run.conf.get("mysql_pass")):
            config["mysql_pass"] = Variable.get("mysql_pass", default_var=config["mysql_pass"])
        if not (dag_run and dag_run.conf and dag_run.conf.get("mysql_host")):
            config["mysql_host"] = Variable.get("mysql_host", default_var=config["mysql_host"])
    except Exception as e:
        logger.warning("Could not load Airflow Variables, using defaults: %s", e)
    
    return config


def get_run_date(**context) -> str:
    """Get run_date from DAG params, execution date, or default to yesterday."""
    dag_run = context.get("dag_run")
    
    # Try DAG run conf first
    if dag_run and dag_run.conf and dag_run.conf.get("run_date"):
        return dag_run.conf["run_date"]
    
    # Try execution date (from Airflow context)
    execution_date = context.get("execution_date")
    if execution_date:
        if isinstance(execution_date, str):
            return execution_date
        elif hasattr(execution_date, "strftime"):
            return execution_date.strftime("%Y-%m-%d")
    
    # Default to yesterday
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def get_end_date(**context) -> Optional[str]:
    """Get end_date from DAG params. If None, returns None (parser will run till today)."""
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    
    # Get params from dag_run.conf (manual trigger) or DAG params (scheduled)
    params = {}
    if dag_run and dag_run.conf:
        params = dag_run.conf
    elif dag and dag.params:
        params = dag.params
    
    end_date = params.get("end_date")
    if end_date:
        return end_date
    
    # If None, return None (parser will default to today)
    return None


# ──────────────────────────────────────────────
# 🧠 Webhook Notification Logic
# ──────────────────────────────────────────────
def _send_webhook(context, status: str, task_name: str = None):
    """Send webhook alert for task success or failure."""
    dag = context.get("dag")
    ti = context.get("task_instance")
    dag_id = dag.dag_id if dag else "unknown"
    task_id = task_name or (ti.task_id if ti else "unknown")
    run_id = context.get("run_id", "unknown")
    log_url = getattr(ti, "log_url", "<no log url>") if ti else "<no log url>"
    msg = str(context.get("exception", "Task failed")) if status == "FAILED" else "Completed successfully ✅"

    payload = {
        "text": (
            f"*Airflow DAG Alert* 🚨\n"
            f"• *Status:* {status}\n"
            f"• *DAG:* {dag_id}\n"
            f"• *Task:* {task_id}\n"
            f"• *Run ID:* {run_id}\n"
            f"• *Log URL:* {log_url}\n"
            f"• *Message:* {msg}"
        )
    }

    try:
        webhook_url = get_webhook_url()
        if not webhook_url:
            logger.warning("No WEBHOOK_URL configured. Skipping webhook alert.")
            return

        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        logger.info("Webhook alert sent (%s)", status)
    except Exception as e:
        logger.error("Failed to send webhook alert: %s", e)


def on_success_callback(context):
    """Callback for task success."""
    _send_webhook(context, "SUCCESS")


def on_failure_callback(context):
    """Callback for task failure."""
    _send_webhook(context, "FAILED")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_success_callback": on_success_callback,
    "on_failure_callback": on_failure_callback,
}

def preprocessing_task(**context):
    """Preprocessing step - placeholder for any preprocessing logic."""
    run_date = get_run_date(**context)
    logger.info("Preprocessing for run_date=%s", run_date)
    # TODO: Add real preprocessing here (e.g., cleanup temp tables, archive logs, etc.)
    logger.info("Preprocessing completed")



# ──────────────────────────────────────────────
# 🗓️ DAG Definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="diff_automation",
    description="DIFF Automation Pipeline",
    # schedule="0 9 * * *",  # Daily at 9:00 AM (adjust as needed)
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 4, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["diff", "mssql", "automation", "data-pipeline"],
    params={
        "run_date": None,  # If None, defaults to yesterday
        "end_date": None,  # If None, defaults to today (for cdc_parser date range)
        "staging_schema": None,
        "prod_schema": None,
        "num_workers": 10,  # Number of workers to spin up (before deid_setup)
        "pg_db_name": None,  # PostgreSQL database name for Django (defaults to "deid_staging")
        "default_offset_value": None,  # Default offset value for deidentification (defaults to 34)
        "stop_after_cdc_restore": False,  # If True, stop after cdc_restore completes (skip downstream tasks)
    },
) as dag:

    # ──────────────────────────────────────────────
    # 📋 Task 1: Preprocessing
    # ──────────────────────────────────────────────
    preprocessing = PythonOperator(
        task_id="preprocessing",
        python_callable=preprocessing_task,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 12: Update Django Settings
    # ──────────────────────────────────────────────
    def update_django_settings(**context):
        """Update Django settings.py with pg_db_name and set DEFAULT_OFFSET_VALUE environment variable."""
        dag_run = context.get("dag_run")
        dag = context.get("dag")
        
        # Get params from dag_run.conf (manual trigger) or DAG params (scheduled)
        params = {}
        if dag_run and dag_run.conf:
            params = dag_run.conf
        elif dag and dag.params:
            params = dag.params
        
        # Get pg_db_name from params or default
        pg_db_name = params.get("pg_db_name") or "arizona_metadata"
        # Get default_offset_value from params or default (convert to int)
        default_offset_value = int(params.get("default_offset_value") or 30)
        
        # Set environment variable
        os.environ["DEFAULT_OFFSET_VALUE"] = str(default_offset_value)
        logger.info("Set DEFAULT_OFFSET_VALUE environment variable to: %s", default_offset_value)
        
        # Path to Django settings.py
        settings_file = os.path.join(
            BASE_REPO_DIR,
            "deIdentification",
            "deIdentification",
            "settings.py",
        )
        
        if not os.path.exists(settings_file):
            logger.warning("Django settings.py not found at %s, skipping update", settings_file)
            return
        
        # Read current settings
        try:
            with open(settings_file, "r", encoding="utf-8") as f:
                content = f.read()
            
            # Update DATABASES["default"]["NAME"]
            import re
            # Pattern to match: "NAME": os.getenv("DE_IDENTIFICATION_DB_NAME", "deid_staging"),
            # Handles both single and double quotes
            pattern_name = r'("NAME":\s*os\.getenv\("DE_IDENTIFICATION_DB_NAME",\s*)(?:"[^"]*"|\'[^\']*\')'
            replacement_name = f'\\1"{pg_db_name}"'
            content = re.sub(pattern_name, replacement_name, content)
            
            # Update DEFAULT_OFFSET_VALUE
            # Pattern to match: DEFAULT_OFFSET_VALUE = int(os.environ.get("DEFAULT_OFFSET_VALUE", 34))
            pattern_offset = r'(DEFAULT_OFFSET_VALUE\s*=\s*int\(os\.environ\.get\("DEFAULT_OFFSET_VALUE",\s*)\d+\)'
            replacement_offset = f'\\1{default_offset_value})'
            content = re.sub(pattern_offset, replacement_offset, content)
            
            # Write back
            with open(settings_file, "w", encoding="utf-8") as f:
                f.write(content)
            
            logger.info("Updated Django settings.py: pg_db_name=%s, default_offset_value=%s", pg_db_name, default_offset_value)
        except Exception as e:
            logger.error("Failed to update Django settings.py: %s", e)
            raise

    update_django_settings_task = PythonOperator(
        task_id="update_django_settings",
        python_callable=update_django_settings,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 13: Setup PostgreSQL Database and Run Migrations
    # ──────────────────────────────────────────────
    def setup_postgres_db(**context):
        """Drop PostgreSQL database if exists, create it, and run Django migrations."""
        dag_run = context.get("dag_run")
        dag = context.get("dag")
        
        # Get params from dag_run.conf (manual trigger) or DAG params (scheduled)
        params = {}
        if dag_run and dag_run.conf:
            params = dag_run.conf
        elif dag and dag.params:
            params = dag.params
        
        # Get pg_db_name from params or default
        pg_db_name = params.get("pg_db_name") or "deid_staging"
        
        # Get PostgreSQL connection details from environment variables (same as Django settings)
        pg_user = os.getenv("DE_IDENTIFICATION_DB_USER", "postgres")
        pg_pass = os.getenv("DE_IDENTIFICATION_DB_PASSWORD", "postgres")
        pg_host = os.getenv("DE_IDENTIFICATION_DB_HOST", "localhost")
        pg_port = os.getenv("DE_IDENTIFICATION_DB_PORT", "5432")
        
        logger.info("Setting up PostgreSQL database: %s", pg_db_name)
        
        # Step 1: Drop database if exists
        import psycopg2
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        from psycopg2 import sql
        
        # Validate database name (alphanumeric and underscore only, PostgreSQL identifier rules)
        # PostgreSQL identifiers can contain letters, digits, and underscores, and must start with letter or underscore
        if not pg_db_name or not (pg_db_name[0].isalpha() or pg_db_name[0] == "_"):
            raise ValueError(f"Invalid database name: {pg_db_name}. Must start with letter or underscore.")
        if not all(c.isalnum() or c == "_" for c in pg_db_name):
            raise ValueError(f"Invalid database name: {pg_db_name}. Only alphanumeric and underscore allowed.")
        
        try:
            # Connect to postgres database (not the target db) to drop/create
            conn = psycopg2.connect(
                host=pg_host,
                port=pg_port,
                user=pg_user,
                password=pg_pass,
                database="postgres"  # Connect to default postgres database
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # Terminate existing connections to the database (using parameterized query)
            cursor.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid();",
                (pg_db_name,)
            )
            
            # Drop database if exists (using sql.Identifier for safe quoting)
            # Note: DROP/CREATE DATABASE don't support parameterized queries, but we validate the name
            drop_query = sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(pg_db_name))
            cursor.execute(drop_query)
            logger.info("Dropped database %s (if it existed)", pg_db_name)
            
            # Create database (using sql.Identifier for safe quoting)
            create_query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(pg_db_name))
            cursor.execute(create_query)
            logger.info("Created database %s", pg_db_name)
            
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error("Failed to setup PostgreSQL database: %s", e)
            raise
        
        # Step 2: Run Django migrations
        django_project_dir = os.path.join(BASE_REPO_DIR, "deIdentification")
        manage_py = os.path.join(django_project_dir, "manage.py")
        
        if not os.path.exists(manage_py):
            raise RuntimeError(f"Django manage.py not found at {manage_py}")
        
        # Set environment variables for Django (settings.py already updated with pg_db_name)
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        
        migrate_cmd = [PYTHON_EXECUTABLE, manage_py, "migrate"]
        
        logger.info("Running Django migrations for database: %s", pg_db_name)
        logger.debug("Command: %s", " ".join(migrate_cmd))
        
        result = subprocess.run(
            migrate_cmd,
            check=False,
            cwd=django_project_dir,
            capture_output=True,
            text=True,
        )
        
        if result.returncode != 0:
            logger.error("Migration output: %s", result.stdout)
            logger.error("Migration errors: %s", result.stderr)
            raise RuntimeError(f"Django migrations failed with exit code {result.returncode}")
        
        logger.info("Django migrations completed successfully")
        logger.debug("Migration output: %s", result.stdout)

    setup_postgres_db_task = PythonOperator(
        task_id="setup_postgres_db",
        python_callable=setup_postgres_db,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 14: Start Workers (before deid_setup)
    # ──────────────────────────────────────────────
    start_workers_script = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts", "start_workers.sh"))
    start_workers = BashOperator(
        task_id="start_workers",
        bash_command=f"bash {start_workers_script} {{{{ params.num_workers }}}}",
    )

    # ──────────────────────────────────────────────
    # 📋 Task 15: Deidentification Setup
    # ──────────────────────────────────────────────
    def run_deid_setup(**context):
        """Run deidentification setup script."""
        config = get_config(**context)
        
        # Get pg_db_name from params (same as used for PostgreSQL database setup)
        dag_run = context.get("dag_run")
        dag = context.get("dag")
        params = {}
        if dag_run and dag_run.conf:
            params = dag_run.conf
        elif dag and dag.params:
            params = dag.params
        
        pg_db_name = params.get("pg_db_name") or "deid_staging"
        
        # Get deidentification configuration from context or defaults
        deid_config = {}
        if dag_run and dag_run.conf:
            deid_config = dag_run.conf.get("deid_config", {})
        original_schema = deid_config.get("original_schema", config["staging_schema"])
        deid_schema = deid_config.get("deid_schema", f"{config['staging_schema']}_deidentified")
        mapping_schema = deid_config.get("mapping_schema", "mapping_oct")
        master_schema = deid_config.get("master_schema", "master_oct")
        phi_csv_path = deid_config.get(
            "phi_csv_path",
            os.path.join(BASE_REPO_DIR, "NOTEBOOK", "DENT", "emd_serono_phi.csv"),
        )
        is_rerun = deid_config.get("is_rerun", True)
        
        setup_script = os.path.join(SCRIPT_DIR, "..", "scripts", "deid_setup.py")
        setup_script = os.path.abspath(setup_script)
        
        cmd = [
            PYTHON_EXECUTABLE,
            setup_script,
            "--db_name", pg_db_name,
            "--original_schema", original_schema,
            "--deid_schema", deid_schema,
            "--mapping_schema", mapping_schema,
            "--master_schema", master_schema,
            "--phi_csv_path", phi_csv_path,
            "--is_rerun", True
        ]
        
        if is_rerun:
            cmd.append("--is_rerun")
        
        logger.info("Running Deidentification Setup")
        logger.debug("Command: %s", " ".join(cmd))
        
        result = subprocess.run(cmd, check=False, cwd=os.path.dirname(setup_script))
        
        if result.returncode != 0:
            raise RuntimeError(f"Deidentification Setup failed with exit code {result.returncode}")
        
        logger.info("Deidentification Setup completed successfully")
    
    deid_setup = PythonOperator(
        task_id="deid_setup",
        python_callable=run_deid_setup,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 16: Deidentification Run
    # ──────────────────────────────────────────────
    def run_deid_run(**context):
        """Run deidentification run script."""
        config = get_config(**context)

        # Get pg_db_name from params (same as used for PostgreSQL database setup)
        dag_run = context.get("dag_run")
        dag = context.get("dag")
        params = {}
        if dag_run and dag_run.conf:
            params = dag_run.conf
        elif dag and dag.params:
            params = dag.params
        
        pg_db_name = params.get("pg_db_name") or "deid_staging"

        # Get deidentification configuration from context or defaults
        deid_config = {}
        if dag_run and dag_run.conf:
            deid_config = dag_run.conf.get("deid_config", {})
        table_status = deid_config.get("table_status", 0)

        run_script = os.path.join(SCRIPT_DIR, "..", "scripts", "deid_run.py")
        run_script = os.path.abspath(run_script)
        
        cmd = [
            PYTHON_EXECUTABLE,
            run_script,
            "--db_name",
            pg_db_name,
            "--table_status",
            str(table_status),
        ]

        logger.info("Running Deidentification")
        logger.debug("Command: %s", " ".join(cmd))

        result = subprocess.run(cmd, check=False, cwd=os.path.dirname(run_script))

        if result.returncode != 0:
            raise RuntimeError(f"Deidentification Run failed with exit code {result.returncode}")

        logger.info("Deidentification Run completed successfully")

    deid_run = PythonOperator(
        task_id="deid_run",
        python_callable=run_deid_run,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 17: Deidentified Data Merge
    # ──────────────────────────────────────────────
    def run_deid_merge(**context):
        """Merge deidentified data from deidentified staging schema to base deidentified schema."""
        config = get_config(**context)
        
        # Get deidentification configuration from context or defaults
        dag_run = context.get("dag_run")
        deid_config = {}
        if dag_run and dag_run.conf:
            deid_config = dag_run.conf.get("deid_config", {})
        
        deid_schema = deid_config.get("deid_schema", f"{config['staging_schema']}_deidentified")
        deid_prod_schema = deid_config.get("deid_prod_schema", "deidentified_oct")
        
        merge_script = os.path.join(CDC_DIR, "cdc_merge.py")
        cmd = [
            PYTHON_EXECUTABLE,
            merge_script,
            "--staging_schema", deid_schema,
            "--prod_schema", deid_prod_schema,
        ]
        
        logger.info("Running Deidentified Data Merge")
        logger.debug("Command: %s", " ".join(cmd))
        logger.info("Merging from %s to %s", deid_schema, deid_prod_schema)
        
        result = subprocess.run(cmd, check=False, cwd=CDC_DIR)
        
        if result.returncode != 0:
            raise RuntimeError(f"Deidentified Data Merge failed with exit code {result.returncode}")
        
        logger.info("Deidentified Data Merge completed successfully")
    
    deid_merge = PythonOperator(
        task_id="deid_merge",
        python_callable=run_deid_merge,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 18: Add Unique Constraint
    # ──────────────────────────────────────────────
    def run_add_unique_constraint(**context):
        """Add UNIQUE constraint on nd_auto_increment_id column for all tables in deidentified schema."""
        config = get_config(**context)
        
        # Get deidentification configuration from context or defaults
        dag_run = context.get("dag_run")
        deid_config = {}
        if dag_run and dag_run.conf:
            deid_config = dag_run.conf.get("deid_config", {})
        
        deid_prod_schema = deid_config.get("deid_prod_schema", "deidentified_oct")
        
        unique_script = os.path.join(SCRIPT_DIR, "..", "scripts", "add_unique_constraint.py")
        unique_script = os.path.abspath(unique_script)
        
        cmd = [
            PYTHON_EXECUTABLE,
            unique_script,
            "--schema", deid_prod_schema,
        ]
        
        logger.info("Running Add Unique Constraint")
        logger.debug("Command: %s", " ".join(cmd))
        logger.info("Schema: %s - deduplicating and adding UNIQUE constraint on nd_auto_increment_id", deid_prod_schema)
        
        result = subprocess.run(cmd, check=False, cwd=os.path.dirname(unique_script))
        
        if result.returncode != 0:
            raise RuntimeError(f"Add Unique Constraint failed with exit code {result.returncode}")
        
        logger.info("Add Unique Constraint completed successfully")
    
    add_unique_constraint = PythonOperator(
        task_id="add_unique_constraint",
        python_callable=run_add_unique_constraint,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 19: GCP Transfer
    # ──────────────────────────────────────────────
    def run_gcp_transfer(**context):
        """Dump deidentified tables to SQL files and upload to Google Cloud Storage."""
        config = get_config(**context)
        run_date = get_run_date(**context)
        
        # Get deidentification configuration from context or defaults
        dag_run = context.get("dag_run")
        deid_config = {}
        if dag_run and dag_run.conf:
            deid_config = dag_run.conf.get("deid_config", {})
        
        deid_schema = deid_config.get("deid_schema", f"{config['staging_schema']}_deidentified")
        
        # Use mobiledoc_staging_deidentified as the source schema for transfer (where all tables are present)
        transfer_schema = deid_config.get("transfer_schema", deid_schema)
        
        bucket_name = deid_config.get("bucket_name", "nd-platform-dent")
        base_path = deid_config.get("base_path", "/Volumes/NDAIVol/GCP Transfer")
        destination_folder = deid_config.get("destination_folder", None)
        
        transfer_script = os.path.join(SCRIPT_DIR, "..", "scripts", "gcp_transfer.py")
        transfer_script = os.path.abspath(transfer_script)
        
        cmd = [
            PYTHON_EXECUTABLE,
            transfer_script,
            "--schema", transfer_schema,
            "--run_date", run_date,
            "--bucket_name", bucket_name,
            "--base_path", base_path,
        ]
        
        if destination_folder:
            cmd.extend(["--destination_folder", destination_folder])
        
        logger.info("Running GCP Transfer")
        logger.debug("Command: %s", " ".join(cmd))
        logger.info(
            "Transferring from schema: %s, run_date: %s, base_path: %s, bucket: %s",
            transfer_schema, run_date, base_path, bucket_name,
        )
        
        result = subprocess.run(cmd, check=False, cwd=os.path.dirname(transfer_script))
        
        if result.returncode != 0:
            raise RuntimeError(f"GCP Transfer failed with exit code {result.returncode}")
        
        logger.info("GCP Transfer completed successfully")
    
    gcp_transfer = PythonOperator(
        task_id="gcp_transfer",
        python_callable=run_gcp_transfer,
    )

    # ──────────────────────────────────────────────
    # 🔗 Task Dependencies
    # ──────────────────────────────────────────────
    # Sequential: preprocessing >> create_cdc_table >> cdc_parser >> add_nd_auto_increment_id >> reset_staging >> cdc_restore
    preprocessing >> update_django_settings_task
    update_django_settings_task >> setup_postgres_db_task >> start_workers >> deid_setup
    
    # Deidentification workflow: setup >> run
    deid_setup >> deid_run
    
    # GCP transfer and deid_merge run in parallel after deid_run completes
    deid_run >> [gcp_transfer, deid_merge]
    
    # Add unique constraint after deid_merge completes
    deid_merge >> add_unique_constraint