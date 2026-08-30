from __future__ import annotations
import json
import logging
import os
import subprocess
import tempfile
from datetime import datetime, timedelta
from typing import Any

import pendulum
import requests


from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
try:
    from airflow.sdk import Variable
except ImportError:
    from airflow.models import Variable  # Fallback for Airflow < 3.x
from airflow.models.param import Param

logger = logging.getLogger(__name__)

# Load .env from Airflow/ root (two levels up from dags/dent/) so all os.environ.get()
# calls below pick up real values when Airflow doesn't inherit them from the shell.
def _load_env_file(path: str) -> None:
    try:
        with open(path) as _f:
            for _line in _f:
                _line = _line.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _key, _, _val = _line.partition("=")
                _key = _key.strip()
                _val = _val.strip().strip('"').strip("'")
                os.environ.setdefault(_key, _val)
    except FileNotFoundError:
        logger.warning("No .env file found at %s", path)

_load_env_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env"))

# ──────────────────────────────────────────────
# 🧭 Configuration
# ──────────────────────────────────────────────
local_tz = pendulum.timezone("Asia/Kolkata")

# Base path for the deidentification repo on the Airflow host
BASE_REPO_DIR = os.environ.get("DEID_REPO_DIR", "/path/to/deidentification_v2")

# Script directory (Airflow DAGs location)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# CDC directory lives inside the deidentification repo
CDC_DIR = os.path.join(BASE_REPO_DIR, "CDC", "MySQL")

# Python executable path (optional - if None, uses 'python' from PATH)
# Set this if you need to use a specific Python interpreter
# Example: PYTHON_EXECUTABLE = "/opt/miniconda3/envs/airflow_deid/bin/python"
# If Airflow runs from the conda environment, 'python' should work fine
PYTHON_EXECUTABLE = os.environ.get("DEID_PYTHON", "python")

# Default configuration (can be overridden via Airflow Variables or DAG params)
DEFAULT_CONFIG = {
    "staging_schema":     os.environ.get("STAGING_DB_NAME", ""),
    "prod_schema":        os.environ.get("PROD_DB_NAME", ""),
    "cdc_schema":         os.environ.get("DB_CDC_SCHEMA", "cdc"),
    "mapping_schema":     os.environ.get("MAPPING_DB_NAME", "mapping"),
    "master_schema":      os.environ.get("MASTER_DB_NAME", "master"),
    "deid_staging_schema": os.environ.get("DEID_STAGING_NAME", "deidentified_staging"),
    "deid_prod_schema":   os.environ.get("DEID_BASE_NAME", "deidentified"),
    "mysql_user":         os.environ.get("DB_USER", ""),
    "mysql_pass":         os.environ.get("DB_PASS", ""),  # raw literal — used in mysql CLI -p args
    "mysql_host":         os.environ.get("DB_HOST", "localhost"),
    "mysql_port":         os.environ.get("DB_PORT", "3306"),
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
            WEBHOOK_URL = Variable.get("gchat_webhook_url", default=None)
        except Exception as e:
            logger.warning("Could not fetch gchat_webhook_url from Airflow Variables: %s", e)
            WEBHOOK_URL = None
    return WEBHOOK_URL


# ──────────────────────────────────────────────
# 🔧 Helper Functions
# ──────────────────────────────────────────────
def get_config(**context) -> dict[str, Any]:
    """Get configuration from DAG params, Airflow Variables, or use defaults."""
    config = DEFAULT_CONFIG.copy()

    dag_run = context.get("dag_run")
    dag = context.get("dag")

    # Priority 1: manual-trigger conf (dag_run.conf)
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
    elif dag and dag.params:
        # Priority 2: DAG-level default params (used by scheduled runs)
        dag_params = dag.params
        if dag_params.get("staging_schema"):
            config["staging_schema"] = dag_params["staging_schema"]
        if dag_params.get("prod_schema"):
            config["prod_schema"] = dag_params["prod_schema"]

    # Priority 3: Airflow Variables (only if still unset)
    try:
        if not config["staging_schema"]:
            config["staging_schema"] = Variable.get("cdc_staging_schema", default=config["staging_schema"])
        if not config["prod_schema"]:
            config["prod_schema"] = Variable.get("cdc_prod_schema", default=config["prod_schema"])
        if not config["mysql_user"]:
            config["mysql_user"] = Variable.get("mysql_user", default=config["mysql_user"])
        if not config["mysql_pass"]:
            config["mysql_pass"] = Variable.get("mysql_pass", default=config["mysql_pass"])
        if not config["mysql_host"]:
            config["mysql_host"] = Variable.get("mysql_host", default=config["mysql_host"])
    except Exception as e:
        logger.warning("Could not load Airflow Variables, using defaults: %s", e)

    return config


def get_run_date(**context) -> str:
    """Get run_date from manual conf, or default to yesterday."""
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("run_date"):
        return dag_run.conf["run_date"]
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def get_end_date(**context) -> str:
    """Get end_date from manual conf; falls back to run_date if not provided."""
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("end_date"):
        return dag_run.conf["end_date"]
    return get_run_date(**context)


def _run_streaming(cmd: list, cwd: str = None) -> int:
    """Run a subprocess and stream its output line-by-line to the Airflow task log in real-time."""
    with subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # merge stderr into stdout so one loop handles both
        text=True,
        cwd=cwd,
        bufsize=1,
    ) as proc:
        for line in proc.stdout:
            logger.info(line.rstrip())
    return proc.returncode


def _require_config(config: dict, *fields: str) -> None:
    """Raise ValueError early if any required config field is empty or missing."""
    missing = [f for f in fields if not config.get(f)]
    if missing:
        raise ValueError(
            f"Required config fields are empty or missing: {missing}. "
            "Check environment variables or Airflow Variables."
        )



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
    "retry_delay": timedelta(hours=5),
    "on_success_callback": on_success_callback,
    "on_failure_callback": on_failure_callback,
}

def preprocessing_task(**context):
    """Preprocessing step - placeholder for any preprocessing logic."""
    run_date = get_run_date(**context)
    logger.info("Preprocessing for run_date=%s", run_date)
    # TODO: Add real preprocessing here (e.g., cleanup temp tables, archive logs, etc.)
    logger.info("Preprocessing completed")


def create_cdc_table_task(**context):
    """Create CDC table with date-based name: change_log_{run_date} where run_date is in mmddyyyy format."""
    config = get_config(**context)
    _require_config(config, "mysql_user", "mysql_host")
    run_date = get_run_date(**context)
    
    # Convert run_date from YYYY-MM-DD to mmddyyyy format
    try:
        date_obj = datetime.strptime(run_date, "%Y-%m-%d")
        run_date_formatted = date_obj.strftime("%m%d%Y")
    except ValueError:
        # If already in different format, try to parse or use as-is
        logger.warning("Could not parse run_date %s, using as-is", run_date)
        run_date_formatted = run_date.replace("-", "")
    
    mysql_user = config["mysql_user"]
    mysql_pass = config["mysql_pass"]
    mysql_host = config["mysql_host"]
    cdc_schema = config.get("cdc_schema", "cdc")
    
    table_name = f"change_log_{run_date_formatted}"
    
    # SQL to drop table if exists, then create it fresh
    drop_and_create_sql = (
        f"DROP TABLE IF EXISTS `{cdc_schema}`.`{table_name}`; "
        f"CREATE TABLE `{cdc_schema}`.`{table_name}` ("
        f"`id` bigint NOT NULL AUTO_INCREMENT, "
        f"`table_name` varchar(255) NOT NULL, "
        f"`operation` enum('INSERT','UPDATE','DELETE') NOT NULL, "
        f"`record_data` json NOT NULL, "
        f"`event_time` datetime DEFAULT CURRENT_TIMESTAMP, "
        f"`binlog_file` varchar(255) DEFAULT NULL, "
        f"`binlog_pos` bigint DEFAULT NULL, "
        f"PRIMARY KEY (`id`), "
        f"KEY `idx_table_name` (`table_name`), "
        f"KEY `idx_binfile` (`binlog_file`)"
        f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
    )
    
    cmd = ["mysql", f"-h{mysql_host}", f"-u{mysql_user}", f"-p{mysql_pass}", "-e", drop_and_create_sql]
    
    logger.info("Replacing CDC table: %s.%s (drop if exists, then create)", cdc_schema, table_name)
    logger.debug("Command: mysql -h%s -u%s -p*** -e \"DROP TABLE IF EXISTS... CREATE TABLE...\"", mysql_host, mysql_user)
    
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise RuntimeError(f"Failed to replace CDC table: {result.stderr}")
    
    logger.info("CDC table %s.%s replaced successfully", cdc_schema, table_name)

    # Push run_date to XCom so downstream tasks use the exact same date,
    # even if the pipeline crosses midnight.
    context["task_instance"].xcom_push(key="run_date", value=run_date)

    # Return table name to be pushed to XCom for subsequent tasks
    return table_name


# Tables the downstream delta/decrypt tasks always read from staging, even on
# days when they had no CDC changes.  They must exist (possibly empty) in staging
# or those tasks fail on a missing-table error:
#   mapping_delta          → users, enc, structdemographics, structdatadetail
#   master_pii_delta       → users, patients
#   master_insurance_delta → hcfa, edi_invoice (INNER JOIN — both required)
#   decrypt_pnotes         → progressnotes (raises RuntimeError if absent)
# Keep this in sync if a downstream script starts reading a new staging table.
_STAGING_FLOOR_TABLES = frozenset({
    "users", "enc", "patients",
    "structdemographics", "structdatadetail", "progressnotes",
    "hcfa", "edi_invoice",
})


# Tables whose *encrypted* form must never be de-identified directly — the
# decrypt_pnotes task first materialises a decrypted copy (progressnotes →
# progressnotes_decryptfinal), and it is that decrypted copy the deid rules CSV
# is configured against (NOTES rule on progressnotes_decryptfinal.summary, etc.).
# If the run list still names the encrypted table, the decrypted copy is never
# in tables_to_run and progress notes silently go through un-deidentified.
# Keys/values are compared case-insensitively.
_DEID_TABLE_SUBSTITUTIONS = {
    "progressnotes": "progressnotes_decryptfinal",
}


def _build_deid_tables_csv(src_csv: str, out_dir: str) -> str:
    """Return a tables CSV for the deid run with encrypted tables swapped for their
    decrypted counterparts (see _DEID_TABLE_SUBSTITUTIONS).

    Reads table names the same way the deid CLI does — first column of each line,
    skipping blanks and '#' comments — applies the substitutions, de-dupes while
    preserving order, and writes a fresh one-name-per-line CSV. Returns src_csv
    unchanged if it is empty/missing (the deid runner reports that itself).
    """
    if not src_csv or not os.path.exists(src_csv):
        return src_csv

    import csv as _csv

    names: list[str] = []
    with open(src_csv, newline="") as fh:
        for row in _csv.reader(fh):
            if not row:
                continue
            name = row[0].strip()
            if not name or name.startswith("#"):
                continue
            names.append(name)

    swapped, seen, changed = [], set(), []
    for name in names:
        new = _DEID_TABLE_SUBSTITUTIONS.get(name.lower(), name)
        if new != name:
            changed.append(f"{name} → {new}")
        if new.lower() not in seen:
            seen.add(new.lower())
            swapped.append(new)

    if changed:
        logger.info("Deid tables CSV substitutions applied: %s", ", ".join(changed))
    else:
        logger.info(
            "No deid table substitutions needed; %s not present in run list",
            list(_DEID_TABLE_SUBSTITUTIONS),
        )

    out_path = os.path.join(out_dir, "deid_tables_effective.csv")
    with open(out_path, "w", newline="") as fh:
        writer = _csv.writer(fh)
        for name in swapped:
            writer.writerow([name])
    logger.info("Wrote effective deid tables CSV (%d tables) to %s", len(swapped), out_path)
    return out_path


def _mysql_rows(config: dict, sql: str) -> list[list[str]]:
    """Run a SELECT via the mysql CLI; return rows as tab-split lists of strings."""
    r = subprocess.run(
        ["mysql",
         f"-h{config['mysql_host']}", f"-u{config['mysql_user']}", f"-p{config['mysql_pass']}",
         "--batch", "--skip-column-names", "-e", sql],
        capture_output=True, text=True, check=False,
    )
    if r.returncode != 0:
        raise RuntimeError(f"MySQL query failed: {r.stderr.strip()}")
    return [ln.split("\t") for ln in r.stdout.splitlines() if ln]


def _resolve_cdc_table_name(**context) -> str:
    """Return the change-log table name built by create_cdc_table (XCom, else derive)."""
    ti = context.get("task_instance")
    cdc_table = ti.xcom_pull(task_ids="create_cdc_table") if ti else None
    if cdc_table:
        return cdc_table
    run_date = get_run_date(**context)
    try:
        return f"change_log_{datetime.strptime(run_date, '%Y-%m-%d').strftime('%m%d%Y')}"
    except ValueError:
        return f"change_log_{run_date.replace('-', '')}"


def reset_staging_schema_task(**context):
    """Reset staging schema: drop, recreate, and clone ONLY the tables that have
    deltas in today's change log (plus the required floor set) from prod structure.

    Cloning just the changed subset — instead of the entire prod schema — is safe
    because cdc_restore drives itself off SELECT DISTINCT table_name from the same
    change log and reads its column metadata from staging: any table not in the log
    is never written, so cloning it is pure waste.
    """
    config = get_config(**context)
    _require_config(config, "mysql_user", "mysql_host", "staging_schema", "prod_schema")

    staging_schema = config["staging_schema"]
    prod_schema = config["prod_schema"]
    cdc_schema = config.get("cdc_schema", "cdc")
    mysql_user = config["mysql_user"]
    mysql_pass = config["mysql_pass"]
    mysql_host = config["mysql_host"]

    logger.info("Resetting staging schema: %s", staging_schema)

    # Determine which tables to clone: today's changed tables ∪ floor set.
    cdc_table = _resolve_cdc_table_name(**context)
    changed = {
        row[0].lower()
        for row in _mysql_rows(config, f"SELECT DISTINCT table_name FROM `{cdc_schema}`.`{cdc_table}`")
        if row and row[0]
    }
    wanted = changed | _STAGING_FLOOR_TABLES

    # Keep only names that exist in prod, using prod's exact spelling — change_log
    # can store inconsistent case (e.g. labdata vs LabData) and may reference tables
    # that no longer exist in prod (cdc_restore skips those as "new tables" anyway).
    prod_by_lower = {
        row[0].lower(): row[0]
        for row in _mysql_rows(
            config,
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{prod_schema}' AND table_type = 'BASE TABLE'",
        )
        if row and row[0]
    }
    tables_to_clone = sorted({prod_by_lower[t] for t in wanted if t in prod_by_lower})

    # Tables in the change log that don't exist in prod are NEW tables — there is no
    # prod structure to clone, so skip them here (cdc_restore skips them as "new tables"
    # too). Logging them explicitly avoids confusion / spurious missing-table errors.
    skipped_new = sorted(changed - set(prod_by_lower))
    if skipped_new:
        logger.warning(
            "Skipping %d changed table(s) not present in prod %s (new tables — nothing to clone): %s",
            len(skipped_new), prod_schema, skipped_new,
        )

    logger.info(
        "Cloning %d/%d changed table(s) + floor to staging %s: %s",
        len(tables_to_clone), len(changed), staging_schema, tables_to_clone,
    )

    # Step 1: Drop and create schema
    drop_create_sql = f"DROP SCHEMA IF EXISTS `{staging_schema}`; CREATE SCHEMA `{staging_schema}`;"
    cmd1 = ["mysql", f"-h{mysql_host}", f"-u{mysql_user}", f"-p{mysql_pass}", "-e", drop_create_sql]

    result1 = subprocess.run(cmd1, check=False, capture_output=True, text=True)
    if result1.returncode != 0:
        raise RuntimeError(f"Failed to reset staging schema: {result1.stderr}")

    if not tables_to_clone:
        logger.warning("No tables to clone for %s.%s — staging reset empty", cdc_schema, cdc_table)
        return

    # Step 2: Dump structure to a temp .sql file, restore with --force, log skipped tables.
    with tempfile.NamedTemporaryFile(
        prefix=f"{prod_schema}_structure_", suffix=".sql", delete=False
    ) as tmp:
        sql_file = tmp.name

    try:
        # 2a. Dump schema structure of ONLY the selected tables to file.
        # FK constraints to un-cloned tables are fine: mysqldump's preamble sets
        # FOREIGN_KEY_CHECKS=0, so CREATE TABLE with a dangling FK still succeeds.
        # (--routines is a no-op with a table list; per-table --triggers still apply.)
        dump_cmd = [
            "mysqldump",
            f"-h{mysql_host}", f"-u{mysql_user}", f"-p{mysql_pass}",
            "--no-data", "--routines", "--triggers",
            "--single-transaction", "--quick", "--set-gtid-purged=OFF",
            f"--result-file={sql_file}",
            prod_schema, *tables_to_clone,
        ]
        logger.info("Dumping structure of %d table(s) from %s to %s", len(tables_to_clone), prod_schema, sql_file)
        r = subprocess.run(dump_cmd, check=False, capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"mysqldump failed: {r.stderr}")

        # 2b. Restore dump with --force so MySQL continues past any row-size errors.
        logger.info("Cloning structure from %s to %s", prod_schema, staging_schema)
        with open(sql_file, "rb") as fh:
            result = subprocess.run(
                ["mysql", "--force",
                 f"-h{mysql_host}", f"-u{mysql_user}", f"-p{mysql_pass}",
                 staging_schema],
                stdin=fh, check=False, capture_output=True,
            )

        # 2c. Find requested tables that failed to land in staging.
        staging_tables = {
            row[0].lower()
            for row in _mysql_rows(
                config,
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{staging_schema}' AND table_type = 'BASE TABLE'",
            )
            if row and row[0]
        }
        failed_tables = {t.lower() for t in tables_to_clone} - staging_tables
        if failed_tables:
            logger.warning(
                "Skipped %d requested table(s) that could not be cloned to staging "
                "(row size too large or similar error): %s",
                len(failed_tables), sorted(failed_tables),
            )

        # Surface errors that are not the password warning or expected row-size errors.
        stderr_text = result.stderr.decode("utf-8", errors="replace")
        unexpected = [
            ln for ln in stderr_text.splitlines()
            if ln
            and "password on the command line" not in ln
            and "ERROR 1118" not in ln
        ]
        if unexpected:
            raise RuntimeError(
                "mysql --force clone raised unexpected errors during staging schema clone:\n"
                + "\n".join(unexpected)
            )

    finally:
        try:
            os.remove(sql_file)
        except OSError:
            pass

    logger.info("Staging schema %s reset successfully (%d tables cloned)", staging_schema, len(tables_to_clone))


# ──────────────────────────────────────────────
# 🗓️ DAG Definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="cdc_automation",
    description="CDC Automation Pipeline: Parse binlogs, restore to staging, and merge to prod",
    # Midnight EDT (server time); Airflow interprets cron in DAG timezone (IST)
    # Midnight EDT (machine time) = 05:00 UTC = 10:30 IST; Airflow reads cron in DAG timezone (IST)
    # schedule="30 10 * * *",
    # start_date=pendulum.datetime(2026, 5, 13, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["cdc", "mysql", "automation", "data-pipeline"],
    params={
        "run_date": None,       # If None, defaults to yesterday (YYYY-MM-DD)
        "end_date": None,       # If None, defaults to run_date
        "staging_schema": "mobiledoc_staging",
        "prod_schema": "mobiledoc",
        "parser_workers": "8",
        "stop_after_cdc_restore": Param(False, type="boolean", description="If True, short-circuit downstream tasks after cdc_restore"),
        "skip_init_nd_columns": Param(True, type="boolean", description="If True (default), skip the init_all_nd_columns step"),
        "p0_csv": Param(
            "", type="string",
            description="Path to the P0 tables CSV. When set, cdc_parser only writes rows for "
                        "these tables (or all EXCEPT these when exclude_p0=True); the whole "
                        "pipeline then scopes itself to those tables. Empty = process ALL tables.",
        ),
        "exclude_p0": Param(
            False, type="boolean",
            description="Invert p0_csv: process all tables EXCEPT those listed in the CSV, "
                        "instead of only those listed. No effect when p0_csv is empty.",
        ),
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
    # 📋 Task 2: Create CDC Table (with date-based name)
    # ──────────────────────────────────────────────
    create_cdc_table = PythonOperator(
        task_id="create_cdc_table",
        python_callable=create_cdc_table_task,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 3: CDC Parser
    # ──────────────────────────────────────────────
    def run_cdc_parser(**context):
        """Run CDC parser script."""
        config = get_config(**context)
        run_date = get_run_date(**context)
        end_date = get_end_date(**context)

        # Get the dynamic table name from previous task (XCom) or construct it
        ti = context.get("task_instance")
        cdc_table_name = ti.xcom_pull(task_ids="create_cdc_table")
        if not cdc_table_name:
            # Fallback: construct table name from run_date
            try:
                date_obj = datetime.strptime(run_date, "%Y-%m-%d")
                run_date_formatted = date_obj.strftime("%m%d%Y")
            except ValueError:
                run_date_formatted = run_date.replace("-", "")
            cdc_table_name = f"change_log_{run_date_formatted}"
        
        parser_script = os.path.join(CDC_DIR, "cdc_parser.py")
        cmd = [
            PYTHON_EXECUTABLE,
            parser_script,
            "--table_name", cdc_table_name,
            "--run_date", run_date,
        ]
        
        # Add end_date if provided (if None, parser will default to today)
        if end_date:
            cmd.extend(["--end_date", end_date])
            logger.info("CDC Parser will process from %s to %s", run_date, end_date)
        else:
            logger.info("CDC Parser will process from %s to today (end_date not specified)", run_date)

        # Add parser_workers (max_workers) if provided
        dag_run = context.get("dag_run")
        dag = context.get("dag")
        params = {}
        if dag_run and dag_run.conf:
            params = dag_run.conf
        elif dag and dag.params:
            params = dag.params

        parser_workers = params.get("parser_workers")
        if parser_workers:
            cmd.extend(["--max_workers", str(parser_workers)])
            logger.info("CDC Parser will use %s workers", parser_workers)
        else:
            logger.info("CDC Parser will use default worker count (auto)")

        # P0 table scoping: when a CSV is given, cdc_parser only writes rows for those
        # tables (or all except them when exclude_p0). Every downstream task reads
        # DISTINCT table_name from the change log, so this scopes the whole pipeline.
        p0_csv = (params.get("p0_csv") or "").strip()
        if p0_csv:
            cmd.extend(["--p0_csv", p0_csv])
            if params.get("exclude_p0", False):
                cmd.append("--exclude_p0")
                logger.info("CDC Parser will process ALL tables EXCEPT those in P0 CSV: %s", p0_csv)
            else:
                logger.info("CDC Parser will process ONLY tables in P0 CSV: %s", p0_csv)
        else:
            logger.info("No p0_csv provided — CDC Parser will process ALL tables")

        logger.info("Running CDC Parser")
        logger.debug("Command: %s", " ".join(cmd))

        returncode = _run_streaming(cmd, cwd=CDC_DIR)
        if returncode != 0:
            raise RuntimeError(f"CDC Parser failed with exit code {returncode}")

        logger.info("CDC Parser completed successfully")
    
    cdc_parser = PythonOperator(
        task_id="cdc_parser",
        python_callable=run_cdc_parser,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 4: Init all nd_* audit columns
    # ──────────────────────────────────────────────
    def run_init_nd_columns(**context):
        """Ensure all 4 nd_* audit columns exist on every table that appears in today's CDC log."""
        dag_run = context.get("dag_run")
        dag = context.get("dag")
        params = dag_run.conf if (dag_run and dag_run.conf) else (dag.params if dag else {})
        if params.get("skip_init_nd_columns", True):
            logger.info("skip_init_nd_columns=True — skipping init_all_nd_columns")
            return

        config = get_config(**context)
        run_date = get_run_date(**context)

        ti = context.get("task_instance")
        cdc_table = ti.xcom_pull(task_ids="create_cdc_table")
        if not cdc_table:
            try:
                date_obj = datetime.strptime(run_date, "%Y-%m-%d")
                run_date_formatted = date_obj.strftime("%m%d%Y")
            except ValueError:
                run_date_formatted = run_date.replace("-", "")
            cdc_table = f"change_log_{run_date_formatted}"

        prod_schema = config["prod_schema"]
        cdc_schema  = config.get("cdc_schema", "cdc")

        init_script = os.path.join(CDC_DIR, "init_all_nd_columns.py")
        cmd = [
            PYTHON_EXECUTABLE,
            init_script,
            "--prod_schema",    prod_schema,
            "--extracted_date", "2026-04-11",
            "--updated_at",     run_date,
            "--cdc_schema",     cdc_schema,
            "--cdc_table",      cdc_table,
        ]

        logger.info("Running init_all_nd_columns for tables in CDC log %s.%s", cdc_schema, cdc_table)
        logger.debug("Command: %s", " ".join(cmd))

        returncode = _run_streaming(cmd, cwd=CDC_DIR)
        if returncode != 0:
            raise RuntimeError(f"init_all_nd_columns failed with exit code {returncode}")

        logger.info("init_all_nd_columns completed successfully")

    add_nd_audit_columns = PythonOperator(
        task_id="add_nd_audit_columns",
        python_callable=run_init_nd_columns,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 5: Reset Staging Schema
    # ──────────────────────────────────────────────
    reset_staging = PythonOperator(
        task_id="reset_staging_schema",
        python_callable=reset_staging_schema_task,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 6: CDC Restore
    # ──────────────────────────────────────────────
    def run_cdc_restore(**context):
        """Run CDC restore script."""
        config = get_config(**context)
        _require_config(config, "staging_schema", "prod_schema")
        run_date = get_run_date(**context)
        
        # Get the dynamic table name from previous task (XCom)
        ti = context.get("task_instance")
        cdc_table_name = ti.xcom_pull(task_ids="create_cdc_table")
        if not cdc_table_name:
            # Fallback: construct table name from run_date
            try:
                date_obj = datetime.strptime(run_date, "%Y-%m-%d")
                run_date_formatted = date_obj.strftime("%m%d%Y")
            except ValueError:
                run_date_formatted = run_date.replace("-", "")
            cdc_table_name = f"change_log_{run_date_formatted}"
        
        restore_script = os.path.join(CDC_DIR, "cdc_restore.py")
        cmd = [
            PYTHON_EXECUTABLE,
            restore_script,
            "--run_date", run_date,
            "--table_name", cdc_table_name,
            "--staging_schema", config["staging_schema"],
            "--prod_schema", config["prod_schema"],
        ]
        
        logger.info("Running CDC Restore")
        logger.debug("Command: %s", " ".join(cmd))

        returncode = _run_streaming(cmd, cwd=CDC_DIR)
        if returncode != 0:
            raise RuntimeError(f"CDC Restore failed with exit code {returncode}")

        logger.info("CDC Restore completed successfully")
    
    cdc_restore = PythonOperator(
        task_id="cdc_restore",
        python_callable=run_cdc_restore,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 7b: Check if should continue (ShortCircuit)
    # ──────────────────────────────────────────────
    def should_continue(**context):
        """Check if we should continue after cdc_restore or stop."""
        dag_run = context.get("dag_run")
        dag = context.get("dag")
        
        # Get params from dag_run.conf (manual trigger) or DAG params (scheduled)
        params = {}
        if dag_run and dag_run.conf:
            params = dag_run.conf
        elif dag and dag.params:
            params = dag.params
        
        stop_after = params.get("stop_after_cdc_restore", False)
        
        if stop_after:
            logger.info("stop_after_cdc_restore=True - stopping after cdc_restore")
            return False  # Short-circuit: skip downstream tasks
        else:
            logger.info("stop_after_cdc_restore=False - continuing with downstream tasks")
            return True  # Continue: execute downstream tasks

    check_continue = ShortCircuitOperator(
        task_id="check_continue",
        python_callable=should_continue,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 7: CDC Merge
    # ──────────────────────────────────────────────
    def run_cdc_merge(**context):
        """Run CDC merge script."""
        config = get_config(**context)
        _require_config(config, "staging_schema", "prod_schema")
        
        merge_script = os.path.join(CDC_DIR, "cdc_merge.py")
        cmd = [
            PYTHON_EXECUTABLE,
            merge_script,
            "--staging_schema", config["staging_schema"],
            "--prod_schema", config["prod_schema"],
        ]

        # Add parser_workers (max_workers) if provided
        dag_run = context.get("dag_run")
        dag = context.get("dag")
        params = {}
        if dag_run and dag_run.conf:
            params = dag_run.conf
        elif dag and dag.params:
            params = dag.params

        parser_workers = params.get("parser_workers")
        if parser_workers:
            cmd.extend(["--max_workers", str(parser_workers)])
            logger.info("CDC Merge will use %s workers", parser_workers)
        else:
            logger.info("CDC Merge will use default worker count (auto)")
        
        logger.info("Running CDC Merge")
        logger.debug("Command: %s", " ".join(cmd))

        returncode = _run_streaming(cmd, cwd=CDC_DIR)
        if returncode != 0:
            raise RuntimeError(f"CDC Merge failed with exit code {returncode}")

        logger.info("CDC Merge completed successfully")
    
    cdc_merge = PythonOperator(
        task_id="cdc_merge",
        python_callable=run_cdc_merge,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 8: Mapping Delta Update
    # ──────────────────────────────────────────────
    def run_mapping_delta(**context):
        """Update mapping tables (patient_mapping_table, encounter_mapping_table) with deltas from staging."""
        config = get_config(**context)
        
        # Get deidentification configuration from context or defaults
        dag_run = context.get("dag_run")
        deid_config = {}
        if dag_run and dag_run.conf:
            deid_config = dag_run.conf.get("deid_config", {})
        
        mapping_schema = deid_config.get("mapping_schema") or config.get("mapping_schema", "mapping")
        staging_schema = config["staging_schema"]
        
        mapping_script = os.path.join(CDC_DIR, "mapping_delta.py")
        cmd = [
            PYTHON_EXECUTABLE,
            mapping_script,
            "--mapping_schema", mapping_schema,
            "--staging_schema", staging_schema,
        ]
        
        logger.info("Running Mapping Delta Update")
        logger.debug("Command: %s", " ".join(cmd))
        logger.info("Updating mapping tables in schema: %s, source staging: %s", mapping_schema, staging_schema)

        returncode = _run_streaming(cmd, cwd=os.path.dirname(mapping_script))
        if returncode != 0:
            raise RuntimeError(f"Mapping Delta Update failed with exit code {returncode}")

        logger.info("Mapping Delta Update completed successfully")
    
    mapping_delta = PythonOperator(
        task_id="mapping_delta",
        python_callable=run_mapping_delta,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 9: Master PII Delta Update
    # ──────────────────────────────────────────────
    def run_master_pii_delta(**context):
        """Update master PII table (pii_data_table) with deltas from staging."""
        config = get_config(**context)
        
        # Get deidentification configuration from context or defaults
        dag_run = context.get("dag_run")
        deid_config = {}
        if dag_run and dag_run.conf:
            deid_config = dag_run.conf.get("deid_config", {})
        
        master_schema = deid_config.get("master_schema") or config.get("master_schema", "master")
        mapping_schema = deid_config.get("mapping_schema") or config.get("mapping_schema", "mapping")
        staging_schema = config["staging_schema"]

        pii_script = os.path.join(CDC_DIR, "master_pii_delta.py")
        cmd = [
            PYTHON_EXECUTABLE,
            pii_script,
            "--master_schema", master_schema,
            "--staging_schema", staging_schema,
            "--mapping_schema", mapping_schema,
        ]
        
        logger.info("Running Master PII Delta Update")
        logger.debug("Command: %s", " ".join(cmd))
        logger.info("Updating PII table in schema: %s, source staging: %s", master_schema, staging_schema)

        returncode = _run_streaming(cmd, cwd=os.path.dirname(pii_script))
        if returncode != 0:
            raise RuntimeError(f"Master PII Delta Update failed with exit code {returncode}")

        logger.info("Master PII Delta Update completed successfully")
    
    master_pii_delta = PythonOperator(
        task_id="master_pii_delta",
        python_callable=run_master_pii_delta,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 10: Master Insurance Delta Update
    # ──────────────────────────────────────────────
    def run_master_insurance_delta(**context):
        """Update master insurance table (master_insurance_table) with deltas from staging."""
        config = get_config(**context)
        
        # Get deidentification configuration from context or defaults
        dag_run = context.get("dag_run")
        deid_config = {}
        if dag_run and dag_run.conf:
            deid_config = dag_run.conf.get("deid_config", {})
        
        master_schema = deid_config.get("master_schema") or config.get("master_schema", "master")
        mapping_schema = deid_config.get("mapping_schema") or config.get("mapping_schema", "mapping")
        staging_schema = config["staging_schema"]

        insurance_script = os.path.join(CDC_DIR, "master_insurance_delta.py")
        cmd = [
            PYTHON_EXECUTABLE,
            insurance_script,
            "--master_schema", master_schema,
            "--staging_schema", staging_schema,
            "--mapping_schema", mapping_schema,
        ]
        
        logger.info("Running Master Insurance Delta Update")
        logger.debug("Command: %s", " ".join(cmd))
        logger.info("Updating insurance table in schema: %s, source staging: %s", master_schema, staging_schema)

        returncode = _run_streaming(cmd, cwd=os.path.dirname(insurance_script))
        if returncode != 0:
            raise RuntimeError(f"Master Insurance Delta Update failed with exit code {returncode}")

        logger.info("Master Insurance Delta Update completed successfully")
    
    master_insurance_delta = PythonOperator(
        task_id="master_insurance_delta",
        python_callable=run_master_insurance_delta,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 11: Decrypt Progress Notes (runs in parallel with delta updates)
    # ──────────────────────────────────────────────
    def run_decrypt_pnotes(**context):
        """Decrypt progress notes from staging schema into progressnotes_decryptfinal."""
        config = get_config(**context)
        staging_schema = config["staging_schema"]
        mysql_host = config["mysql_host"]
        mysql_user = config["mysql_user"]
        mysql_pass = config["mysql_pass"]

        decrypt_script = os.path.join(CDC_DIR, "decrypt_pnotes.py")
        cmd = [
            PYTHON_EXECUTABLE,
            decrypt_script,
            "--staging_schema", staging_schema,
            "--mysql_host", mysql_host,
            "--mysql_user", mysql_user,
            "--mysql_pass", mysql_pass,
        ]

        logger.info("Running Decrypt Progress Notes")
        logger.debug("Command: %s", " ".join(cmd))
        logger.info("Staging schema: %s", staging_schema)

        returncode = _run_streaming(cmd, cwd=os.path.dirname(decrypt_script))
        if returncode != 0:
            raise RuntimeError(f"Decrypt progress notes failed with exit code {returncode}")

        logger.info("Decrypt progress notes completed successfully")

    decrypt_pnotes = PythonOperator(
        task_id="decrypt_pnotes",
        python_callable=run_decrypt_pnotes,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 12a: Reset DEID Staging Schema
    # ──────────────────────────────────────────────
    def reset_deid_staging_schema_task(**context):
        """Drop and recreate DEID_STAGING_NAME so previous run data cannot interfere."""
        config = get_config(**context)

        deid_staging_schema = config.get("deid_staging_schema", "deidentified_staging")
        mysql_user = config["mysql_user"]
        mysql_pass = config["mysql_pass"]
        mysql_host = config["mysql_host"]

        logger.info("Resetting deid staging schema: %s", deid_staging_schema)

        drop_create_sql = (
            f"DROP SCHEMA IF EXISTS `{deid_staging_schema}`; "
            f"CREATE SCHEMA `{deid_staging_schema}`;"
        )
        cmd = ["mysql", f"-h{mysql_host}", f"-u{mysql_user}", f"-p{mysql_pass}", "-e", drop_create_sql]

        result = subprocess.run(cmd, check=False, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to reset deid staging schema: {result.stderr}")

        logger.info("Deid staging schema %s reset successfully", deid_staging_schema)

    reset_deid_staging = PythonOperator(
        task_id="reset_deid_staging_schema",
        python_callable=reset_deid_staging_schema_task,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 12b: Deidentification Run
    # ──────────────────────────────────────────────
    def run_deid_run(**_):
        """Run deidentification using manual_deidentification_run_v2.py."""
        deid_script = os.path.abspath(
            os.path.join(SCRIPT_DIR, "..", "..", "scripts", "manual_deidentification_run_v2.py")
        )

        deid_config_file = os.environ.get(
            "DEID_CONFIG_FILE",
            os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", "scripts", "ecw", "ecw_config_incremental.yaml")),
        )
        deid_tables_csv = os.environ.get("DEID_TABLES_CSV", "")

        # Swap encrypted tables for their decrypted counterparts (progressnotes →
        # progressnotes_decryptfinal) so the deid run operates on the table
        # decrypt_pnotes produced, which is what the rules CSV is configured against.
        tmp_dir = tempfile.mkdtemp(prefix="deid_tables_")
        effective_tables_csv = _build_deid_tables_csv(deid_tables_csv, tmp_dir)

        cmd = [
            PYTHON_EXECUTABLE,
            deid_script,
            "--config", deid_config_file,
            "--tables", effective_tables_csv,
        ]

        logger.info("Running Deidentification")
        logger.info("Config : %s", deid_config_file)
        logger.info("Tables : %s (from %s)", effective_tables_csv, deid_tables_csv)
        logger.debug("Command: %s", " ".join(cmd))

        try:
            returncode = _run_streaming(cmd, cwd=os.path.dirname(deid_script))
        finally:
            if effective_tables_csv != deid_tables_csv:
                try:
                    os.remove(effective_tables_csv)
                    os.rmdir(tmp_dir)
                except OSError:
                    pass
        if returncode != 0:
            raise RuntimeError(f"Deidentification Run failed with exit code {returncode}")

        logger.info("Deidentification Run completed successfully")

    deid_run = PythonOperator(
        task_id="deid_run",
        python_callable=run_deid_run,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 13: Deidentified Data Merge
    # ──────────────────────────────────────────────
    def run_deid_merge(**context):
        """Merge deidentified data from deidentified staging schema to base deidentified schema."""
        config = get_config(**context)
        
        # Get deidentification configuration from context or defaults
        dag_run = context.get("dag_run")
        deid_config = {}
        if dag_run and dag_run.conf:
            deid_config = dag_run.conf.get("deid_config", {})
        
        deid_schema = deid_config.get("deid_schema") or config.get("deid_staging_schema", "deidentified_staging")
        deid_prod_schema = deid_config.get("deid_prod_schema") or config.get("deid_prod_schema", "deidentified")

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

        returncode = _run_streaming(cmd, cwd=CDC_DIR)
        if returncode != 0:
            raise RuntimeError(f"Deidentified Data Merge failed with exit code {returncode}")

        logger.info("Deidentified Data Merge completed successfully")
    
    deid_merge = PythonOperator(
        task_id="deid_merge",
        python_callable=run_deid_merge,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 14: Add Unique Constraint
    # ──────────────────────────────────────────────
    def run_add_unique_constraint(**context):
        """Add UNIQUE constraint on nd_auto_increment_id column for all tables in deidentified schema."""
        config = get_config(**context)
        
        # Get deidentification configuration from context or defaults
        dag_run = context.get("dag_run")
        deid_config = {}
        if dag_run and dag_run.conf:
            deid_config = dag_run.conf.get("deid_config", {})
        
        deid_prod_schema = deid_config.get("deid_prod_schema") or config.get("deid_prod_schema", "deidentified")

        unique_script = os.path.join(CDC_DIR, "add_unique_constraint.py")
        cmd = [
            PYTHON_EXECUTABLE,
            unique_script,
            "--schema", deid_prod_schema,
        ]
        
        logger.info("Running Add Unique Constraint")
        logger.debug("Command: %s", " ".join(cmd))
        logger.info("Schema: %s - deduplicating and adding UNIQUE constraint on nd_auto_increment_id", deid_prod_schema)

        returncode = _run_streaming(cmd, cwd=os.path.dirname(unique_script))
        if returncode != 0:
            raise RuntimeError(f"Add Unique Constraint failed with exit code {returncode}")

        logger.info("Add Unique Constraint completed successfully")
    
    add_unique_constraint = PythonOperator(
        task_id="add_unique_constraint",
        python_callable=run_add_unique_constraint,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 15: S3 Transfer
    # ──────────────────────────────────────────────
    def run_s3_transfer(**context):
        """Dump deidentified tables to SQL files and upload to AWS S3."""
        config = get_config(**context)
        run_date = get_run_date(**context)

        # schema is runtime state; everything else (bucket, base_path, creds) lives in .env
        transfer_schema = config.get("deid_staging_schema", "deidentified_staging")

        # Optional worker override from DAG params (shared with other tasks)
        dag_run = context.get("dag_run")
        dag = context.get("dag")
        params = dag_run.conf if (dag_run and dag_run.conf) else (dag.params if dag else {})
        max_workers = params.get("parser_workers", None)

        transfer_script = os.path.join(SCRIPT_DIR, "..", "..", "scripts", "s3_transfer.py")
        try:
            date_folder = datetime.strptime(run_date, "%Y-%m-%d").strftime("%m%d%Y")
        except ValueError:
            raise RuntimeError(f"Invalid run_date format: {run_date}. Expected YYYY-MM-DD")
        destination_prefix = f"tables/{date_folder}"

        cmd = [
            PYTHON_EXECUTABLE,
            transfer_script,
            "--schema", transfer_schema,
            "--run_date", run_date,
            "--destination_prefix", destination_prefix
        ]

        if max_workers:
            cmd.extend(["--max_workers", str(max_workers)])
            logger.info("S3 Transfer will use %s workers", max_workers)

        logger.info("Running S3 Transfer: schema=%s, run_date=%s", transfer_schema, run_date)
        logger.debug("Command: %s", " ".join(cmd))

        returncode = _run_streaming(cmd, cwd=os.path.dirname(transfer_script))
        if returncode != 0:
            raise RuntimeError(f"S3 Transfer failed with exit code {returncode}")

        logger.info("S3 Transfer completed successfully")

    s3_transfer = PythonOperator(
        task_id="s3_transfer",
        python_callable=run_s3_transfer,
    )

    # ──────────────────────────────────────────────
    # 📋 Task 7c: Create daily binlog checkpoint snapshot
    # ──────────────────────────────────────────────
    def run_create_dump_metadata_snapshot(**context):
        """
        After S3 transfer completes, materialise cdc.dump_metadata_{run_date}.
        The next run's cdc_restore reads this table as its binlog lower-bound
        filter instead of the original dump_metadata.
        """
        config = get_config(**context)
        ti = context["task_instance"]
        # Pull the run_date locked in by create_cdc_table so change_log and
        # dump_metadata always share the same date even if the pipeline crosses midnight.
        run_date = ti.xcom_pull(task_ids="create_cdc_table", key="run_date") or get_run_date(**context)

        snapshot_script = os.path.join(CDC_DIR, "create_dump_metadata_snapshot.py")
        cmd = [
            PYTHON_EXECUTABLE,
            snapshot_script,
            "--run_date",    run_date,
            "--schema_name", "mobiledoc",
        ]

        logger.info("Creating binlog checkpoint snapshot for run_date=%s schema=mobiledoc", run_date)
        logger.debug("Command: %s", " ".join(cmd))

        returncode = _run_streaming(cmd, cwd=CDC_DIR)
        if returncode != 0:
            raise RuntimeError(f"create_dump_metadata_snapshot failed with exit code {returncode}")

        logger.info("Binlog checkpoint snapshot created successfully")

    create_dump_metadata_snapshot = PythonOperator(
        task_id="create_dump_metadata_snapshot",
        python_callable=run_create_dump_metadata_snapshot,
    )

    # ──────────────────────────────────────────────
    # 🔗 Task Dependencies
    # ──────────────────────────────────────────────
    # Sequential: preprocessing >> create_cdc_table >> cdc_parser >> add_nd_audit_columns >> reset_staging >> cdc_restore
    preprocessing >> create_cdc_table >> cdc_parser >> add_nd_audit_columns >> reset_staging >> cdc_restore
    
    # Check if we should continue (ShortCircuit will skip downstream if stop_after_cdc_restore=True)
    cdc_restore >> check_continue
    
    # decrypt_pnotes runs before cdc_merge so progressnotes_decryptfinal is updated in prod first
    check_continue >> decrypt_pnotes >> cdc_merge
    cdc_merge >> [mapping_delta, master_pii_delta, master_insurance_delta]

    # After mapping/master updates, reset deid staging schema then run deidentification
    [mapping_delta, master_pii_delta, master_insurance_delta] >> reset_deid_staging >> deid_run

    # S3 transfer and deid_merge run in parallel after deid_run completes
    deid_run >> [s3_transfer, deid_merge]

    # Snapshot runs after s3_transfer — not on the critical path to S3
    # (checkpoint is only needed for the next run's cdc_restore)
    s3_transfer >> create_dump_metadata_snapshot

    # Add unique constraint after deid_merge completes
    deid_merge >> add_unique_constraint
