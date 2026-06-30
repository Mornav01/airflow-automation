import csv
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Set

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────
SQLCMD_CONTAINER = os.getenv("SQLCMD_CONTAINER", "02a16329fd95b6a217ca44751f4f9d6bb24b642f416413f2af9b5e46220c65f3")
SQL_SERVER_CONTAINER = os.getenv("SQL_SERVER_CONTAINER", "localhost")
SA_USER = os.getenv("SA_USER", "sa")
SA_PASS = os.getenv("SA_PASS", "")
SQLCMD_BIN = os.getenv("SQLCMD_BIN", "/opt/mssql-tools18/bin/sqlcmd")
DRY_RUN = os.getenv("RESTORE_DRY_RUN", "0").lower() in ("1", "true", "yes")
DATA_LOGS_DIR = os.getenv("DATA_LOGS_DIR", "/Users/ndai/Desktop/Air_DEID/airflow-automation/deid_runtime/dataLogs")

DB_CONFIGS = [
    {
        "db_name": "PrimerecordBin_Latest",
        "standby_file": "/var/opt/mssql/undo/PrimeRecordBin_standby.undo",
        "trn_dir": "/var/opt/mssql/dump/SQLBackups/PrimeRecordBin1697/LOG/",
        "applied_logs_csv": os.path.join(DATA_LOGS_DIR, "applied_logs_PrimeRecordBin.csv"),
    },
    {
        "db_name": "Primerecord_Latest",
        "standby_file": "/var/opt/mssql/undo/PrimeRecord_standby.undo",
        "trn_dir": "/var/opt/mssql/dump/SQLBackups/PrimeRecord1697/LOG/",
        "applied_logs_csv": os.path.join(DATA_LOGS_DIR, "applied_logs_PrimeRecord.csv"),
    },
]


# ──────────────────────────────────────────────────────────────
# SQL helpers
# ──────────────────────────────────────────────────────────────
def run_sqlcmd(sql: str) -> None:
    cmd = [
        "docker", "exec", SQLCMD_CONTAINER, SQLCMD_BIN,
        "-S", SQL_SERVER_CONTAINER, "-U", SA_USER, "-P", SA_PASS,
        "-Q", sql, "-C",
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if proc.returncode != 0:
        raise RuntimeError(f"sqlcmd failed (rc={proc.returncode}): {proc.stderr.strip()}")


def get_trn_files_from_container(container: str, dir_path: str) -> List[str]:
    res = subprocess.run(
        ["docker", "exec", container, "find", dir_path, "-maxdepth", "1", "-type", "f", "-name", "*.trn"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, check=True,
    )
    return [line.strip() for line in res.stdout.splitlines() if line.strip()]


# ──────────────────────────────────────────────────────────────
# Applied-log tracking
# ──────────────────────────────────────────────────────────────
def load_applied_logs(file_path_str: str) -> Set[str]:
    applied: Set[str] = set()
    file_path = Path(file_path_str).expanduser()
    if not file_path.exists():
        return applied
    with file_path.open("r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            trn_val = row.get("trn_file") or row.get("trn") or row.get("file")
            if trn_val:
                applied.add(trn_val)
    return applied


def mark_log_as_applied(trn_file: str, file_path_str: str) -> None:
    file_path = Path(file_path_str).expanduser()
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = file_path.exists()
    with file_path.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(["trn_file", "applied_time_utc"])
        writer.writerow([trn_file, datetime.now(timezone.utc).isoformat()])


# ──────────────────────────────────────────────────────────────
# Restore logic
# ──────────────────────────────────────────────────────────────
def restore_log(db_config: dict, trn_file: str, dry_run: bool = False) -> None:
    sql = (
        f"RESTORE LOG [{db_config['db_name']}] "
        f"FROM DISK = N'{trn_file}' "
        f"WITH STANDBY = N'{db_config['standby_file']}'"
    )
    if dry_run:
        logger.info(f"DRY-RUN: {sql}")
        return
    run_sqlcmd(sql)
    mark_log_as_applied(trn_file, db_config["applied_logs_csv"])


def process_database(config: dict) -> None:
    logger.info(f"Starting restore for: {config['db_name']}")

    trn_files = get_trn_files_from_container(SQLCMD_CONTAINER, config["trn_dir"])
    sorted_files = sorted(
        trn_files,
        key=lambda f: re.search(r"_(\d{8}_\d{6})", f).group(1) if re.search(r"_(\d{8}_\d{6})", f) else f,
    )
    applied = load_applied_logs(config["applied_logs_csv"])
    new_logs = [f for f in sorted_files if f not in applied]

    if not new_logs:
        logger.info(f"No new logs for {config['db_name']}, skipping.")
        return

    # Acquire single-user mode only when there are logs to apply, and always
    # release it — even if an individual restore fails mid-batch.
    run_sqlcmd(f"ALTER DATABASE [{config['db_name']}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;")
    try:
        for trn in new_logs:
            restore_log(config, trn, dry_run=DRY_RUN)
            logger.info(f"Applied: {trn}")
    finally:
        run_sqlcmd(f"ALTER DATABASE [{config['db_name']}] SET MULTI_USER WITH ROLLBACK IMMEDIATE;")

    logger.info(f"Finished restore for: {config['db_name']}")


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    failed = False
    for db_cfg in DB_CONFIGS:
        try:
            process_database(db_cfg)
        except Exception as e:
            logger.error(f"Failed processing {db_cfg['db_name']}: {e}")
            failed = True
    if failed:
        sys.exit(1)
