from __future__ import annotations

import logging
import os
import subprocess
import sys

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Make scripts/ importable for db_config defaults.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(SCRIPT_DIR, "..", "scripts")))

from db_config import (  # noqa: E402
    STATE_DIR,
    DIFF_MERGE_TABLES_CSV,
    DEID_POST_PROCESS_TABLES_CSV,
)

logger = logging.getLogger(__name__)
local_tz = pendulum.timezone("Asia/Kolkata")

_REPORT_SCRIPT = os.path.abspath(
    os.path.join(SCRIPT_DIR, "..", "scripts", "pipeline_status_report.py")
)
_DEFAULT_OUT = os.path.join(STATE_DIR, "status", "status_report.csv")


def build_status_report(**context):
    """READ-ONLY: shell out to pipeline_status_report.py to roll up per-table state
    into a CSV. Runs no pipeline step — it only reads the JSON state files."""
    params = context["params"]
    out_csv = params.get("out_csv") or _DEFAULT_OUT

    cmd = [
        sys.executable, _REPORT_SCRIPT,
        "--out",                  out_csv,
        "--diff-merge-tables",    params["diff_merge_tables_csv"],
        "--post-process-tables",  params["deid_post_process_tables_csv"],
        "--clinicalbin-tables",   params.get("clinicalbin_tables_csv", ""),
    ]
    if not params.get("include_cnotes", True):
        cmd.append("--no-cnotes")

    result = subprocess.run(cmd, capture_output=True, text=True,
                            env={**os.environ, "PYTHONUNBUFFERED": "1"})
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"pipeline status report failed (rc={result.returncode}):\n"
            f"{result.stderr or result.stdout}"
        )
    return out_csv   # XCom: path to the generated CSV


with DAG(
    dag_id="pipeline_status",
    description="Read-only per-table status report (diff_merge / deidentification / aws_transfer). "
                "Reads state files only — runs no pipeline step.",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    tags=["status", "report", "read-only"],
    params={
        # Empty out_csv → script default ({STATE_DIR}/status/status_report.csv).
        "out_csv":                      "",
        "diff_merge_tables_csv":        DIFF_MERGE_TABLES_CSV,
        "deid_post_process_tables_csv": DEID_POST_PROCESS_TABLES_CSV,
        # ecw leaves this a placeholder → no cnotes rows (auto-skipped, no error).
        "clinicalbin_tables_csv":       os.environ.get("CLINICALBIN_TABLES_CSV", ""),
        "include_cnotes":               True,
    },
) as dag:

    build_status_report_task = PythonOperator(
        task_id="build_status_report",
        python_callable=build_status_report,
    )
