"""
daily_restore_dag — standalone MSSQL transaction-log restore for this EHR machine.

An identical copy lives in every MSSQL EHR dags folder (greenway / ecw / mind)
because each machine points Airflow's ``dags_folder`` at its own EHR subfolder.
The restore is the same for all of them — apply the .trn logs dumped by the
daily_dump DAG — so behaviour is driven entirely by the RESTORE_* env vars
(RESTORE_DB_CONFIGS) and the shared scripts/restore_incremental_logs.py.

Two stages, matching the pipeline's restore guard:

    check_db_availability  →  restore

check_db_availability fails loudly if any target DB is not ONLINE or has active
user sessions (a deid/diff-merge job or another restore in flight), because the
restore flips each DB into SINGLE_USER / RESTORING mode and would otherwise
disrupt that work.  Both stages honour DUMP-independent skip via RESTORE_SKIP.

This runs the *same* restore that the pipeline can also run (still gated by
skip_restore there).  To avoid double-restoring, enable exactly one path per
machine: either this DAG (RESTORE_SKIP unset) with the pipeline's skip_restore
set true, or the pipeline's restore with RESTORE_SKIP=1 here.
"""
from __future__ import annotations

import os
import sys

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Add dags/ to sys.path so the shared package is importable (loads .env as a
# side-effect via shared/__init__.py).  This file sits in an EHR subfolder, so
# the shared package is one level up.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from shared.notifications import default_args  # noqa: E402 — must follow sys.path insert
from shared.tasks import task_check_db_availability, task_restore  # noqa: E402

local_tz = pendulum.timezone("Asia/Kolkata")

_SCHEDULE = os.environ.get("RESTORE_SCHEDULE", "30 1 * * *")  # default 01:30 Asia/Kolkata (after the dump)
_SKIP_RESTORE = os.environ.get("RESTORE_SKIP", "").strip().lower() in ("1", "true", "yes", "on")


with DAG(
    dag_id="daily_restore",
    description="Standalone MSSQL .trn log restore — DB list configured per machine via RESTORE_DB_CONFIGS",
    schedule=_SCHEDULE,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["restore", "daily", "mssql"],
    params={
        "skip_restore": _SKIP_RESTORE,
    },
) as dag:

    check_db_availability = PythonOperator(
        task_id="check_db_availability",
        python_callable=task_check_db_availability,
    )

    restore = PythonOperator(
        task_id="restore",
        python_callable=task_restore,
    )

    check_db_availability >> restore
