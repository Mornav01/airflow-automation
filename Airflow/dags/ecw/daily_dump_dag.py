"""
daily_dump_dag — daily data dump (+ optional unzip) for this EHR machine.

An identical copy of this DAG lives in every EHR dags folder
(greenway / ecw / mind / dent / …) because each machine points Airflow's
``dags_folder`` at its own EHR subfolder, so the DAG must be physically present
there.  The file is the same everywhere — behaviour (SMB shared folder, Azure
Blob, or local-folder check) is chosen entirely by the DUMP_* variables in that
machine's .env and executed by the unified scripts/daily_dump.py.  Deploying to
a new Mac needs no code change: set the .env and this DAG does the right thing.

Tasks:  daily_dump  →  check_unzip_required  →  unzip

Some sources deliver the dump as zip archives.  ``check_unzip_required`` is a
ShortCircuit gate driven by DUMP_UNZIP_ENABLED — where unzipping isn't needed it
skips the ``unzip`` task; where it is, ``unzip`` extracts the archives into a
separate folder (DUMP_UNZIP_* config).
"""
from __future__ import annotations

import os
import sys

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator

# Add dags/ to sys.path so the shared package is importable (loads .env as a
# side-effect via shared/__init__.py).  This file sits in an EHR subfolder, so
# the shared package is one level up.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from shared.notifications import default_args  # noqa: E402 — must follow sys.path insert
from shared.tasks import task_check_unzip_required, task_daily_dump, task_unzip_dump  # noqa: E402

local_tz = pendulum.timezone("Asia/Kolkata")

# Behaviour is env-driven; surface the active source in the tags and gate the
# run with a single DUMP_SKIP flag (per-machine, no client YAML needed).
_SOURCE_TYPE = os.environ.get("DUMP_SOURCE_TYPE", "unset").strip().lower() or "unset"
_SCHEDULE = os.environ.get("DUMP_SCHEDULE", "0 1 * * *")  # default 01:00 Asia/Kolkata
_SKIP_DUMP = os.environ.get("DUMP_SKIP", "").strip().lower() in ("1", "true", "yes", "on")


with DAG(
    dag_id="daily_dump",
    description="Unified daily data dump (+ optional unzip) — configured per machine via DUMP_* env vars",
    schedule=_SCHEDULE,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["dump", "daily", _SOURCE_TYPE],
    params={
        "skip_dump": _SKIP_DUMP,
    },
) as dag:

    daily_dump = PythonOperator(
        task_id="daily_dump",
        python_callable=task_daily_dump,
    )

    check_unzip_required = ShortCircuitOperator(
        task_id="check_unzip_required",
        python_callable=task_check_unzip_required,
    )

    unzip = PythonOperator(
        task_id="unzip",
        python_callable=task_unzip_dump,
    )

    daily_dump >> check_unzip_required >> unzip
