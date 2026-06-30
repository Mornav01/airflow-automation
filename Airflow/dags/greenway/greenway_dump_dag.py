from __future__ import annotations

import logging
import os
import sys

import yaml

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Add dags/ directory to sys.path so the shared package is importable.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from shared.notifications import default_args  # noqa: E402 — must follow sys.path insert
from shared.tasks import _SCRIPTS_DIR, make_daily_dump  # noqa: E402

logger = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Kolkata")

_GREENWAY_SCRIPTS_DIR = os.path.join(_SCRIPTS_DIR, "greenway")


# ──────────────────────────────────────────────────────────────────────────────
# Client config — reused from the pipeline DAG. The daily dump only needs the
# skip_dump flag; everything else is ignored here.
# ──────────────────────────────────────────────────────────────────────────────
PRACTICE_NAME = os.environ.get("PRACTICE_NAME", "")


def _load_client_config() -> dict:
    if not PRACTICE_NAME:
        logger.warning("PRACTICE_NAME not set in .env — dump DAG will use hardcoded defaults")
        return {}
    path = os.path.join(_GREENWAY_SCRIPTS_DIR, "clients", f"{PRACTICE_NAME}.yaml")
    try:
        with open(path) as _f:
            return yaml.safe_load(_f) or {}
    except FileNotFoundError:
        logger.warning("Client config not found: %s — using hardcoded defaults", path)
        return {}


_CLIENT_CFG = _load_client_config()

task_daily_dump = make_daily_dump(_GREENWAY_SCRIPTS_DIR)


# ══════════════════════════════════════════════════════════════════════════════
# DAG definition — runs the SMB dump on a daily schedule, independent of the
# pipeline. The dump only syncs transaction-log backup files; it never touches
# the databases, so it is safe to run while the pipeline (restore/deid) runs.
# ══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="greenway_daily_dump",
    description="Daily SMB sync of Greenway transaction-log backups (dump only; restore runs in the pipeline)",
    schedule="0 1 * * *",  # every day at 01:00 Asia/Kolkata
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=["greenway", "dump", "daily"],
    params={
        "skip_dump": _CLIENT_CFG.get("skip_dump", False),
    },
) as dag:

    daily_dump = PythonOperator(
        task_id="daily_dump",
        python_callable=task_daily_dump,
    )
