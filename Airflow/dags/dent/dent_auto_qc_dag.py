"""Auto-QC DAG for dent — run the deid QC framework over a table list, emit summary + findings CSVs.

Per-vendor copy of the auto-QC DAG; only ``_VENDOR`` (and its default paths) differ across
ecw / greenway / athenaone / dent. Runs after the vendor pipeline (or on a schedule) and fails —
firing the GChat alert — if any table fails QC. All paths come from the practice ``.env`` so the
same file works on every Mac Studio.

Requires the deid package with the ``auto-qc`` command installed in ``DEID_PYTHON``'s env.
"""
from __future__ import annotations

import logging
import os
import sys

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Add dags/ directory to sys.path so the shared package is importable.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from shared.notifications import default_args  # noqa: E402 — must follow sys.path insert
from shared.tasks import _SCRIPTS_DIR, task_auto_qc  # noqa: E402

logger = logging.getLogger(__name__)
local_tz = pendulum.timezone("Asia/Kolkata")

_VENDOR = "dent"
_VENDOR_SCRIPTS_DIR = os.path.join(_SCRIPTS_DIR, _VENDOR)
_STATE_DIR = os.environ.get("STATE_DIR", "")

# All overridable via the practice .env.
_CONFIG_YAML = os.environ.get("AUTO_QC_CONFIG", os.path.join(_VENDOR_SCRIPTS_DIR, f"{_VENDOR}_config_incremental.yaml"))
# Reuse the pipeline's existing PHI rules CSV (RULES_CSV) unless a QC-specific one is set.
_RULES_CSV   = os.environ.get("AUTO_QC_RULES_CSV", os.environ.get("RULES_CSV", ""))
_OUT_DIR     = os.environ.get("AUTO_QC_OUT_DIR", os.path.join(_STATE_DIR, "auto_qc", _VENDOR))
# Tables to QC: defaults to the post-process tables list; empty → every table in the rules CSV.
_TABLES_FILE = os.environ.get("AUTO_QC_TABLES_CSV", os.environ.get("DEID_POST_PROCESS_TABLES_CSV", ""))


with DAG(
    dag_id=f"{_VENDOR}_auto_qc",
    description=f"De-identification auto-QC for {_VENDOR} — summary + findings CSVs",
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    tags=[_VENDOR, "deid", "qc"],
    params={
        "config_yaml":                 _CONFIG_YAML,
        "auto_qc_rules_csv":           _RULES_CSV,
        "auto_qc_out_dir":             _OUT_DIR,
        "auto_qc_tables_filepath":     _TABLES_FILE,
        "auto_qc_max_workers":         int(os.environ.get("AUTO_QC_MAX_WORKERS", "4")),
        "auto_qc_residual_backend":    os.environ.get("AUTO_QC_RESIDUAL_BACKEND", "regex"),
        "auto_qc_pii_master_conn_str": os.environ.get("AUTO_QC_PII_MASTER_CONN_STR", ""),
        "auto_qc_delta_after":         os.environ.get("AUTO_QC_DELTA_AFTER", ""),
        "auto_qc_no_gate":             os.environ.get("AUTO_QC_NO_GATE", "").lower() in ("1", "true", "yes"),
    },
) as dag:
    PythonOperator(
        task_id="run_auto_qc",
        python_callable=task_auto_qc,
    )
