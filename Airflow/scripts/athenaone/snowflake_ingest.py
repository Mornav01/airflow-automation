#!/usr/bin/env python3
"""
snowflake_ingest.py — AthenaOne ingestion (Snowflake → MySQL), 11-step runner.

Standalone port of the reference repo's ``manual_pipeline_runner.py``
(athenaone_airflow_automation), decoupled from Django:

  * config comes from ``ingest/config_loader.py`` (env-driven, no SchedulerConfig)
  * the Django ``register_dump_in_queue()`` handoff is dropped — mapping/master
    run off the config-driven v5 scripts, not the queue
  * in-script Google-Chat notifications are dropped — alerting is handled by the
    Airflow task callbacks in dags/shared/notifications.py
  * the reference's separate "fix two tables" + "extract two tables" workaround
    is dropped: it only existed because CLOB columns were mapped to TEXT (64 KB)
    and NUMBER(5,0) to SMALLINT. Step 2's extractor now maps VARCHAR(16777216)→
    LONGTEXT and NUMBER(5,0)→INT (matching the airflow extract_table), so the
    single extraction pass handles INTERFACEMESSAGEDATACLOB/MEDICATION correctly
    — no re-extraction, no double-pull.

Steps (abort on the first failure, exit non-zero so the Airflow task fails):
  1. Recreate incremental schema       7. Update nd_auto_increment_id
  2. Extract from Snowflake            8. Ensure merge indexes
  3. Appointment table alter           9. Set historical flags to N
  4. Extract scheduling schema        10. Merge to historical
  5. Extract financials schema        11. Finalize active flags
  6. Add extraction date column

Steps 8–11 maintain AthenaOne's SCD-2 nd_active_flag in the historical schema
(one active row per PK). This is where AthenaOne's merge differs from the other
EHRs, which use the shared state_diff_merge_v3 instead. See
ingest/active_flag_merge.py.

Usage:  python snowflake_ingest.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

from sqlalchemy import create_engine, text

# The step modules import ``from config_loader import ...`` and one another by
# bare name, so ingest/ must be importable as a flat directory.
_INGEST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ingest")
sys.path.insert(0, _INGEST_DIR)

from config_loader import (  # noqa: E402 — must follow sys.path insert
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    INCREMENTAL_SCHEMA,
)


def _log(msg: str) -> None:
    print(msg, flush=True)


def _server_engine():
    """Engine bound to the MySQL server (no database) for CREATE/DROP DATABASE."""
    return create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/")


def _run_step(num: int, total: int, name: str, fn) -> None:
    _log(f"\n{'=' * 80}\nSTEP {num}/{total}: {name.upper()}\n{'=' * 80}")
    fn()
    _log(f"✅ Step {num} completed successfully")


def main() -> int:
    start = datetime.now()
    _log("\n" + "=" * 80)
    _log("🚀 STARTING ATHENAONE INGESTION (Snowflake → MySQL)")
    _log("=" * 80)

    # The step modules open SQLAlchemy engines bound to INCREMENTAL_SCHEMA at
    # *import time*, so the database must exist before we import them.
    _log(f"🔍 Ensuring database `{INCREMENTAL_SCHEMA}` exists...")
    eng = _server_engine()
    with eng.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{INCREMENTAL_SCHEMA}`"))
    eng.dispose()

    _log("📦 Importing ingestion step modules...")
    from snowflake_to_mysql_extraction import main as extract_main
    from appointment_alter import main as appointment_alter_main
    from sf_to_mysql_scheduling_schema import main as scheduling_main
    from sf_to_mysql_financials_schema import main as financials_main
    from nd_extracted_date_column import main as add_date_main
    from update_nd_auto_inc_id import main as update_nd_id_main
    from merging import main as merge_main
    from active_flag_merge import ensure_indexes, set_historical_flags, finalize_active_flags

    def step1_recreate_schema():
        e = _server_engine()
        with e.connect() as conn:
            _log(f"🗑️  Dropping schema `{INCREMENTAL_SCHEMA}`")
            conn.execute(text(f"DROP DATABASE IF EXISTS `{INCREMENTAL_SCHEMA}`"))
            _log(f"🆕 Creating fresh schema `{INCREMENTAL_SCHEMA}`")
            conn.execute(text(f"CREATE DATABASE `{INCREMENTAL_SCHEMA}`"))
        e.dispose()

    steps = [
        ("Recreate Incremental Schema", step1_recreate_schema),
        ("Extract from Snowflake", extract_main),
        ("Appointment Table Alter", appointment_alter_main),
        ("Extract Scheduling Schema", scheduling_main),
        ("Extract Financials Schema", financials_main),
        ("Add Extraction Date", add_date_main),
        ("Update ND Auto Increment IDs", update_nd_id_main),
        # AthenaOne SCD-2 (nd_active_flag) — see ingest/active_flag_merge.py.
        ("Ensure Merge Indexes", ensure_indexes),
        ("Set Historical Flags to N", set_historical_flags),
        ("Merge to Historical", merge_main),
        ("Finalize Active Flags", finalize_active_flags),
    ]

    total = len(steps)
    for i, (name, fn) in enumerate(steps, start=1):
        try:
            _run_step(i, total, name, fn)
        except Exception as e:  # noqa: BLE001 — surface any step failure and abort
            _log(f"❌ Step {i} ({name}) failed: {e}")
            return 1

    duration = str(datetime.now() - start).split(".")[0]
    _log("\n" + "=" * 80)
    _log(f"🎉 ATHENAONE INGESTION COMPLETED — all {total} steps ok (took {duration})")
    _log("=" * 80 + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
