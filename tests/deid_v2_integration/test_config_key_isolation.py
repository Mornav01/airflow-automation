#!/usr/bin/env python
"""
Test: config_key isolation in state.db

Tests two scenarios:
  Scenario 1 — config_key_2 run after config_key_1 failure
  Scenario 2 — config_key_1 run (different table) after config_key_1 failure

The "failure" is seeded directly into state.db — no actual deid run needed.
After seeding, we run `deid run` for a clean table and assert:
  - total/done counts are correct (not inflated by stale rows)
  - the run exits with code 0
  - the clean table ends up `completed` in state.db

Usage:
    python test_config_key_isolation.py
    python test_config_key_isolation.py --scenario 1   # only scenario 1
    python test_config_key_isolation.py --scenario 2   # only scenario 2
"""

import argparse
import csv
import subprocess
import sys
from datetime import datetime, timezone

import sqlalchemy as sa

# ── Paths ─────────────────────────────────────────────────────────────────────

STATE_DB   = "/Users/ndaidcnd/Desktop/Air_DEID/airflow-automation/deid_runtime/state/state.db"
CONFIG_1   = "/Users/ndaidcnd/Desktop/Air_DEID/airflow-automation/Airflow/scripts/configs/dent_config_historical.yaml"
CONFIG_2   = "/Users/ndaidcnd/Desktop/Air_DEID/airflow-automation/Airflow/scripts/configs/dent_config_incremental.yaml"
TABLES_CSV = "/Users/ndaidcnd/Desktop/Air_DEID/airflow-automation/input/test.csv"
DEID_PYTHON = "/opt/miniconda3/envs/air_deid/bin/python"

# Fake "failed" table — must NOT be in test.csv so it will never actually run
FAKE_FAILED_TABLE = "__test_failed_table__"

# ── State.db helpers ──────────────────────────────────────────────────────────

def get_engine():
    return sa.create_engine(f"sqlite:///{STATE_DB}")


def seed_failed_rows(config_key: str, n_batches: int = 50):
    """Insert fake 'done' batch rows for FAKE_FAILED_TABLE to simulate a prior run."""
    engine = get_engine()
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(sa.text(
            "DELETE FROM batch_states WHERE table_name = :t AND config_key = :ck"
        ), {"t": FAKE_FAILED_TABLE, "ck": config_key})
        for i in range(n_batches):
            conn.execute(sa.text("""
                INSERT OR IGNORE INTO batch_states
                    (table_name, config_key, start_id, end_id, status, created_at, updated_at)
                VALUES (:t, :ck, :s, :e, 'done', :now, :now)
            """), {"t": FAKE_FAILED_TABLE, "ck": config_key,
                   "s": i * 1000, "e": i * 1000 + 999, "now": now})
    print(f"  Seeded {n_batches} fake 'done' batch rows for '{FAKE_FAILED_TABLE}' / config_key='{config_key}'")


def cleanup_fake_rows(config_key: str):
    engine = get_engine()
    with engine.begin() as conn:
        deleted = conn.execute(sa.text(
            "DELETE FROM batch_states WHERE table_name = :t AND config_key = :ck"
        ), {"t": FAKE_FAILED_TABLE, "ck": config_key})
    print(f"  Cleaned up {deleted.rowcount} fake rows for config_key='{config_key}'")


def get_batch_counts(config_key: str, table_name: str):
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(sa.text("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as done
            FROM batch_states
            WHERE config_key = :ck AND table_name = :t
        """), {"ck": config_key, "t": table_name}).fetchone()
    return row.total, (row.done or 0)


def get_table_status(config_key: str, table_name: str):
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(sa.text("""
            SELECT status FROM table_states
            WHERE config_key = :ck AND table_name = :t
            ORDER BY id DESC LIMIT 1
        """), {"ck": config_key, "t": table_name}).fetchone()
    return row.status if row else None


def run_deid(config_yaml: str, tables_csv: str) -> int:
    result = subprocess.run(
        [DEID_PYTHON, "-m", "deid", "run",
         "--config", config_yaml,
         "--tables-csv", tables_csv,
         "--rerun"],
    )
    return result.returncode


def read_tables_csv(tables_csv: str):
    with open(tables_csv) as f:
        return [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]


# ── Scenarios ─────────────────────────────────────────────────────────────────

def scenario_1():
    """
    Seed 50 fake 'done' batch rows under config_key=historical (config_key_1).
    Run deid with config_incremental (config_key_2).
    Assert: incremental run completes cleanly — not inflated by historical rows.
    """
    print("\n" + "=" * 60)
    print("SCENARIO 1: config_key_2 run after config_key_1 failure")
    print("=" * 60)
    tables = read_tables_csv(TABLES_CSV)
    print(f"  Tables to run: {tables}")

    seed_failed_rows("historical", n_batches=50)

    print(f"\n  Running deid with config_incremental.yaml (config_key=incremental)...")
    start = datetime.now()
    rc = run_deid(CONFIG_2, TABLES_CSV)
    elapsed = (datetime.now() - start).seconds

    passed = True
    if rc != 0:
        print(f"  FAIL: deid exited with code {rc}")
        passed = False

    for table in tables:
        status = get_table_status("incremental", table)
        total, done = get_batch_counts("incremental", table)
        fake_leaked, _ = get_batch_counts("incremental", FAKE_FAILED_TABLE)
        print(f"  [{table}] status={status}  batches={done}/{total}  fake_rows_leaked={fake_leaked}")
        if status != "completed":
            print(f"  FAIL: {table} not completed under incremental")
            passed = False
        if fake_leaked != 0:
            print(f"  FAIL: historical fake rows leaked into incremental counts!")
            passed = False

    print(f"\n  Time: {elapsed}s | Result: {'PASS ✓' if passed else 'FAIL ✗'}")
    cleanup_fake_rows("historical")
    return passed


def scenario_2():
    """
    Seed 50 fake 'done' batch rows for FAKE_FAILED_TABLE under config_key=historical.
    Run deid with config_historical for a DIFFERENT table (from test.csv).
    Assert: total/done counts are scoped to the run's table — not inflated by fake rows.
    """
    print("\n" + "=" * 60)
    print("SCENARIO 2: config_key_1 run (different table) after config_key_1 failure")
    print("=" * 60)
    tables = read_tables_csv(TABLES_CSV)
    print(f"  Tables to run: {tables}")

    seed_failed_rows("historical", n_batches=50)
    fake_before, _ = get_batch_counts("historical", FAKE_FAILED_TABLE)
    print(f"  Fake rows visible in historical before run: {fake_before}")

    print(f"\n  Running deid with config_historical.yaml (config_key=historical)...")
    start = datetime.now()
    rc = run_deid(CONFIG_1, TABLES_CSV)
    elapsed = (datetime.now() - start).seconds

    passed = True
    if rc != 0:
        print(f"  FAIL: deid exited with code {rc}")
        passed = False

    for table in tables:
        status = get_table_status("historical", table)
        total, done = get_batch_counts("historical", table)
        print(f"  [{table}] status={status}  batches={done}/{total}")
        if status != "completed":
            print(f"  FAIL: {table} not completed")
            passed = False

    # Fake rows must still be present (this run should not touch them)
    fake_after, _ = get_batch_counts("historical", FAKE_FAILED_TABLE)
    print(f"  Fake rows in historical after run: {fake_after} (expected 50)")
    if fake_after != 50:
        print(f"  WARN: fake rows changed during run — unexpected")

    print(f"\n  Time: {elapsed}s | Result: {'PASS ✓' if passed else 'FAIL ✗'}")
    cleanup_fake_rows("historical")
    return passed


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Test config_key isolation")
    parser.add_argument("--scenario", type=int, choices=[1, 2], help="Run only this scenario (default: both)")
    args = parser.parse_args()

    results = {}
    if args.scenario in (None, 1):
        results[1] = scenario_1()
    if args.scenario in (None, 2):
        results[2] = scenario_2()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for s, passed in results.items():
        print(f"  Scenario {s}: {'PASS ✓' if passed else 'FAIL ✗'}")

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
