#!/usr/bin/env python
"""
test_db_connection.py — Standalone MSSQL connectivity check.

Reproduces EXACTLY the connections the diff/merge pipeline makes: it imports the
same connection strings from db_config.py (which loads Airflow/.env), so no
credentials are hardcoded here — everything comes from your .env.

Run with the same interpreter the pipeline uses (the new_deid / air_deid conda
env, which has pyodbc + sqlalchemy installed):

    /opt/miniconda3/envs/new_deid/bin/python scripts/test_db_connection.py

It tests each MSSQL database (BASE / PROD / DIFF_STAGING), prints the resolved
host/port/db/user (password masked), and classifies any failure — login failure
vs. login timeout vs. missing driver vs. host unreachable — so you can tell a
bad password apart from a network/driver problem at a glance.
"""
from __future__ import annotations

import os
import re
import sys
import time
import urllib.parse

# db_config lives next to this file and loads Airflow/.env on import.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import sqlalchemy as sa
    from sqlalchemy import text
except ImportError:
    sys.exit("ERROR: sqlalchemy not installed in this interpreter. Use the "
             "new_deid/air_deid conda env python (see DEID_PYTHON in .env).")

try:
    import pyodbc
except ImportError:
    pyodbc = None

try:
    from db_config import BASE_DB, PROD_DB, DIFF_STAGING_DB
except Exception as e:  # noqa: BLE001 — surface any db_config/.env load error clearly
    sys.exit(f"ERROR: could not import connection strings from db_config: {e}\n"
             "Check that Airflow/.env exists and is populated.")


# ── How long to wait before giving up on a login (seconds) ─────────────────────
# Short on purpose so this fails fast instead of hanging like the pipeline does.
LOGIN_TIMEOUT = int(os.environ.get("TEST_LOGIN_TIMEOUT", "8"))


def _mask(conn_str: str) -> str:
    """Redact the password in a SQLAlchemy URL for safe printing."""
    return re.sub(r"(:)([^:@/]+)(@)", r"\1****\3", conn_str)


def _describe(conn_str: str) -> str:
    """Human-readable host:port/db · user, parsed from the URL."""
    try:
        url = sa.engine.make_url(conn_str)
        driver = urllib.parse.parse_qs(url.query.get("driver", "")) or url.query.get("driver", "")
        return (f"host={url.host} port={url.port} db={url.database} "
                f"user={url.username} driver={url.query.get('driver', '?')}")
    except Exception:  # noqa: BLE001
        return _mask(conn_str)


def _with_timeout(conn_str: str) -> str:
    """Append a short login timeout so the test fails fast."""
    sep = "&" if "?" in conn_str else "?"
    return f"{conn_str}{sep}timeout={LOGIN_TIMEOUT}"


def _classify(err: Exception) -> str:
    """Map a driver error to a plain-English cause."""
    msg = str(err)
    if "28000" in msg or "Login failed for user" in msg:
        return "AUTH FAILURE — wrong username/password (or user lacks DB access)."
    if "HYT00" in msg or "Login timeout expired" in msg:
        return ("LOGIN TIMEOUT — server not reachable/responding in time "
                "(down, wrong host/port, firewall, or server saturated).")
    if "IM002" in msg or "Data source name not found" in msg or "Can't open lib" in msg:
        return "DRIVER MISSING — the named ODBC driver is not installed (see drivers below)."
    if "08001" in msg or "TCP Provider" in msg or "Connection refused" in msg:
        return "NETWORK — cannot open a TCP socket to host:port (nothing listening / blocked)."
    if "18456" in msg:
        return "AUTH FAILURE — SQL Server rejected the login (state in message narrows why)."
    return "UNCLASSIFIED — see the raw error above."


def _test_one(name: str, conn_str: str) -> bool:
    print(f"\n── {name} " + "─" * (60 - len(name)))
    print(f"   {_describe(conn_str)}")
    engine = sa.create_engine(_with_timeout(conn_str))
    t0 = time.monotonic()
    try:
        with engine.connect() as conn:
            val = conn.execute(text("SELECT 1")).scalar()
        dt = time.monotonic() - t0
        print(f"   ✅ OK  (SELECT 1 -> {val}, {dt:.1f}s)")
        return True
    except Exception as e:  # noqa: BLE001
        dt = time.monotonic() - t0
        print(f"   ❌ FAILED after {dt:.1f}s")
        print(f"      {type(e).__name__}: {str(e).splitlines()[0]}")
        print(f"      → {_classify(e)}")
        return False
    finally:
        engine.dispose()


def main() -> int:
    print("=" * 70)
    print("MSSQL connectivity test — connection strings from db_config.py / .env")
    print(f"login timeout: {LOGIN_TIMEOUT}s  (override with TEST_LOGIN_TIMEOUT)")
    print("=" * 70)

    if pyodbc is not None:
        drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
        print(f"\nInstalled ODBC 'SQL Server' drivers: {drivers or '(none found!)'}")

    targets = [
        ("BASE_DB", BASE_DB),
        ("PROD_DB", PROD_DB),
        ("DIFF_STAGING_DB", DIFF_STAGING_DB),
    ]
    results = {name: _test_one(name, cs) for name, cs in targets}

    print("\n" + "=" * 70)
    print("SUMMARY")
    for name, ok in results.items():
        print(f"   {'✅ PASS' if ok else '❌ FAIL'}  {name}")
    print("=" * 70)
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
