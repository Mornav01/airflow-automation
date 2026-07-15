"""
check_db_availability.py — Pre-restore guard.

The pipeline applies transaction-log backups with `RESTORE LOG ... WITH STANDBY`,
which briefly flips each target database into SINGLE_USER / RESTORING mode
(rolling back any other connections with ROLLBACK IMMEDIATE).  Running that while
a deid / diff-merge job is reading the database would kill that job, and starting
a second restore while one is already in flight leaves the DB stuck in RESTORING.

This script is the guard that runs *before* the restore step.  For every target
database it checks that the DB is ONLINE (not mid-restore) and that no other user
sessions are connected.  If anything looks unsafe it exits non-zero so the Airflow
task fails loudly instead of disrupting work in progress.

DB list and connection settings are imported from restore_incremental_logs so
there is a single source of truth.
"""
import logging
import subprocess
import sys

from restore_incremental_logs import (
    DB_CONFIGS,
    SQLCMD_BIN,
    SQLCMD_CONTAINER,
    SA_PASS,
    SA_USER,
    SQL_SERVER_CONTAINER,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# SQL helper — returns query rows as a list of column lists
# ──────────────────────────────────────────────────────────────
def query_rows(sql: str) -> list[list[str]]:
    cmd = [
        "docker", "exec", SQLCMD_CONTAINER, SQLCMD_BIN,
        "-S", SQL_SERVER_CONTAINER, "-U", SA_USER, "-P", SA_PASS,
        "-Q", sql, "-C",
        "-h", "-1",   # no header row
        "-W",         # strip trailing whitespace
        "-s", "|",    # column separator
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if proc.returncode != 0:
        raise RuntimeError(f"sqlcmd failed (rc={proc.returncode}): {proc.stderr.strip()}")
    return [
        [c.strip() for c in line.split("|")]
        for line in proc.stdout.splitlines()
        if line.strip()
    ]


# ──────────────────────────────────────────────────────────────
# Availability check
# ──────────────────────────────────────────────────────────────
def check_database(db_name: str) -> list[str]:
    """Return a list of human-readable problems for db_name (empty == available)."""
    sql = (
        "SET NOCOUNT ON; "
        "SELECT d.state_desc, "
        "(SELECT COUNT(*) FROM sys.dm_exec_sessions s "
        " WHERE s.database_id = DB_ID(d.name) "
        "   AND s.session_id <> @@SPID "
        "   AND s.is_user_process = 1) AS active_sessions "
        f"FROM sys.databases d WHERE d.name = N'{db_name}';"
    )
    rows = query_rows(sql)
    if not rows:
        return [f"{db_name}: database not found on server"]

    state, active_sessions = rows[0][0], rows[0][1]
    problems: list[str] = []

    if state.upper() != "ONLINE":
        problems.append(f"{db_name}: state is {state} (expected ONLINE — a restore may be in progress)")

    try:
        sessions = int(active_sessions)
    except ValueError:
        sessions = 0
    if sessions > 0:
        problems.append(f"{db_name}: {sessions} active user session(s) — database is in use")

    if not problems:
        logger.info("%s: ONLINE, no active sessions — available for restore", db_name)
    return problems


def main() -> None:
    db_names = [cfg["db_name"] for cfg in DB_CONFIGS]
    logger.info("Checking availability of %d database(s): %s", len(db_names), ", ".join(db_names))

    all_problems: list[str] = []
    for db_name in db_names:
        all_problems.extend(check_database(db_name))

    if all_problems:
        for p in all_problems:
            logger.error(p)
        raise RuntimeError(
            "Databases are NOT available for restore:\n  - " + "\n  - ".join(all_problems)
        )

    logger.info("All databases are available for restore.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(str(e))
        sys.exit(1)
