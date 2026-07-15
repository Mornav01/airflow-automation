"""
restore_incremental_logs.py — shared MSSQL transaction-log restore.

Applies the ``.trn`` transaction-log backups dumped by the daily-dump DAG to one
or more MSSQL databases via ``RESTORE LOG ... WITH STANDBY`` (sqlcmd inside the
SQL Server container).  A single shared script serves every MSSQL EHR
(Greenway / eCW / MIND); the per-database list is supplied by each machine's
``.env`` so no code changes are needed to deploy.

The database list comes from ``RESTORE_DB_CONFIGS`` — a JSON array where each
entry describes one database::

    RESTORE_DB_CONFIGS='[
      {"db_name": "Primerecord_Latest",
       "source_db_name": "PrimeRecord1697",
       "standby_file": "/var/opt/mssql/undo/PrimeRecord_standby.undo",
       "trn_dir": "/var/opt/mssql/dump/SQLBackups/PrimeRecord1697/LOG/"}
    ]'

  - ``db_name``        : the TARGET database being restored into.
  - ``standby_file``   : the STANDBY undo file.
  - ``trn_dir``        : folder holding the .trn logs (searched recursively by
                         default — see RESTORE_TRN_RECURSIVE).
  - ``source_db_name`` : optional; the database name recorded INSIDE the backups
                         (as they were taken on the source server).  Only needed
                         when ``trn_dir`` holds logs for more than one database
                         (e.g. a shared parent) so the right ones are selected.
  - ``applied_logs_csv``: optional audit CSV, defaults to
                         ``DATA_LOGS_DIR/applied_logs_<db_name>.csv``.

When ``RESTORE_DB_CONFIGS`` is unset, a single-database config is built from the
legacy env vars (``RESTORE_LOGS_DB`` / ``STANDBY_FILE`` / ``TRN_DIR`` /
``APPLIED_LOGS_FILE``) so older single-DB machines keep working.

Ordering (``RESTORE_ORDER_MODE``)
---------------------------------
Transaction logs MUST be applied in an unbroken LSN chain.  Two modes:

  lsn  (default) — authoritative.  Reads each backup's header (``RESTORE
        HEADERONLY``), keeps the log backups for the target database, orders by
        ``FirstLSN``, and validates the chain (``next.FirstLSN ==
        prev.LastLSN``) so a gap is caught before anything is applied.  Files
        already applied are skipped using msdb restore history.
  filename — legacy fallback.  Sorts by the ``_YYYYMMDD_HHMMSS`` timestamp in the
        filename.  Cheaper (no header reads) but cannot detect gaps.

Either way, the apply loop is idempotent and self-correcting: SQL Server's own
"too early" (already applied → skip) / "too recent" (gap → stop) errors are the
final guard.

check_db_availability.py imports DB_CONFIGS and the connection settings from this
module so there is a single source of truth.
"""
import csv
import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Set, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Connection / behaviour settings (all overridable via env)
# ──────────────────────────────────────────────────────────────
SQLCMD_CONTAINER = os.getenv("SQLCMD_CONTAINER", "")
SQL_SERVER_CONTAINER = os.getenv("SQL_SERVER_CONTAINER", "localhost")
SA_USER = os.getenv("SA_USER", "sa")
SA_PASS = os.getenv("SA_PASS", "")
SQLCMD_BIN = os.getenv("SQLCMD_BIN", "/opt/mssql-tools18/bin/sqlcmd")
DRY_RUN = os.getenv("RESTORE_DRY_RUN", "0").lower() in ("1", "true", "yes")
DATA_LOGS_DIR = os.getenv("DATA_LOGS_DIR", "")

# Ordering: "lsn" (authoritative, reads backup headers) or "filename" (legacy).
ORDER_MODE = os.getenv("RESTORE_ORDER_MODE", "lsn").strip().lower()
# Search trn_dir recursively (default) or only its top level.
TRN_RECURSIVE = os.getenv("RESTORE_TRN_RECURSIVE", "1").lower() in ("1", "true", "yes")

BACKUP_TYPE_LOG = 2  # RESTORE HEADERONLY BackupType for a transaction-log backup

# Column separator for parsing sqlcmd tabular output.  RESTORE HEADERONLY is read
# by column *name* (not position), so it is version-independent — no need to pin a
# SQL Server version's header schema.  A row whose field count doesn't match the
# header (e.g. a '|' inside a text column) is treated as unreadable and skipped.
_SEP = "|"


class ChainGapError(RuntimeError):
    """Raised when the log chain has a gap (SQL Server: 'too recent')."""


# ──────────────────────────────────────────────────────────────
# DB list — from RESTORE_DB_CONFIGS (JSON) with a legacy single-DB fallback
# ──────────────────────────────────────────────────────────────
def _applied_logs_default(db_name: str) -> str:
    fname = f"applied_logs_{db_name}.csv"
    return os.path.join(DATA_LOGS_DIR, fname) if DATA_LOGS_DIR else fname


def _normalize_config(entry: dict) -> dict:
    missing = [k for k in ("db_name", "standby_file", "trn_dir") if not entry.get(k)]
    if missing:
        raise ValueError(f"RESTORE_DB_CONFIGS entry missing required key(s) {missing}: {entry!r}")
    return {
        "db_name": entry["db_name"],
        "standby_file": entry["standby_file"],
        "trn_dir": entry["trn_dir"],
        "source_db_name": entry.get("source_db_name", ""),
        "applied_logs_csv": entry.get("applied_logs_csv") or _applied_logs_default(entry["db_name"]),
    }


def load_db_configs() -> List[dict]:
    """Build the list of per-database restore configs from the environment."""
    raw = os.getenv("RESTORE_DB_CONFIGS", "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"RESTORE_DB_CONFIGS is not valid JSON: {e}")
        if not isinstance(parsed, list) or not parsed:
            raise ValueError("RESTORE_DB_CONFIGS must be a non-empty JSON array of objects.")
        return [_normalize_config(entry) for entry in parsed]

    # Legacy single-DB fallback (older machines set these instead).
    db_name = os.getenv("RESTORE_LOGS_DB", "")
    if db_name:
        return [_normalize_config({
            "db_name": db_name,
            "standby_file": os.getenv("STANDBY_FILE", ""),
            "trn_dir": os.getenv("TRN_DIR", ""),
            "applied_logs_csv": os.getenv("APPLIED_LOGS_FILE", ""),
        })]

    raise RuntimeError(
        "No restore databases configured — set RESTORE_DB_CONFIGS (JSON array) "
        "or the legacy RESTORE_LOGS_DB / STANDBY_FILE / TRN_DIR vars in .env."
    )


# Loaded at import time so check_db_availability can `from restore_incremental_logs
# import DB_CONFIGS`.  If the env is not yet configured this stays empty and the
# error is raised in main() — importing must not crash the availability check.
try:
    DB_CONFIGS: List[dict] = load_db_configs()
except Exception as _e:  # pragma: no cover - surfaced again at run time
    logger.warning("DB_CONFIGS not loaded at import: %s", _e)
    DB_CONFIGS = []


# ──────────────────────────────────────────────────────────────
# sqlcmd helpers
# ──────────────────────────────────────────────────────────────
def _sqlcmd_base() -> List[str]:
    return [
        "docker", "exec", SQLCMD_CONTAINER, SQLCMD_BIN,
        "-S", SQL_SERVER_CONTAINER, "-U", SA_USER, "-P", SA_PASS, "-C",
    ]


def run_sqlcmd(sql: str) -> None:
    """Run a statement, raising on any error (used for ALTER DATABASE etc.)."""
    proc = subprocess.run(
        _sqlcmd_base() + ["-Q", sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"sqlcmd failed (rc={proc.returncode}): {proc.stderr.strip()}")


def run_sqlcmd_capture(sql: str, extra_args: Optional[List[str]] = None) -> Tuple[int, str, str]:
    """Run a statement and return (returncode, stdout, stderr) without raising."""
    proc = subprocess.run(
        _sqlcmd_base() + (extra_args or []) + ["-Q", sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _query_rows(sql: str) -> List[List[str]]:
    """Run a SELECT and return rows as lists of stripped column strings."""
    rc, out, err = run_sqlcmd_capture(sql, extra_args=["-h", "-1", "-W", "-s", "|"])
    if rc != 0:
        raise RuntimeError(f"sqlcmd query failed (rc={rc}): {err.strip() or out.strip()}")
    return [
        [c.strip() for c in line.split("|")]
        for line in out.splitlines()
        if line.strip()
    ]


# ──────────────────────────────────────────────────────────────
# .trn discovery
# ──────────────────────────────────────────────────────────────
def get_trn_files_from_container(container: str, dir_path: str, recursive: bool = True) -> List[str]:
    """List .trn files under *dir_path* inside the container (recursive by default)."""
    cmd = ["docker", "exec", container, "find", dir_path]
    if not recursive:
        cmd += ["-maxdepth", "1"]
    cmd += ["-type", "f", "-name", "*.trn"]
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         universal_newlines=True, check=True)
    return [line.strip() for line in res.stdout.splitlines() if line.strip()]


# ──────────────────────────────────────────────────────────────
# Backup-header (LSN) reading
# ──────────────────────────────────────────────────────────────
def _parse_named_table(output: str) -> List[dict]:
    """Parse sqlcmd tabular output (header row kept) into a list of {col: value}.

    Reading by column name makes header parsing independent of the SQL Server
    version's RESTORE HEADERONLY schema.  Non-data lines (the '---' underline and
    the '(N rows affected)' footer) are dropped, and any row whose field count
    differs from the header (e.g. a separator char inside a text column) is
    skipped as unreadable.
    """
    lines = [ln for ln in output.splitlines() if ln.strip()]
    if not lines:
        return []
    header = [c.strip() for c in lines[0].split(_SEP)]
    rows: List[dict] = []
    for ln in lines[1:]:
        stripped = ln.strip()
        # Skip the dashes underline row and the rows-affected footer.
        if set(stripped) <= set("-| ") or stripped.startswith("(") or "rows affected" in stripped:
            continue
        fields = [c.strip() for c in ln.split(_SEP)]
        if len(fields) != len(header):
            continue
        rows.append(dict(zip(header, fields)))
    return rows


def read_backup_header(trn_file: str) -> Optional[dict]:
    """Return {database_name, backup_type, first_lsn, last_lsn, database_backup_lsn}
    for the (first) log backup set in *trn_file*, or None if it can't be read."""
    escaped = trn_file.replace("'", "''")
    sql = f"SET NOCOUNT ON; RESTORE HEADERONLY FROM DISK = N'{escaped}';"
    # Keep the header row (no -h -1); widen so sqlcmd never wraps the (very wide) row.
    rc, out, err = run_sqlcmd_capture(sql, extra_args=["-W", "-s", _SEP, "-w", "65535"])
    if rc != 0:
        logger.warning("Could not read header for %s: %s", trn_file, (err.strip() or out.strip()))
        return None

    for row in _parse_named_table(out):
        try:
            if int(row.get("BackupType", "")) != BACKUP_TYPE_LOG:
                continue
            return {
                "path": trn_file,
                "database_name": row.get("DatabaseName", ""),
                "backup_type": BACKUP_TYPE_LOG,
                "first_lsn": int(row["FirstLSN"]),
                "last_lsn": int(row["LastLSN"]),
                "database_backup_lsn": int(row["DatabaseBackupLSN"]),
            }
        except (ValueError, KeyError):
            continue
    logger.warning("No log-backup header row parsed for %s", trn_file)
    return None


def get_db_current_lsn(db_name: str) -> Optional[int]:
    """Highest LastLSN already restored into *db_name* (from msdb), or None."""
    escaped = db_name.replace("'", "''")
    sql = (
        "SET NOCOUNT ON; "
        "SELECT CAST(MAX(bs.last_lsn) AS VARCHAR(40)) "
        "FROM msdb.dbo.restorehistory rh "
        "JOIN msdb.dbo.backupset bs ON rh.backup_set_id = bs.backup_set_id "
        f"WHERE rh.destination_database_name = N'{escaped}';"
    )
    try:
        rows = _query_rows(sql)
    except RuntimeError as e:
        logger.warning("Could not read restore history for %s: %s", db_name, e)
        return None
    if not rows or not rows[0] or rows[0][0] in ("", "NULL"):
        return None
    try:
        return int(rows[0][0])
    except ValueError:
        return None


# ──────────────────────────────────────────────────────────────
# Ordering
# ──────────────────────────────────────────────────────────────
def _ts_key(path: str) -> str:
    m = re.search(r"_(\d{8}_\d{6})", path)
    return m.group(1) if m else path


def order_trn_files_by_filename(trn_files: List[str]) -> List[str]:
    """Legacy ordering — sort by the _YYYYMMDD_HHMMSS timestamp in the name."""
    return sorted(trn_files, key=_ts_key)


def order_trn_files_by_lsn(config: dict, trn_files: List[str]) -> List[str]:
    """Authoritative ordering: read headers, keep the target DB's log backups,
    sort by FirstLSN, validate the chain, and drop already-applied logs."""
    headers = [h for h in (read_backup_header(f) for f in trn_files) if h]
    if not headers:
        raise RuntimeError(
            f"{config['db_name']}: no readable log-backup headers found under {config['trn_dir']}"
        )

    # Select the right source database's logs (safe recursive / shared parent).
    source = config.get("source_db_name") or ""
    distinct = sorted({h["database_name"] for h in headers})
    if source:
        headers = [h for h in headers if h["database_name"] == source]
        if not headers:
            raise RuntimeError(
                f"{config['db_name']}: no backups for source_db_name={source!r} "
                f"under {config['trn_dir']} (found: {', '.join(distinct)})"
            )
    elif len(distinct) > 1:
        raise RuntimeError(
            f"{config['db_name']}: {config['trn_dir']} holds logs for multiple source "
            f"databases ({', '.join(distinct)}) — set 'source_db_name' to disambiguate."
        )

    headers.sort(key=lambda h: h["first_lsn"])

    # Offline chain validation — warn about any gap in the discovered set.
    for prev, nxt in zip(headers, headers[1:]):
        if nxt["first_lsn"] != prev["last_lsn"]:
            logger.warning(
                "%s: log chain gap between %s (LastLSN=%s) and %s (FirstLSN=%s)",
                config["db_name"], os.path.basename(prev["path"]), prev["last_lsn"],
                os.path.basename(nxt["path"]), nxt["first_lsn"],
            )

    # Drop already-applied logs using the DB's current restore point.
    current = get_db_current_lsn(config["db_name"])
    if current is not None:
        pending = [h for h in headers if h["first_lsn"] >= current]
        if pending and pending[0]["first_lsn"] != current:
            logger.warning(
                "%s: next log FirstLSN=%s does not chain onto DB current LSN=%s "
                "(a log may be missing)",
                config["db_name"], pending[0]["first_lsn"], current,
            )
        headers = pending
        logger.info("%s: DB current LSN=%s — %d log(s) pending", config["db_name"], current, len(headers))
    else:
        logger.info("%s: no restore history found — considering all %d discovered log(s)",
                    config["db_name"], len(headers))

    return [h["path"] for h in headers]


# ──────────────────────────────────────────────────────────────
# Applied-log tracking (secondary audit trail)
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
# Restore
# ──────────────────────────────────────────────────────────────
def restore_log(db_config: dict, trn_file: str, dry_run: bool = False) -> str:
    """Apply one log. Returns 'applied' | 'skipped' (already applied);
    raises ChainGapError on a gap ('too recent') or RuntimeError otherwise."""
    escaped = trn_file.replace("'", "''")
    sql = (
        f"RESTORE LOG [{db_config['db_name']}] "
        f"FROM DISK = N'{escaped}' "
        f"WITH STANDBY = N'{db_config['standby_file']}'"
    )
    if dry_run:
        logger.info("DRY-RUN: %s", sql)
        return "applied"

    rc, out, err = run_sqlcmd_capture(sql)
    if rc == 0:
        mark_log_as_applied(trn_file, db_config["applied_logs_csv"])
        return "applied"

    text = f"{out}\n{err}".lower()
    if "too early" in text:
        # This log precedes the DB's current point — already applied.
        return "skipped"
    if "too recent" in text:
        raise ChainGapError(
            f"{db_config['db_name']}: log chain gap at {os.path.basename(trn_file)} "
            f"(too recent — an earlier log is missing): {err.strip() or out.strip()}"
        )
    raise RuntimeError(f"RESTORE LOG failed for {trn_file} (rc={rc}): {err.strip() or out.strip()}")


def process_database(config: dict) -> None:
    logger.info("Starting restore for: %s (order_mode=%s, recursive=%s)",
                config["db_name"], ORDER_MODE, TRN_RECURSIVE)

    trn_files = get_trn_files_from_container(SQLCMD_CONTAINER, config["trn_dir"], TRN_RECURSIVE)
    if not trn_files:
        logger.info("No .trn files found under %s for %s — skipping.", config["trn_dir"], config["db_name"])
        return

    if ORDER_MODE == "filename":
        ordered = order_trn_files_by_filename(trn_files)
        # In filename mode, drop logs recorded in the applied-log CSV (no header info).
        applied = load_applied_logs(config["applied_logs_csv"])
        ordered = [f for f in ordered if f not in applied]
    else:
        ordered = order_trn_files_by_lsn(config, trn_files)

    if not ordered:
        logger.info("No new logs for %s, skipping.", config["db_name"])
        return

    logger.info("%s: applying %d log(s) in order.", config["db_name"], len(ordered))

    # Acquire single-user mode only when there are logs to apply, and always
    # release it — even if an individual restore fails mid-batch.
    run_sqlcmd(f"ALTER DATABASE [{config['db_name']}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;")
    applied_count = skipped_count = 0
    try:
        for trn in ordered:
            status = restore_log(config, trn, dry_run=DRY_RUN)
            if status == "applied":
                applied_count += 1
                logger.info("Applied: %s", trn)
            else:
                skipped_count += 1
                logger.info("Skipped (already applied): %s", trn)
    finally:
        run_sqlcmd(f"ALTER DATABASE [{config['db_name']}] SET MULTI_USER WITH ROLLBACK IMMEDIATE;")

    logger.info("Finished restore for %s — applied=%d, skipped=%d.",
                config["db_name"], applied_count, skipped_count)


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Reload here so a mis-set env fails loudly on run (not silently at import).
    DB_CONFIGS = load_db_configs()
    if ORDER_MODE not in ("lsn", "filename"):
        logger.warning("Unknown RESTORE_ORDER_MODE=%r — falling back to 'lsn'.", ORDER_MODE)
        ORDER_MODE = "lsn"

    failed = False
    for db_cfg in DB_CONFIGS:
        try:
            process_database(db_cfg)
        except Exception as e:
            logger.error("Failed processing %s: %s", db_cfg["db_name"], e)
            failed = True
    if failed:
        sys.exit(1)
