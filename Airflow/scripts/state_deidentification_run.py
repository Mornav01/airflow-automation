#!/usr/bin/env python
"""
State-gated Deidentification Runner (deid v2)

Wraps `deid run` but FIRST filters the tables CSV down to only the tables that
still need de-identification this cycle — i.e. those whose per-table JSON state
`deidentification.deidentification_status` is NOT already "success". Tables that
already succeeded are skipped, so a re-run only re-processes what's left
(idempotent / resumable per-table).

Replaces manual_deidentification_run_v2.py for the pipeline's run_deid step. The
deid engine invocation (`python -m deid run --config --tables-csv [--rerun]`) and
the DEID_REPO_DIR import pinning are unchanged — only the tables list is filtered.

Usage:
    python state_deidentification_run.py --config /path/config.yaml --tables /path/tables.csv
"""

import argparse
import csv
import json
import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime

from dotenv import load_dotenv

# ── Load .env ─────────────────────────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ENV_FILE   = os.path.join(_SCRIPT_DIR, "..", ".env")
load_dotenv(dotenv_path=os.path.abspath(_ENV_FILE), override=True)

DEID_PYTHON   = os.environ["DEID_PYTHON"]
# Which checkout's `deid` code to run (prepended to PYTHONPATH below).
DEID_REPO_DIR = os.environ.get("DEID_REPO_DIR")
STATE_DIR     = os.environ.get("STATE_DIR", "")


# ── Logging ───────────────────────────────────────────────────────────────────
def setup_logging():
    log_dir = os.path.join(_SCRIPT_DIR, "..", "..", "deid_runtime", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f"state_deid_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Log file: %s", log_file)


# ── State-based table filtering ───────────────────────────────────────────────
def _read_tables(path: str) -> list:
    with open(path, newline="") as f:
        return [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]


def _needs_deid(table: str) -> bool:
    """True if the table still needs de-identification this cycle, i.e. its
    deidentification_status is NOT 'success'. A missing/unreadable state file is
    treated as needing deid (fail open — better to re-run than silently skip)."""
    if not STATE_DIR:
        return True
    state_path = os.path.join(STATE_DIR, f"{table}.json")
    if not os.path.exists(state_path):
        return True
    try:
        with open(state_path) as f:
            state = json.load(f)
    except (OSError, json.JSONDecodeError):
        return True
    return state.get("deidentification", {}).get("deidentification_status") != "success"


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="State-gated deid v2 runner")
    parser.add_argument("--config",  required=True, help="Path to config YAML")
    parser.add_argument("--tables",  required=True, help="Path to tables CSV")
    parser.add_argument("--no-rerun", action="store_true", help="Omit --rerun flag (resume from last state)")
    args = parser.parse_args()

    setup_logging()

    logging.info("Config : %s", args.config)
    logging.info("Tables : %s", args.tables)
    logging.info("Python : %s", DEID_PYTHON)
    logging.info("State  : %s", STATE_DIR or "(STATE_DIR unset — no filtering)")

    if not os.path.exists(args.config):
        logging.error("Config file not found: %s", args.config)
        sys.exit(1)
    if not os.path.exists(args.tables):
        logging.error("Tables file not found: %s", args.tables)
        sys.exit(1)

    # ── Filter tables by per-table JSON state ─────────────────────────────────
    all_tables, todo, skipped = _read_tables(args.tables), [], []
    for t in all_tables:
        (todo if _needs_deid(t) else skipped).append(t)

    logging.info(
        "Tables: total=%d, to-deid=%d, already-success (skipped)=%d",
        len(all_tables), len(todo), len(skipped),
    )
    if skipped:
        logging.info("Skipping already-success tables: %s", ", ".join(skipped))

    if not todo:
        logging.info("No tables need de-identification (all already success) — nothing to run.")
        return

    # Write the filtered list to a temp CSV that the deid engine consumes.
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix="_deid_tables.csv", delete=False, newline=""
    )
    with tmp:
        writer = csv.writer(tmp)
        for t in todo:
            writer.writerow([t])
    filtered_csv = tmp.name
    logging.info("Filtered tables CSV: %s (%d tables)", filtered_csv, len(todo))

    cmd = [
        DEID_PYTHON, "-m", "deid", "run",
        "--config",     args.config,
        "--tables-csv", filtered_csv,
    ]
    if not args.no_rerun:
        cmd.append("--rerun")

    # ── Pin the `deid` import to DEID_REPO_DIR so the right checkout runs ──────
    env = os.environ.copy()
    if DEID_REPO_DIR:
        repo = os.path.abspath(DEID_REPO_DIR)
        if not os.path.isdir(os.path.join(repo, "deid")):
            logging.error("DEID_REPO_DIR (%s) has no 'deid/' package — check the path "
                          "(it must point at the deidentification_v2 dir).", repo)
            sys.exit(1)
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = repo + (os.pathsep + existing if existing else "")
        logging.info("Pinning deid import path (DEID_REPO_DIR): %s", repo)
        # Log which deid module actually resolves, so a wrong checkout is obvious.
        probe = subprocess.run(
            [DEID_PYTHON, "-c", "import deid, sys; sys.stdout.write(deid.__file__)"],
            env=env, capture_output=True, text=True,
        )
        loaded = probe.stdout.strip()
        logging.info("deid resolved to: %s", loaded or probe.stderr.strip())
        if loaded and repo not in loaded:
            logging.warning("deid did NOT load from DEID_REPO_DIR — got %s. "
                            "A conflicting install is shadowing it.", loaded)
    else:
        logging.warning("DEID_REPO_DIR not set — 'deid' resolves to the installed package, "
                        "which may be a different checkout.")

    logging.info("Running: %s", " ".join(cmd))

    try:
        result = subprocess.run(cmd, env=env)
    finally:
        try:
            os.unlink(filtered_csv)   # don't leave temp filtered-CSVs behind
        except OSError:
            pass

    if result.returncode != 0:
        logging.error("deid run failed with exit code %d", result.returncode)
        sys.exit(result.returncode)

    logging.info("deid run completed successfully")


if __name__ == "__main__":
    main()
