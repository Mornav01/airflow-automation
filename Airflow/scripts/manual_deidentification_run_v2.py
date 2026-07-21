#!/usr/bin/env python
"""
Manual Deidentification Runner (deid v2)

Runs `deid run` directly using the deid v2 CLI.
Blocks until all tables are complete. Exit code 0 = success.

Usage:
    python manual_deidentification_run_v2.py
    python manual_deidentification_run_v2.py --config /path/to/config.yaml --tables /path/to/tables.csv
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime

from dotenv import load_dotenv

# ── Load manual.env ───────────────────────────────────────────────────────────

_SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
_ENV_FILE     = os.path.join(_SCRIPT_DIR, "..", ".env")
load_dotenv(dotenv_path=os.path.abspath(_ENV_FILE), override=True)

# ── Defaults ─────────────────────────────────────────────────────────────────
# DEFAULT_CONFIG = os.environ["DEID_V2_CONFIG"]
# DEFAULT_TABLES = os.environ["DEID_V2_TABLES"]

# Uses new_deid conda env if set, falls back to current Python
DEID_PYTHON = os.environ["DEID_PYTHON"]

# Which checkout's `deid` code to run. Prepended to PYTHONPATH below so
# `python -m deid` imports from THIS repo, not whatever is pip-installed in the
# conda env — the fix for multiple checkouts (V2/V3/...) sharing one interpreter.
DEID_REPO_DIR = os.environ.get("DEID_REPO_DIR")

# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging():
    log_dir = os.path.join(_SCRIPT_DIR, "..", "..", "deid_runtime", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f"manual_deid_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
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

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Manual deid v2 runner")
    parser.add_argument("--config",  help="Path to config YAML")
    parser.add_argument("--tables",  help="Path to tables CSV")
    parser.add_argument("--no-rerun", action="store_true",   help="Omit --rerun flag (resume from last state)")
    args = parser.parse_args()

    setup_logging()

    logging.info("Config : %s", args.config)
    logging.info("Tables : %s", args.tables)
    logging.info("Python : %s", DEID_PYTHON)

    if not os.path.exists(args.config):
        logging.error("Config file not found: %s", args.config)
        sys.exit(1)
    if not os.path.exists(args.tables):
        logging.error("Tables file not found: %s", args.tables)
        sys.exit(1)

    cmd = [
        DEID_PYTHON, "-m", "deid", "run",
        "--config",     args.config,
        "--tables-csv", args.tables,
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

    result = subprocess.run(cmd, env=env)

    if result.returncode != 0:
        logging.error("deid run failed with exit code %d", result.returncode)
        sys.exit(result.returncode)

    logging.info("deid run completed successfully")


if __name__ == "__main__":
    main()
