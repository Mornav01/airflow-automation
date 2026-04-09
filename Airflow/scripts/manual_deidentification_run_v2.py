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

# ── Load .env (always from Airflow/.env regardless of CWD) ───────────────────

_SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
_ENV_FILE     = os.path.join(_SCRIPT_DIR, "..", ".env")
load_dotenv(dotenv_path=os.path.abspath(_ENV_FILE), override=False)

# ── Defaults ─────────────────────────────────────────────────────────────────
_CONFIGS_DIR  = os.path.join(_SCRIPT_DIR, "configs")

DEFAULT_CONFIG = os.path.join(_CONFIGS_DIR, "config_historical.yaml")
DEFAULT_TABLES = "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/input/test.csv"

# Uses new_deid conda env if set, falls back to current Python
DEID_PYTHON = os.environ.get("DEID_PYTHON", sys.executable)

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
    parser.add_argument("--config",  default=DEFAULT_CONFIG, help="Path to config YAML")
    parser.add_argument("--tables",  default=DEFAULT_TABLES, help="Path to tables CSV")
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

    logging.info("Running: %s", " ".join(cmd))

    result = subprocess.run(cmd)

    if result.returncode != 0:
        logging.error("deid run failed with exit code %d", result.returncode)
        sys.exit(result.returncode)

    logging.info("deid run completed successfully")


if __name__ == "__main__":
    main()
