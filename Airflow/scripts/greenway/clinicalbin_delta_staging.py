#!/usr/bin/env python3
"""
clinicalbin_delta_staging.py

Extracts delta rows from clinicalbin_decrypted_26 (MySQL) into a staging
table clinicalbin_decrypted_staging.

Delta = rows with nd_auto_increment_id greater than the last processed ID
recorded in the state file. On the first run, all existing rows are staged.

The staging table is then consumed by a separate deid run targeting only
the new/changed clinical notes, avoiding re-processing the full history
every day.
"""

import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(Path(__file__).parents[2] / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

DEST_CONN     = os.environ.get("CNOTES_DEST_CONN", "")
STATE_DIR     = os.environ["STATE_DIR"]
STATE_FILE    = os.path.join(STATE_DIR, "clinicalbin_delta_state.json")

SOURCE_TABLE  = "clinicalbin_decrypted_26"
STAGING_TABLE = "clinicalbin_decrypted_staging"


# ──────────────────────────────────────────────────────────────
# State helpers
# ──────────────────────────────────────────────────────────────
def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {"last_nd_auto_increment_id": 0}
    with open(STATE_FILE) as f:
        return json.load(f)


def save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)


# ──────────────────────────────────────────────────────────────
# Core logic
# ──────────────────────────────────────────────────────────────
def run() -> int:
    state   = load_state()
    last_id = state.get("last_nd_auto_increment_id", 0)
    logger.info("Last processed nd_auto_increment_id: %d", last_id)

    engine = create_engine(DEST_CONN, pool_pre_ping=True)

    with engine.begin() as conn:
        # Ensure staging table mirrors the source schema
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS `{STAGING_TABLE}` LIKE `{SOURCE_TABLE}`"))

        # Truncate staging — only the current delta batch should be present
        conn.execute(text(f"TRUNCATE TABLE `{STAGING_TABLE}`"))

        # Count delta rows before inserting so we can log + short-circuit
        delta_count = conn.execute(
            text(f"SELECT COUNT(*) FROM `{SOURCE_TABLE}` WHERE nd_auto_increment_id > :last_id"),
            {"last_id": last_id},
        ).scalar()

        logger.info("Delta rows to stage: %d", delta_count)

        if delta_count == 0:
            logger.info("No new rows since last run — staging table left empty.")
            save_state({
                "last_nd_auto_increment_id": last_id,
                "last_processed_date": date.today().isoformat(),
                "rows_staged": 0,
                "success": True,
            })
            return 0

        # Insert delta rows into staging in a single INSERT … SELECT
        conn.execute(
            text(f"""
                INSERT INTO `{STAGING_TABLE}`
                SELECT * FROM `{SOURCE_TABLE}`
                WHERE nd_auto_increment_id > :last_id
            """),
            {"last_id": last_id},
        )

        new_max_id = conn.execute(
            text(f"SELECT MAX(nd_auto_increment_id) FROM `{STAGING_TABLE}`")
        ).scalar()

    logger.info("Staged %d rows. New max nd_auto_increment_id: %d", delta_count, new_max_id)

    save_state({
        "last_nd_auto_increment_id": new_max_id,
        "last_processed_date": date.today().isoformat(),
        "rows_staged": delta_count,
        "success": True,
    })

    return delta_count


if __name__ == "__main__":
    run()
