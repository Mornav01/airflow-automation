#!/usr/bin/env python3
"""
Daily incremental update of clinicalbin_decrypted_26.

Fetches records from ClinicalBin (MSSQL) where cd.Created >= daily_cutoff,
skips duplicates by composite key, and inserts new rows into MySQL.

Usage:
    python decrypt_cnotes.py                           # uses yesterday as cutoff
    python decrypt_cnotes.py --daily-cutoff 2025-12-31
"""

import argparse
import json
import logging
import os
import re
import sys
import zlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from lxml import etree
from sqlalchemy import create_engine, text

# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parents[2] / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

SOURCE_CONN   = os.environ.get("CNOTES_SOURCE_CONN", "")
DEST_CONN     = os.environ.get("CNOTES_DEST_CONN",   "")
LINKED_DB     = os.environ.get("CNOTES_LINKED_DB",   "Primerecord_Latest")
BATCH_SIZE    = int(os.environ.get("CNOTES_BATCH_SIZE", "10000"))

SOURCE_TABLE  = "clinicalbin_decrypted_26"
STAGING_TABLE = "clinicalbin_decrypted_staging"
STATE_DIR     = os.environ.get("STATE_DIR", "")
STATE_FILE    = os.path.join(STATE_DIR, "clinicalbin_delta_state.json") if STATE_DIR else ""

# Airflow context injected as env vars by the BashOperator.
AIRFLOW_DAG_ID        = os.environ.get("AIRFLOW_DAG_ID", "clinicalbin_decrypt_daily")
AIRFLOW_RUN_ID        = os.environ.get("AIRFLOW_RUN_ID", "")
AIRFLOW_EXECUTION_DATE = os.environ.get("AIRFLOW_EXECUTION_DATE", "")

# BinTypeIDs to process; only XML types are decompressed/parsed.
XML_BINTYPES = {1000, 1004, 1005, 1016}
BINTYPEID_TO_EXT = {1004: "xml", 1000: "xml", 1001: "pdf", 1016: "xml", 1003: "txt", 1005: "xml", 1007: "tif"}


# ──────────────────────────────────────────────────────────────
# XML helpers
# ──────────────────────────────────────────────────────────────
def clean_xml(xml_bytes: bytes) -> str | None:
    xml_string = xml_bytes.decode("utf-8", errors="ignore")
    xml_string = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", xml_string)
    try:
        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(xml_string.encode("utf-8"), parser)
        return etree.tostring(root, encoding="utf-8").decode("utf-8")
    except Exception as e:
        logger.debug("XML parse failed, skipping record: %s", e)
        return None


def _safe_int(val):
    return int(val) if val is not None else None


# ──────────────────────────────────────────────────────────────
# Run-log metadata helpers
# ──────────────────────────────────────────────────────────────
_CREATE_RUN_LOG = """
CREATE TABLE IF NOT EXISTS clinicalbin_decrypt_run_log (
    id               BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    dag_id           VARCHAR(250)  NOT NULL,
    run_id           VARCHAR(250)  NOT NULL,
    execution_date   VARCHAR(50)   NOT NULL,
    cutoff_used      VARCHAR(50)   NOT NULL,
    rows_inserted    INT UNSIGNED  NOT NULL DEFAULT 0,
    rows_skipped_dup INT UNSIGNED  NOT NULL DEFAULT 0,
    rows_skipped_xml INT UNSIGNED  NOT NULL DEFAULT 0,
    run_start_utc    DATETIME      NOT NULL,
    run_end_utc      DATETIME      NOT NULL,
    duration_seconds DECIMAL(10,3) NOT NULL,
    status           VARCHAR(20)   NOT NULL,
    error_message    TEXT,
    created_at       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _ensure_run_log_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_CREATE_RUN_LOG))


def _write_run_log(engine, *, cutoff_used: str, rows_inserted: int,
                   rows_skipped_dup: int, rows_skipped_xml: int,
                   run_start: datetime, run_end: datetime,
                   status: str, error_message: str | None = None) -> None:
    duration = (run_end - run_start).total_seconds()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO clinicalbin_decrypt_run_log
                    (dag_id, run_id, execution_date, cutoff_used,
                     rows_inserted, rows_skipped_dup, rows_skipped_xml,
                     run_start_utc, run_end_utc, duration_seconds,
                     status, error_message)
                VALUES
                    (:dag_id, :run_id, :execution_date, :cutoff_used,
                     :rows_inserted, :rows_skipped_dup, :rows_skipped_xml,
                     :run_start_utc, :run_end_utc, :duration_seconds,
                     :status, :error_message)
            """),
            {
                "dag_id":           AIRFLOW_DAG_ID,
                "run_id":           AIRFLOW_RUN_ID,
                "execution_date":   AIRFLOW_EXECUTION_DATE,
                "cutoff_used":      cutoff_used,
                "rows_inserted":    rows_inserted,
                "rows_skipped_dup": rows_skipped_dup,
                "rows_skipped_xml": rows_skipped_xml,
                "run_start_utc":    run_start.strftime("%Y-%m-%d %H:%M:%S"),
                "run_end_utc":      run_end.strftime("%Y-%m-%d %H:%M:%S"),
                "duration_seconds": round(duration, 3),
                "status":           status,
                "error_message":    error_message,
            },
        )
    logger.info(
        "Run log written: status=%s inserted=%d skipped_dup=%d skipped_xml=%d duration=%.1fs",
        status, rows_inserted, rows_skipped_dup, rows_skipped_xml, duration,
    )


# ──────────────────────────────────────────────────────────────
# Delta staging helpers
# ──────────────────────────────────────────────────────────────
def _load_staging_state() -> int:
    """Return last staged nd_auto_increment_id (0 on first run)."""
    if not STATE_FILE or not os.path.exists(STATE_FILE):
        return 0
    with open(STATE_FILE) as f:
        return json.load(f).get("last_nd_auto_increment_id", 0)


def _run_delta_staging(engine, last_staged_id: int) -> int:
    """Copy rows newer than last_staged_id into the staging table; update state file."""
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS `{STAGING_TABLE}` LIKE `{SOURCE_TABLE}`"))
        conn.execute(text(f"TRUNCATE TABLE `{STAGING_TABLE}`"))

        delta_count = conn.execute(
            text(f"SELECT COUNT(*) FROM `{SOURCE_TABLE}` WHERE nd_auto_increment_id > :last_id"),
            {"last_id": last_staged_id},
        ).scalar()

        logger.info("Clinicalbin delta staging: %d rows to stage (last_id=%d)", delta_count, last_staged_id)

        if delta_count == 0:
            logger.info("No new rows — staging table left empty.")
            _save_staging_state(last_staged_id, 0)
            return 0

        conn.execute(
            text(f"INSERT INTO `{STAGING_TABLE}` SELECT * FROM `{SOURCE_TABLE}` WHERE nd_auto_increment_id > :last_id"),
            {"last_id": last_staged_id},
        )
        new_max_id = conn.execute(
            text(f"SELECT MAX(nd_auto_increment_id) FROM `{STAGING_TABLE}`")
        ).scalar()

    logger.info("Staged %d rows. New max nd_auto_increment_id: %d", delta_count, new_max_id)
    _save_staging_state(new_max_id, delta_count)
    return delta_count


def _save_staging_state(last_id: int, rows_staged: int) -> None:
    if not STATE_FILE:
        logger.warning("STATE_DIR not set — skipping staging state file write.")
        return
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(
            {
                "last_nd_auto_increment_id": last_id,
                "last_processed_date": date.today().isoformat(),
                "rows_staged": rows_staged,
                "success": True,
            },
            f,
            indent=4,
        )


# ──────────────────────────────────────────────────────────────
# Core runner
# ──────────────────────────────────────────────────────────────
def run(daily_cutoff: str, batch_size: int = BATCH_SIZE) -> int:
    source_engine = create_engine(SOURCE_CONN, pool_pre_ping=True, pool_recycle=3600)
    dest_engine   = create_engine(DEST_CONN,   pool_pre_ping=True, pool_recycle=3600)

    logger.info("source: %s", SOURCE_CONN.split("@")[-1])
    logger.info("dest  : %s", DEST_CONN.split("@")[-1])
    logger.info("cutoff: %s", daily_cutoff)

    # Read staging state before any inserts so we know the exact boundary for staging.
    last_staged_id = _load_staging_state()

    _ensure_run_log_table(dest_engine)
    run_start = datetime.now(timezone.utc)

    total_inserted = 0
    total_skipped_dup = 0
    total_skipped_xml = 0

    try:
        # 1. Load existing composite keys to avoid duplicates.
        logger.info("Loading existing keys from clinicalbin_decrypted_26...")
        with dest_engine.connect() as conn:
            existing_rows = conn.execute(
                text("SELECT patient_id, visit_id, DocumentID, SequenceNumber, BinTypeID FROM clinicalbin_decrypted_26")
            ).fetchall()

        existing_keys = {
            (_safe_int(r.patient_id), _safe_int(r.visit_id), _safe_int(r.DocumentID),
             _safe_int(r.SequenceNumber), _safe_int(r.BinTypeID))
            for r in existing_rows
        }
        logger.info("Loaded %d existing keys.", len(existing_keys))

        # 2. Current max nd_auto_increment_id.
        with dest_engine.connect() as conn:
            max_id = conn.execute(
                text("SELECT COALESCE(MAX(nd_auto_increment_id), 0) FROM clinicalbin_decrypted_26")
            ).scalar()
        next_nd_id = (max_id or 0) + 1
        logger.info("Next nd_auto_increment_id: %d", next_nd_id)

        # 3. Stream source in batches and insert new records.
        offset = 0

        with source_engine.connect() as src_conn:
            while True:
                # OFFSET/FETCH integers are not user input; safe to interpolate.
                query = text(f"""
                    SELECT c.*, cd.PatientID, cd.VisitID, cd.DocTypeID, cd.DocName,
                           cd.DocDescription, cd.Created, cd.LastModified
                    FROM ClinicalBin AS c
                    INNER JOIN [{LINKED_DB}].[dbo].[ClinicalDocuments] AS cd
                        ON c.DocumentID = cd.DocumentID
                    WHERE c.BinTypeID IN (1000, 1004, 1005, 1016)
                      AND cd.Created >= :cutoff
                    ORDER BY c.DocumentID
                    OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY
                """)
                results = src_conn.execute(query, {"cutoff": daily_cutoff}).fetchall()

                if not results:
                    break

                logger.info("Batch offset=%d: fetched %d rows", offset, len(results))

                insert_data = []
                for row in results:
                    key = (
                        _safe_int(row.PatientID), _safe_int(row.VisitID),
                        _safe_int(row.DocumentID), _safe_int(row.SequenceNumber),
                        _safe_int(row.BinTypeID),
                    )
                    if key in existing_keys:
                        total_skipped_dup += 1
                        continue

                    file_ext = BINTYPEID_TO_EXT.get(row.BinTypeID, "bin")
                    binary_data = row.DocImage

                    if file_ext == "xml":
                        if binary_data and (binary_data[:2] in (b"x\x9c", b"\x78\x9c", b"\x78\x01", b"\x78\xda")):
                            try:
                                binary_data = zlib.decompress(binary_data)
                            except zlib.error:
                                logger.debug("zlib decompress failed for DocumentID=%s", row.DocumentID)
                                total_skipped_xml += 1
                                continue
                        xml_string = clean_xml(binary_data) if binary_data else None
                        if xml_string is None:
                            total_skipped_xml += 1
                            continue
                        doc_content = xml_string
                    else:
                        doc_content = binary_data.decode("utf-8", errors="ignore") if binary_data else None

                    insert_data.append({
                        "nd_auto_increment_id": next_nd_id,
                        "patient_id":           row.PatientID,
                        "DocumentID":           row.DocumentID,
                        "SequenceNumber":       row.SequenceNumber,
                        "BinTypeID":            row.BinTypeID,
                        "DocContent":           doc_content,
                        "visit_id":             row.VisitID,
                        "doctype_id":           row.DocTypeID,
                        "doc_name":             row.DocName,
                        "doc_desc":             row.DocDescription,
                        "created_at":           row.Created,
                        "updated_at":           row.LastModified,
                    })
                    existing_keys.add(key)
                    next_nd_id += 1

                if insert_data:
                    with dest_engine.begin() as dest_conn:
                        dest_conn.execute(
                            text("""
                                INSERT INTO clinicalbin_decrypted_26
                                (nd_auto_increment_id, patient_id, DocumentID, SequenceNumber,
                                 BinTypeID, DocContent, visit_id, doctype_id, doc_name, doc_desc,
                                 created_at, updated_at)
                                VALUES
                                (:nd_auto_increment_id, :patient_id, :DocumentID, :SequenceNumber,
                                 :BinTypeID, :DocContent, :visit_id, :doctype_id, :doc_name, :doc_desc,
                                 :created_at, :updated_at)
                            """),
                            insert_data,
                        )
                    total_inserted += len(insert_data)
                    logger.info("  Inserted %d new records (running total: %d)", len(insert_data), total_inserted)

                offset += batch_size

        logger.info("Done. inserted=%d  skipped_dup=%d  skipped_xml=%d",
                    total_inserted, total_skipped_dup, total_skipped_xml)

        _write_run_log(
            dest_engine,
            cutoff_used=daily_cutoff,
            rows_inserted=total_inserted,
            rows_skipped_dup=total_skipped_dup,
            rows_skipped_xml=total_skipped_xml,
            run_start=run_start,
            run_end=datetime.now(timezone.utc),
            status="SUCCESS",
        )

    except Exception as exc:
        _write_run_log(
            dest_engine,
            cutoff_used=daily_cutoff,
            rows_inserted=total_inserted,
            rows_skipped_dup=total_skipped_dup,
            rows_skipped_xml=total_skipped_xml,
            run_start=run_start,
            run_end=datetime.now(timezone.utc),
            status="FAILED",
            error_message=str(exc),
        )
        raise

    # Delta staging: copy the rows just inserted into the staging table for the
    # downstream clinicalbin deid run. Using last_staged_id (read before any
    # inserts) as the boundary means a retry is safe — if decrypt succeeded but
    # staging failed, the state file is not updated, so the next run re-stages.
    _run_delta_staging(dest_engine, last_staged_id)

    return total_inserted


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Daily incremental update of clinicalbin_decrypted_26")
    parser.add_argument(
        "--daily-cutoff",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Cutoff date for cd.Created (default: yesterday)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Rows per batch (default: CNOTES_BATCH_SIZE from .env, or 10000)",
    )
    args = parser.parse_args()

    if args.daily_cutoff:
        cutoff = args.daily_cutoff.strip()
        if " " not in cutoff:
            cutoff = f"{cutoff} 00:00:00.000"
    else:
        cutoff = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00.000")

    run(daily_cutoff=cutoff, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
