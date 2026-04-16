#!/usr/bin/env python

import argparse
import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "manual.env")

# ============================================================
# ===================== DJANGO SETUP =========================
# ============================================================

STATE_DIRECTORY = os.environ["DEID_POLAR_STATE_DIR"]

BASE_REPO_DIR = os.environ["DEID_POLAR_REPO_DIR"]

DJANGO_PROJECT_DIR = os.path.join(BASE_REPO_DIR, "deIdentification")
sys.path.insert(0, DJANGO_PROJECT_DIR)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

import django
django.setup()

from nd_api.models import DbDetailsModel, TableDetailsModel
from nd_api.views.de_identification_task import create_deidentification_tasks_parallel
from worker.models import Task

# ============================================================
# ===================== PARALLEL CONFIG ======================
# ============================================================

#LARGE_TABLE_THRESHOLD = 4_000_000   # ID-range size above which → parallel
#PARALLEL_TASKS_COUNT  = 4           # parallel sub-tasks for large tables

RECORDS_PER_TASK = 1_000_000   # 1 task per 1M rows
# ============================================================
# ===================== LOGGING ==============================
# ============================================================

def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(
        log_dir,
        f"deid_runner_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )


# ============================================================
# =================== PRE-TASK HELPERS =======================
# ============================================================

def fix_reference_value(table_id: int) -> None:
    """Ensure reference patient/encounter columns are set for a table."""
    table = TableDetailsModel.objects.get(id=table_id)

    cols = table.table_details_for_ui.get("columns_details", []) or []

    pid_col = [
        col["column_name"]
        for col in cols
        if col.get("de_identification_rule") == "PATIENT_ID"
    ]
    enc_col = [
        col["column_name"]
        for col in cols
        if col.get("de_identification_rule") == "ENCOUNTER_ID"
    ]

    patient_id = pid_col[0] if pid_col else None
    enc_id = enc_col[0] if enc_col else None

    if not table.table_details_for_ui:
        table.table_details_for_ui = {}

    table.table_details_for_ui["reference_enc_id_column"] = enc_id
    table.table_details_for_ui["reference_patient_id_column"] = patient_id
    table.save(update_fields=["table_details_for_ui"])

    logging.debug(
        "Fixed reference values for table_id=%s: patient_id=%s, enc_id=%s",
        table_id, patient_id, enc_id,
    )


def update_table_configs(db) -> None:
    """Update reference_mapping config for specific tables before creating deidentification tasks."""
    table_configs = [
        {
            "table_name": "edi_inv_cpt",
            "reference_mapping": {
                "conditions": [{"column_name": "Id", "source_column": "InvoiceId", "reference_table": "edi_invoice"}],
                "source_table": "edi_inv_cpt",
                "destination_column": "PatientId",
                "destination_column_type": "patient_id",
            },
        },
        {
            "table_name": "edi_inv_diagnosis",
            "reference_mapping": {
                "conditions": [{"column_name": "Id", "source_column": "InvoiceId", "reference_table": "edi_invoice"}],
                "source_table": "edi_inv_diagnosis",
                "destination_column": "PatientId",
                "destination_column_type": "patient_id",
            },
        },
        {
            "table_name": "hl7labdatadetail",
            "reference_mapping": {
                "conditions": [{"column_name": "ReportId", "source_column": "ReportId", "reference_table": "labdata"}],
                "source_table": "hl7labdatadetail",
                "destination_column": "EncounterId",
                "destination_column_type": "encounter_id",
            },
        },
        {
            "table_name": "hl7labnotes",
            "reference_mapping": {
                "conditions": [{"column_name": "ReportId", "source_column": "reportid", "reference_table": "labdata"}],
                "source_table": "hl7labnotes",
                "destination_column": "EncounterId",
                "destination_column_type": "encounter_id",
            },
        },
        {
            "table_name": "labdataex",
            "reference_mapping": {
                "conditions": [{"column_name": "ReportId", "source_column": "reportId", "reference_table": "labdata"}],
                "source_table": "labdataex",
                "destination_column": "EncounterId",
                "destination_column_type": "encounter_id",
            },
        },
    ]

    for config in table_configs:
        table_name = config["table_name"]
        try:
            table_obj = TableDetailsModel.objects.get(db=db, table_name=table_name)
            if not table_obj.table_details_for_ui:
                table_obj.table_details_for_ui = {}
            table_obj.table_details_for_ui["reference_mapping"] = config["reference_mapping"]
            table_obj.save()
            logging.info("Updated reference_mapping config for table: %s", table_name)
        except TableDetailsModel.DoesNotExist:
            logging.debug("Table '%s' not found, skipping config update", table_name)
        except Exception as e:
            logging.warning("Failed to update config for table '%s': %s", table_name, e)


# ============================================================
# ===================== CORE LOGIC ===========================
# ============================================================

def process_table(table_name, db, csv_writer):

    try:
        table_obj = TableDetailsModel.objects.get(db=db, table_name=table_name)
    except TableDetailsModel.DoesNotExist:
        logging.warning("Table %s not found in DB, skipping", table_name)
        csv_writer.writerow({
            "table_name":    table_name,
            "status":        "skipped",
            "tasks_created": "",
            "chain_id":      "",
            "timestamp":     datetime.now().isoformat(),
            "error":         "Table not found in DB",
        })
        return

    logging.info("Processing table: %s", table_name)

    try:
        # Delete old tasks
        Task.objects.filter(arguments__table_id=table_obj.id).delete()

        # Fix reference patient/encounter columns
        try:
            fix_reference_value(table_obj.id)
            table_obj.refresh_from_db()
        except Exception as e:
            logging.warning("Could not fix reference values for %s: %s", table_name, e)

        # create_deidentification_tasks_parallel handles the single-vs-parallel
        # decision internally using a MIN/MAX query.  No rows_count needed.
        tasks, chain = create_deidentification_tasks_parallel(
            table_obj=table_obj,
            delete_table=True,
            records_per_task=RECORDS_PER_TASK
        )

        logging.info("Created %d tasks for %s", len(tasks), table_name)

        csv_writer.writerow({
            "table_name":    table_name,
            "status":        "started",
            "tasks_created": len(tasks),
            "chain_id":      getattr(chain, "id", ""),
            "timestamp":     datetime.now().isoformat(),
            "error":         "",
        })

    except Exception as e:
        logging.exception("Failed processing table: %s", table_name)

        csv_writer.writerow({
            "table_name":    table_name,
            "status":        "failed",
            "tasks_created": "",
            "chain_id":      "",
            "timestamp":     datetime.now().isoformat(),
            "error":         str(e),
        })


# ============================================================
# =========================== MAIN ===========================
# ============================================================

CSV_FIELDS = ["table_name", "status", "tasks_created", "chain_id", "timestamp", "error"]


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dbdetailsmodel_name",
        default=os.environ["DEID_POLAR_DBMODEL"],
    )
    parser.add_argument(
        "--tables_filepath",
        default=os.environ["DEID_POLAR_TABLES_FILE"],
    )

    args = parser.parse_args()

    run_dir = os.path.join(STATE_DIRECTORY, "deid")
    os.makedirs(run_dir, exist_ok=True)

    setup_logging(run_dir)

    # Validate DB
    try:
        db = DbDetailsModel.objects.get(db_name=args.dbdetailsmodel_name)
    except DbDetailsModel.DoesNotExist:
        logging.error("Database %s not found", args.dbdetailsmodel_name)
        sys.exit(1)

    # Read tables from CSV
    with open(args.tables_filepath, newline="") as f:
        tables = [row[0] for row in csv.reader(f)]

    logging.info("Found %d tables in CSV", len(tables))

    logging.info("Updating table configurations...")
    update_table_configs(db)

    csv_path = os.path.join(run_dir, f"deid_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    logging.info("Writing run state to: %s", csv_path)

    with open(csv_path, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
        writer.writeheader()

        for table in tables:
            process_table(table.strip(), db, writer)
            csv_file.flush()

    logging.info("Deidentification tasks creation started")


if __name__ == "__main__":
    main()
