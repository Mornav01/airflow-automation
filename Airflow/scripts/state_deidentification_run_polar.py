#!/usr/bin/env python

import argparse
import logging
import os
import sys
import json
import csv
from datetime import datetime

# ============================================================
# ===================== DJANGO SETUP =========================
# ============================================================

STATE_DIRECTORY = "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/state"

BASE_REPO_DIR = "/Users/ndaicnsarizona/Desktop/deidenmtification_polars/deidentification_v2/"

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

LARGE_TABLE_THRESHOLD = 4_000_000   # ID-range size above which → parallel
PARALLEL_TASKS_COUNT  = 4           # parallel sub-tasks for large tables


# ============================================================
# ===================== STATE MANAGER ========================
# ============================================================

class StateManager:

    def __init__(self, state_dir):
        self.state_dir = state_dir
        os.makedirs(self.state_dir, exist_ok=True)

    def _file_path(self, table):
        return os.path.join(self.state_dir, f"{table}.json")

    def load(self, table):
        path = self._file_path(table)

        if not os.path.exists(path):
            return {
                "diff": {},
                "merge": {},
                "deidentification": {}
            }

        with open(path, "r") as f:
            state = json.load(f)

        state.setdefault("diff", {})
        state.setdefault("merge", {})
        state.setdefault("deidentification", {})

        return state

    def save(self, table, state):
        path = self._file_path(table)
        with open(path, "w") as f:
            json.dump(state, f, indent=4)


# ============================================================
# ===================== LOGGING ==============================
# ============================================================

def setup_logging(state_dir):
    os.makedirs(state_dir, exist_ok=True)

    log_dir = os.path.join(state_dir, "deidentification_runner")
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

def process_table(table_name, db, state_manager):

    state = state_manager.load(table_name)

    try:
        table_obj = TableDetailsModel.objects.get(db=db, table_name=table_name)
    except TableDetailsModel.DoesNotExist:
        logging.warning("Table %s not found in DB, skipping", table_name)
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
            num_parallel=PARALLEL_TASKS_COUNT,
            large_table_threshold=LARGE_TABLE_THRESHOLD,
        )

        logging.info(
            "Created %d tasks for %s",
            len(tasks),
            table_name
        )

        state["deidentification"] = {
            "last_processed_date": datetime.now().isoformat(),
            "status": "started",   # options: started, completed, failed
            "tasks_created": len(tasks),
            "chain_id": getattr(chain, "id", None)
        }

        state_manager.save(table_name, state)

    except Exception as e:
        logging.exception("Failed processing table: %s", table_name)

        state["deidentification"] = {
            "last_processed_date": datetime.now().isoformat(),
            "status": "failed",    # options: started, completed, failed
            "error": str(e)
        }

        state_manager.save(table_name, state)


# ============================================================
# =========================== MAIN ===========================
# ============================================================

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--dbdetailsmodel_name", required=True)
    parser.add_argument("--tables_filepath", required=True)

    args = parser.parse_args()

    setup_logging(STATE_DIRECTORY)

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

    state_manager = StateManager(STATE_DIRECTORY)

    logging.info("Updating table configurations...")
    update_table_configs(db)

    for table in tables:
        process_table(table.strip(), db, state_manager)

    logging.info("Deidentification tasks creation started")


if __name__ == "__main__":
    main()