#!/usr/bin/env python
"""
Deidentification Run Script

This script runs the deidentification process by creating tasks for tables
that need deidentification in the given logical database (DbDetailsModel).

Usage:
    python deid_run.py --db_name "deid_prod" --table_status 0

- db_name: name in DbDetailsModel (e.g. "deid_prod")
- table_status: 0=pending, 1=in progress, 2=completed, 3=failed
"""

import argparse
import logging
import os
import sys

# Configure logging early (before Django setup)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Setup Django environment
# Base path for the deidentification repo on the Airflow host
BASE_REPO_DIR = "/Users/ndaicnsarizona/Desktop/deidentification"

DJANGO_PROJECT_DIR = os.path.join(
    BASE_REPO_DIR,
    "deIdentification",
)
sys.path.insert(0, DJANGO_PROJECT_DIR)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

try:
    import django
    django.setup()
except Exception as e:
    logger.error("Failed to setup Django: %s", e)
    sys.exit(1)

# Now import Django models
from nd_api.models import DbDetailsModel, TableDetailsModel
from nd_api.views.de_identification_task import create_deidentification_task
from worker.models import Task


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

    logger.debug(
        "✅ Fixed reference values for table_id=%s: patient_id=%s, enc_id=%s",
        table_id,
        patient_id,
        enc_id,
    )


def get_tables_for_deidentification(
    db_name: str,
    table_status: int = 0,
) -> list[TableDetailsModel]:
    """Return tables in the given DB with the requested de-id status."""
    db = DbDetailsModel.objects.get(db_name=db_name)

    tables = list(
        TableDetailsModel.objects.filter(db=db, table_name = 'abncodes')#table_status=table_status)
    )

    logger.info(
        "✅ Found %d tables for deidentification (db=%s, status=%s)",
        len(tables),
        db_name,
        table_status,
    )
    return tables


def update_table_configs() -> None:
    """Update reference_mapping config for specific tables before creating deidentification tasks."""
    table_configs = [
        {
            "table_name": "edi_inv_cpt",
            "reference_mapping": {
                "conditions": [
                    {
                        "column_name": "Id",
                        "source_column": "InvoiceId",
                        "reference_table": "edi_invoice",
                    }
                ],
                "source_table": "edi_inv_cpt",
                "destination_column": "PatientId",
                "destination_column_type": "patient_id",
            },
        },
        {
            "table_name": "edi_inv_diagnosis",
            "reference_mapping": {
                "conditions": [
                    {
                        "column_name": "Id",
                        "source_column": "InvoiceId",
                        "reference_table": "edi_invoice",
                    }
                ],
                "source_table": "edi_inv_diagnosis",
                "destination_column": "PatientId",
                "destination_column_type": "patient_id",
            },
        },
        {
            "table_name": "hl7labdatadetail",
            "reference_mapping": {
                "conditions": [
                    {
                        "column_name": "ReportId",
                        "source_column": "ReportId",
                        "reference_table": "labdata",
                    }
                ],
                "source_table": "hl7labdatadetail",
                "destination_column": "EncounterId",
                "destination_column_type": "encounter_id",
            },
        },
        {
            "table_name": "hl7labnotes",
            "reference_mapping": {
                "conditions": [
                    {
                        "column_name": "ReportId",
                        "source_column": "reportid",
                        "reference_table": "labdata",
                    }
                ],
                "source_table": "hl7labnotes",
                "destination_column": "EncounterId",
                "destination_column_type": "encounter_id",
            },
        },
        {
            "table_name": "labdataex",
            "reference_mapping": {
                "conditions": [
                    {
                        "column_name": "ReportId",
                        "source_column": "reportId",
                        "reference_table": "labdata",
                    }
                ],
                "source_table": "labdataex",
                "destination_column": "EncounterId",
                "destination_column_type": "encounter_id",
            },
        },
    ]

    for config in table_configs:
        table_name = config["table_name"]
        reference_mapping = config["reference_mapping"]

        try:
            table_obj = TableDetailsModel.objects.get(table_name=table_name)
            if not table_obj.table_details_for_ui:
                table_obj.table_details_for_ui = {}
            table_obj.table_details_for_ui["reference_mapping"] = reference_mapping
            table_obj.save()
            logger.info("✅ Updated reference_mapping config for table: %s", table_name)
        except TableDetailsModel.DoesNotExist:
            logger.debug("⚠️ Table '%s' not found, skipping config update", table_name)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "⚠️ Failed to update config for table '%s': %s", table_name, e
            )

def create_deidentification_tasks_for_tables(
    db_name: str,
    table_status: int = 0,
) -> dict:
    """Create deidentification tasks for all matching tables."""
    delete_existing_tasks = True
    fix_references = True

    stats = {
        "total_tables": 0,
        "successful": 0,
        "failed": 0,
        "failed_tables": [],
    }

    tables = get_tables_for_deidentification(db_name=db_name, table_status=table_status)
    stats["total_tables"] = len(tables)

    if not tables:
        logger.warning("⚠️ No tables found for deidentification")
        return stats

    # Update table configs before creating deidentification tasks
    logger.info("Updating table configurations...")
    update_table_configs()

    for table_obj in tables:
        table_name = table_obj.table_name
        logger.info("Processing table: %s", table_name)

        try:
            # Delete existing tasks if requested
            if delete_existing_tasks:
                deleted_count, _ = Task.objects.filter(
                    arguments__table_id=table_obj.id
                ).delete()
                if deleted_count:
                    logger.debug(
                        "  Deleted %d existing tasks for %s", deleted_count, table_name
                    )

            # Fix reference values if requested
            if fix_references:
                try:
                    fix_reference_value(table_obj.id)
                    table_obj.refresh_from_db()
                except Exception as e:  # noqa: BLE001
                    logger.warning("  ⚠️ Could not fix reference values for %s: %s", table_name, e)

            # Create deidentification tasks
            tasks, chain = create_deidentification_task(table_obj, delete_table=True)
            logger.info(
                "  ✅ Created %d tasks for %s (rows: %s, chain_id=%s)",
                len(tasks),
                table_name,
                table_obj.rows_count,
                getattr(chain, "id", None),
            )
            stats["successful"] += 1

        except Exception as e:
            logger.error("  ❌ Failed to create tasks for %s: %s", table_name, e)
            stats["failed"] += 1
            stats["failed_tables"].append(table_name)
            import traceback

            logger.debug(traceback.format_exc())

    logger.info(
        "✅ Deidentification task creation completed: total=%s, successful=%s, failed=%s",
        stats["total_tables"],
        stats["successful"],
        stats["failed"],
    )

    if stats["failed_tables"]:
        logger.warning("⚠️ Failed tables: %s", stats["failed_tables"])

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deidentification process")

    parser.add_argument(
        "--db_name",
        required=True,
        help="Database name in DbDetailsModel (e.g. 'deid_prod')",
    )
    parser.add_argument(
        "--table_status",
        type=int,
        default=0,
        help="Table status filter (0=pending, 1=in progress, 2=completed, 3=failed)",
    )

    args = parser.parse_args()

    try:
        # Check that DB exists
        try:
            db = DbDetailsModel.objects.get(db_name=args.db_name)
            logger.info("✅ Found database: %s (id=%s)", args.db_name, db.id)
        except DbDetailsModel.DoesNotExist:
            logger.error("❌ Database '%s' does not exist. Run deid_setup.py first.", args.db_name)
            sys.exit(1)

        stats = create_deidentification_tasks_for_tables(
            db_name=args.db_name,
            table_status=args.table_status,
        )

        if stats["failed"] > 0:
            logger.warning("⚠️ Completed with %s failures", stats["failed"])
            sys.exit(1)

        logger.info("✅ Deidentification run completed successfully")

    except Exception as e:  # noqa: BLE001
        logger.error("❌ Deidentification run failed: %s", e)
        import traceback

        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()