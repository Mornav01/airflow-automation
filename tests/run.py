import os
import django
import sys
# Set up Django environment
sys.path.append('C:\\ROHITCHOUHAN\\PORTAL\\deidentification\\deIdentification')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()
import json
import traceback
from django.conf import settings
from django.db import transaction
from rest_framework import status
from worker.models import Task, Chain
from rest_framework.views import APIView
from core.dbPkg.dbhandler import NDDBHandler
from rest_framework.response import Response
from nd_api.models import TableDetailsModel, DbDetailsModel, IgnoreRowsDeIdentificaiton
from core.process.main import start_de_identification_for_table

current_file_name = os.path.splitext(os.path.basename(__file__))[0]
logs_file  = f'C:\\ROHITCHOUHAN\\PORTAL\\deidentification\\logs\\{current_file_name}.json'
os.makedirs(os.path.dirname(logs_file), exist_ok=True)

try:
    logs_json = json.load(open(logs_file), 'r')
except:
    logs_json = {}

 
tables =[{'table_name': "pmorders", "offset": 0},
         {'table_name': "reconciliation", "offset": 0},
         {'table_name': "hl7labnotes", "offset": 0}
 ]
import csv
import json
def fix_reference_value(table_id):
    table = TableDetailsModel.objects.get(id=table_id)
    
    pid_col = [
        col['column_name'] for col in table.table_details_for_ui['columns_details'] if col["de_identification_rule"] == "PATIENT_ID"
    ]
    enc_col = [
        col['column_name'] for col in table.table_details_for_ui['columns_details'] if col["de_identification_rule"] == "ENCOUNTER_ID"
    ]
    print(pid_col)
    print(enc_col)
    patient_id = pid_col[0] if len(pid_col)>0 else None
    enc_id = enc_col[0] if len(enc_col)>0 else None
    table.table_details_for_ui['reference_enc_id_column'] = enc_id
    table.table_details_for_ui['reference_patient_id_column'] = patient_id
    table.save()

def has_notes(table_id):
    table = TableDetailsModel.objects.get(id=table_id)
    
    pid_col = [
        col['column_name'] for col in table.table_details_for_ui['columns_details'] if col["de_identification_rule"] == "NOTES"
    ]
    return len(pid_col)>0

csv_file = "C:\\ROHITCHOUHAN\\PORTAL\\deidentification\\NOTEBOOK\\row_count.csv"  # Replace with your actual CSV filename

data_dict = {}

# Read CSV and convert to dictionary
with open(csv_file, mode="r", encoding="utf-8") as file:
    reader = csv.reader(file)
    next(reader)  # Skip header row
    for row in reader:
        table_name, row_count = row
        data_dict[table_name] = int(row_count)
    

texas_join = {
    "questaoeans": {
        "source_table": "questaoeans",
        "destination_column": "EncounterId",
        "destination_column_type": "encounter_id",
        "conditions": [
            {
                "source_column": "ReportId",
                "reference_table": "labdata",
                "column_name": "ReportId",
            }
        ],
    },
    "emrereferralattachments": {
        "source_table": "emrereferralattachments",
        "destination_column": "patientID",
        "destination_column_type": "patient_id",
        "conditions": [
            {
                "source_column": "ReferralReqId",
                "reference_table": "referral",
                "column_name": "ReferralId",
            }
        ],
    },
    "edi_inv_cpt": {
        "source_table": "edi_inv_cpt",
        "destination_column": "PatientId",
        "destination_column_type": "patient_id",
        "conditions": [
            {
                "source_column": "InvoiceId",
                "reference_table": "edi_invoice",
                "column_name": "Id",
            }
        ],
    },
    "edi_inv_diagnosis": {
        "source_table": "edi_inv_diagnosis",
        "destination_column": "PatientId",
        "destination_column_type": "patient_id",
        "conditions": [
            {
                "source_column": "InvoiceId",
                "reference_table": "edi_invoice",
                "column_name": "Id",
            }
        ],
    },
    "hl7labdatadetail": {
        "source_table": "hl7labdatadetail",
        "destination_column": "EncounterId",
        "destination_column_type": "encounter_id",
        "conditions": [
            {
                "source_column": "ReportId",
                "reference_table": "labdata",
                "column_name": "ReportId",
            }
        ],
    },
    "hl7labnotes": {
        "source_table": "hl7labnotes",
        "destination_column": "EncounterId",
        "destination_column_type": "encounter_id",
        "conditions": [
            {
                "source_column": "reportid",
                "reference_table": "labdata",
                "column_name": "ReportId",
            }
        ],
    },
    "labattachment_archive": {
        "source_table": "labattachment_archive",
        "destination_column": "EncounterId",
        "destination_column_type": "encounter_id",
        "conditions": [
            {
                "source_column": "reportid",
                "reference_table": "labdata",
                "column_name": "ReportId",
            }
        ],
    },
    "labdatadetail": {
        "source_table": "labdatadetail",
        "destination_column": "EncounterId",
        "destination_column_type": "encounter_id",
        "conditions": [
            {
                "source_column": "reportid",
                "reference_table": "labdata",
                "column_name": "ReportId",
            }
        ],
    },
    "labdataex": {
        "source_table": "labdataex",
        "destination_column": "EncounterId",
        "destination_column_type": "encounter_id",
        "conditions": [
            {
                "source_column": "reportId",
                "reference_table": "labdata",
                "column_name": "ReportId",
            }
        ],
    },
    "laborders": {
        "source_table": "laborders",
        "destination_column": "EncounterId",
        "destination_column_type": "encounter_id",
        "conditions": [
            {
                "source_column": "ReportIds",
                "reference_table": "labdata",
                "column_name": "ReportId",
            }
        ],
    },
    "labordersdetails": {
        "source_table": "labordersdetails",
        "destination_column": "EncounterId",
        "destination_column_type": "encounter_id",
        "conditions": [
            {
                "source_column": "reportId",
                "reference_table": "labdata",
                "column_name": "ReportId",
            }
        ],
    },
    "oldrxdetail": {
        "source_table": "oldrxdetail",
        "destination_column": "encounterId",
        "destination_column_type": "encounter_id",
        "conditions": [
            {
                "source_column": "OldRxId",
                "reference_table": "oldrxmain",
                "column_name": "OldRxId",
            }
        ],
    },
    "recelectroniclabresults": {
        "source_table": "recelectroniclabresults",
        "destination_column": "patientID",
        "destination_column_type": "patient_id",
        "conditions": [
            {
                "source_column": "MessageId",
                "reference_table": "electroniclabresults",
                "column_name": "messageid",
            }
        ],
    },
    "reconciliation": {
        "source_table": "reconciliation",
        "destination_column": "patientID",
        "destination_column_type": "patient_id",
        "conditions": [
            {
                "source_column": "messageid",
                "reference_table": "electroniclabresults",
                "column_name": "messageid",
            }
        ],
    },
}

for table_dict in tables:
    table = table_dict['table_name']
    offset = table_dict['offset']
    try:
        batch_size = settings.BATCH_SIZE_DURING_DE_IDENTIFICATION
        table_obj = TableDetailsModel.objects.get(table_name=table)
        Task.objects.filter(arguments__table_id=table_obj.id).delete()
        table_obj.marked_as_not_started()
        table_obj.refresh_from_db()
        if table in texas_join:
            table_obj.table_details_for_ui['reference_mapping'] = texas_join[table]
        table_obj.rows_count = data_dict[table]
        table_obj.save()
        table_obj.refresh_from_db()
        fix_reference_value(table_obj.id)
        
        dest_connection: NDDBHandler = table_obj.db.get_destination_db_connection()
        dest_connection.drop_table(table_obj.table_name)
        ignore_rows = IgnoreRowsDeIdentificaiton.objects.filter(db_name=table_obj.db.db_name, table_name=table_obj.table_name)
        print(f"Dropping ignore fors for {table_obj.table_name}, {table_obj.db.db_name}")
        ignore_rows.delete()
        tables_config = table_obj.table_details_for_ui
        for offset in range(offset, table_obj.rows_count, batch_size):
            if offset in logs_json.get(table, {}).get('done', []):
                continue
            start_de_identification_for_table(table_obj.id, batch_size, offset, tables_config)
            print(f"{table}, offset: {offset}, batch_size: {batch_size}")

            if table not in logs_json:
                logs_json[table] = {"done": [], "failed": []}
            logs_json[table]['done'].append(offset)
        
        table_obj.refresh_from_db()
        table_obj.marked_as_completed()

        with open(logs_file, "w", encoding="utf-8") as file:
            json.dump(logs_json, file, indent=4)

        

    except Exception as e:
        print(f"failed table: {table}, {e}")
        if table not in logs_json:
            logs_json[table] = {"done": [], "failed": []}
        logs_json[table]['failed'].append({"offest": offset, "remark": str(e)})
        with open(logs_file, "w", encoding="utf-8") as file:
            json.dump(logs_json, file, indent=4)


with open(logs_file, "w", encoding="utf-8") as file:
    json.dump(logs_json, file, indent=4)

