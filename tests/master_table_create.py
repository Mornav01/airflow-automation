import os
import django
import sys

# Set up Django environment
sys.path.append(
    "/Users/neurodiscoveryai/Desktop/Soorykant/deidentification/deIdentification"
)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from core.dbPkg.phi_table.create_table import PIITable


pii_table_config = {
    "pii_data_table": {
        "primary_column_name": "patient_id",
        "upsert_instead_of_append": True,
        "tables": {
            "users": {
                "primary_col": "uid",
                "other_required_columns": [
                    "uname",
                    "upwd",
                    "umobileno",
                    "upagerno",
                    "ufname",
                    "uminitial",
                    "ulname",
                    "uemail",
                    "upaddress",
                    "upcity",
                    "upPhone",
                    "dob",
                    "ssn",
                    "upaddress2",
                    "initials",
                    "ptDob",
                    "upreviousname",
                ],
            },
            "patients": {
                "primary_col": "pid",
                "other_required_columns": [
                    "employername",
                    "employeraddress",
                    "employeraddress2",
                    "employercity",
                    "employerPhone",
                    "insname",
                    "insgroupno",
                    "inssubscriberno",
                    "inscopay",
                    "insname2",
                    "insgroupno2",
                    "inssubscriberno2",
                    "inscopay2",
                    "straddress",
                    "city",
                    "insId",
                    "insId2",
                    "strAddress2",
                    "GrId",
                    "preferred_name",
                ],
            },
        },
    }
}



import urllib.parse

encoded_password = urllib.parse.quote_plus("ndADMIN@2025")
# Connection String with Encoded Password
src_db_url = "mssql+pyodbc://sa:ndADMIN2025@localhost:1433/mobiledoc?driver=ODBC+Driver+17+for+SQL+Server"
dest_db_url = f"mysql+pymysql://ndadmin:{encoded_password}2025@localhost:3306/master"



# src_db_url = "mysql+pymysql://root:Texas%402025@localhost/mobiledoc"
# dest_db_url = "mysql+pymysql://root:Texas%402025@localhost/master"
pii = PIITable(src_db_url, dest_db_url, pii_table_config)
pii.generate_pii_tables()