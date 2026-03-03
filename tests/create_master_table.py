import os
import django
import sys

# Set up Django environment
sys.path.append(
    "/Users/ndaineurocenterpa/Desktop/De-identification/deidentification/deIdentification"
)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from core.dbPkg.phi_table.create_table import PIITable



pii_tables_config = {
    "pii_data_table": {
        "primary_column_name": "patient_id",
        "upsert_instead_of_append": True, # only if primary_column is None
        "tables": {
            "PATIENT": {
                "primary_col": "PATIENTID",
                "other_required_columns": ["CONTEXTID", "CONTEXTNAME", "ENTERPRISEID", "FIRSTNAME", "LASTNAME", "MIDDLEINITIAL", "NAMESUFFIX", "DOB", "ADDRESS", "ADDRESS2", "CITY", "ZIP", "PATIENTEMPLOYERID", "PATIENTHOMEPHONE", "WORKPHONE", "MOBILEPHONE", "CONTACTPREFERENCE", "EMAIL", "NEWPATIENTID", "GUARANTORFIRSTNAME", "GUARANTORLASTNAME", "GUARANTORMIDDLEINITIAL", "GUARANTORNAMESUFFIX", "GUARANTORDOB", "GUARANTORSSN", "GUARANTORADDRESS", "GUARANTORADDRESS2", "GUARANTORCITY", "GUARANTORZIP", "GUARANTOREMAIL", "GUARANTOREMPLOYERID", "GUARDIANFIRSTNAME", "GUARDIANLASTNAME", "GUARDIANMIDDLEINITIAL", "GUARDIANNAMESUFFIX", "EMERGENCYCONTACTNAME", "EMERGENCYCONTACTRELATIONSHIP", "EMERGENCYCONTACTPHONE", "PATIENTSSN", "GUARANTORPHONE", "TESTPATIENTYN", "TRANSLATEDHOMEPHONEINDEX", "TRANSLATEDMOBILEPHONEINDEX", "TRANSLATEDWORKPHONEINDEX", "LASTUPDATED", "DELETEDDATETIME"]
            }
        }
    }
}


import urllib.parse

encoded_password = urllib.parse.quote_plus("ndADMIN@2025")
# Connection String with Encoded Password
src_db_url = "mysql+pymysql://ndadmin:ndADMIN%402025@localhost:3306/athenaone"
dest_db_url = "mysql+pymysql://ndadmin:ndADMIN%402025@localhost:3306/master"



# src_db_url = "mysql+pymysql://root:Texas%402025@localhost/mobiledoc"
# dest_db_url = "mysql+pymysql://root:Texas%402025@localhost/master"
pii = PIITable(src_db_url, dest_db_url, pii_tables_config)
pii.generate_pii_tables()