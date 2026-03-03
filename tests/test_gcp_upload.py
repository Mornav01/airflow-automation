# import os
# import django
# import sys

# # Set up Django environment
# sys.path.append(
#     "/Users/rohit.chouhan/NEDI/CODE/Dump/Project/DeIdentification/deIdentification"
# )
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
# os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
# django.setup()

# from core.ops.cloudclient.gcp import GCPClient
from google.cloud import storage
import os
import traceback
import hashlib
from urllib.parse import urlparse, parse_qs
import subprocess
from google.cloud import storage
import tempfile
from django.conf import settings


# # upload_file_from_fs(self, source, filename, full_path_to_file, content_type="application/octet-stream")
# full_path_to_file = '/Users/rohit.chouhan/NEDI/CODE/Dump/Project/deidentification/Dockerfile'
# source = 'testing'
# GCPClient.upload_file_from_fs()

CLOUD_BUCKET_NAME = "databricks-171764875885623"
blob_path = "testing_flow/testing/Dockerfile"



def calculate_checksum(file_path, algorithm="md5"):
    hash_func = hashlib.new(algorithm)
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except Exception as e:
        return str(e)

def parse_mysql_connection_string(conn_str):
    url = urlparse(conn_str)
    return {
        "host": url.hostname,
        "user": url.username,
        "password": url.password,
        "database": url.path.lstrip("/"),
        "port": url.port or 3306
    }


def take_dump_and_upload_to_cloud(table_name, conn_str):
    creds = parse_mysql_connection_string(conn_str)
    blob_name = f"testing_flow/testing/{table_name}.sql"
    with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as temp_file:
        dump_path = temp_file.name
        
        cmd = [
            "mysqldump",
            "-h", creds["host"],
            "-u", creds["user"],
            f"--password={creds['password']}",
            creds["database"],
            table_name,
            "--single-transaction",
            "--quick",
            "--no-create-db"
        ]

        with open(dump_path, "w") as f:
            subprocess.run(cmd, stdout=f, check=True)
        md5sum = calculate_checksum(dump_path)
        print(md5sum)
        upload_to_gcs(dump_path, blob_name)


def upload_to_gcs(file_path, blob_name):
    client = storage.Client()
    bucket = client.bucket(CLOUD_BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    print(f"Uploaded {file_path} to gs://{CLOUD_BUCKET_NAME}/{blob_name}")


conn_str = "mysql+pymysql://root:123456789@localhost:3306/nddenttest_mapping"
table_name = "encounter_mapping_table"

take_dump_and_upload_to_cloud(table_name, conn_str)
