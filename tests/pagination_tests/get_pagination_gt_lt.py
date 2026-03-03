import os
import sys
import django
import pandas as pd
import random
#from faker import Faker
from tqdm import tqdm
import time

# ---- Setup Django ----
sys.path.append('/Users/neurodiscoveryai/Documents/deidentification_df/deidentification/deIdentification/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from nd_api.models import DbDetailsModel, TableDetailsModel
from core.process_df.main import start_de_identification_for_table

tbl = TableDetailsModel.objects.get(table_name='ELIGIBILITYTRACK')
print(tbl.table_name)

start_de_identification_for_table(tbl.id, 10000, {'gt': 1, 'lt':10000}, tbl.table_details_for_ui)