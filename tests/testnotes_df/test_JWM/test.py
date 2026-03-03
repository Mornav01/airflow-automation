import os
import django
import sys
# Set up Django environment
sys.path.append('/Users/ndaijwm/Documents/deidentification_df/deidentification/deIdentification/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pandas as pd
from nd_api.views.de_identification_task import create_deidentification_task
from nd_api.models import DbDetailsModel, TableDetailsModel, IgnoreRowsDeIdentificaiton
from worker.models import Task, Chain
from django.contrib.auth.models import User
from keycloakauth.rolemodel import RoleModel, get_default_permissions
from nd_scripts.create_account import create_account
from nd_api.views.db_views import create_stats_generation_tasks
from core.process_df.main import start_de_identification_for_table
from nd_api.views.de_identification_task import create_deidentification_task
from nd_api.views.db_views import run_stats_generation_task, _get_default_table_details_for_ui

def clean_db():
    RoleModel.objects.all().delete()
    User.objects.all().delete()
    DbDetailsModel.objects.all().delete()
    Chain.objects.all().delete()


db, _ = DbDetailsModel.objects.get_or_create(db_name="jwm_deidentification")
t = TableDetailsModel.objects.get(table_name = 'PatMedRecon', db=db)

start_de_identification_for_table(t.id, 10000, 0, t.table_details_for_ui)