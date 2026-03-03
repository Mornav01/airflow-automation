import os
import django
import sys

# Set up Django environment
sys.path.append(
    "/Users/rohit.chouhan/NEDI/CODE/Dump/Project/DeIdentification/deIdentification"
)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


from nd_api.models import DbDetailsModel, TableDetailsModel
from worker.models import Task, Chain
from django.contrib.auth.models import User
from nd_scripts.create_account import create_account


def clean_db():
    RoleModel.objects.all().delete()
    User.objects.all().delete()
    DbDetailsModel.objects.all().delete()
    Chain.objects.all().delete()


from core.process.rules.helper import remove_ignored_rows, check_if_need_to_ignore_column, check_if_need_to_deidentify_column

rows = [
    {"UserType": 10, "patient_name" : "p1"},
    # {"UserType": 5, "patient_name" : "p2"},
    # {"UserType": 2, "patient_name" : "p3"},
    # {"UserType": 3, "patient_name" : "p4"},
    # {"UserType": 4, "patient_name" : "p5"}
]
config = {
    "ignore_column": {
        "operation": "or",
        "columns": [
            {"name": "UserType", "value": 10, "condition": "neq"},
            # {"name": "UserType", "value": 5, "condition": "eq"}
        ]
    }
}

ig = check_if_need_to_ignore_column(config, rows[0])
# ig = check_if_need_to_deidentify_column(config, rows[0])
print(ig)
# after_remove = remove_ignored_rows(rows, config)
# print(after_remove)
# breakpoint()