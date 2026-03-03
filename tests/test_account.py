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
from nd_scripts.create_account import create_account


def clean_db():
    # RoleModel.objects.all().delete()
    # User.objects.all().delete()
    DbDetailsModel.objects.all().delete()
    Chain.objects.all().delete()


username = "rohit10"
user_details = {
    "username": username,
    "email": f"{username}@gmail.com",
    "password": username,
    "first_name": username,
    "last_name": username
}
create_account(user_details, {})