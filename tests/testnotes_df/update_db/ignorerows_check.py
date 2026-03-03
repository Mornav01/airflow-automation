import os
import django
import sys
import json

# Set up Django environment
sys.path.append('/Users/karanchilwal/Documents/project/deidentification/deIdentification/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from nd_api.models import IgnoreRowsDeIdentificaiton


def get_model_data():
    obj = IgnoreRowsDeIdentificaiton.objects.filter(db_name='northwest_metadata').first()

    if obj:
        print("ID:", obj.id)
        print("DB Name:", obj.db_name)
        print("Table Name:", obj.table_name)
        print("Row (JSON):")
        print(json.dumps(obj.row, indent=2))
    else:
        print("No object found with id=1")


if __name__ == "__main__":
    get_model_data()
