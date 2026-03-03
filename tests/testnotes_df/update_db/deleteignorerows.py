import os
import django
import sys

# Set up Django environment
sys.path.append('/Users/karanchilwal/Documents/project/deidentification/deIdentification/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from nd_api.models import IgnoreRowsDeIdentificaiton


def delete_all_ignore_rows():
    count, _ = IgnoreRowsDeIdentificaiton.objects.all().delete()
    print(f"Deleted {count} IgnoreRowsDeIdentificaiton entries.")


if __name__ == "__main__":
    delete_all_ignore_rows()
