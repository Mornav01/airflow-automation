
def bootstrap_django():

    import os
    import sys
    import django

    sys.path.append("/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/")

    os.environ.setdefault(
        "DJANGO_SETTINGS_MODULE",
        "deIdentification.deIdentification.settings")

    os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

    django.setup()


bootstrap_django()

print("django setup done")

from deIdentification.nd_api.models import DbDetailsModel, TableDetailsModel

print("imported packages")

db = DbDetailsModel.objects.get(
        db_name="arizona_mobiledoc_dec_diff"
    )
    
print(db)
print(db.db_name)

tables = list(
        TableDetailsModel.objects.filter(
            db=db,
            table_name="enc"
        ).values(
            "id",
            "table_name",
            "table_details_for_ui"
        )
    )
print(tables)
