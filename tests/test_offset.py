
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


from core.process.rules.ops import DateOffsetDeIdntRule

from datetime import datetime

current_time = datetime.now()


# row = {"date": current_time}
row = {"date": "01-13-2021"}
# print(current_time)
a = DateOffsetDeIdntRule()
out = a.fill_offset_value({}, {}, {"patient_id": None})
print(out)
# val = a.de_identify_value("rohit_test", {"column_name": "date"}, row, {}, {}, {"offset_value": 15})
val = a.de_identify_value("rohit_test", {"column_name": "date"}, row, {}, {}, out)
print(val)
