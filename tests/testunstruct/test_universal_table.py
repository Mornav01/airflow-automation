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

from core.process.rules.unstruct.pii_mask import PIIValuesMasking

medical_report = """
Texas Neurology Institute, Rohit
"""

loaded_tables = {
    "facility": {
        "metadata": {
            "Name": "((FacilityName))",
            "EMail": "((FacilityDetails))"
        },
        "rows": [{"Name": "Texas Neurology Institute", "Email": "rohit@gmail.com"}]
    }
}
from core.process.rules.unstruct.universal_mask import UniversalPIIDeIdentifier

universal_deidentifier = UniversalPIIDeIdentifier(medical_report, loaded_tables, {})
notes_text = universal_deidentifier.deidentify()

breakpoint()
        
