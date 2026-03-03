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
I reported a patient whose name is Rohit Chouhan with patient-id 12345, his dob is 15-08-1999 or 15/08/1999 or 15Aug 1999 in hospital dent neurology institute,
we checked Mr. Rohit blood group as well and also Mr Chouhan reported covid negative his email is rohit@gmail.com,
and his ssn number is 123-45-6789, his employername is Neurodiscovery.ai. also his encounter id is 33333. his zipcode is 451442,
mobile no. 9198960554
"""
master_pii_dict = {
    "mask": {
        "uname": {
        "masking_value": "((PATIENTNAME))",
        "regex": None,
        "processing_func": None,
    },
    "ufname": {"masking_value": "((PATIENTFIRSTNAME))", "regex": None},
    "ulname": {
        "masking_value": "((PATIENTLASTNAME))",
        "regex": None,
        "processing_func": None,
    },
    "mobile": {"masking_value": "((PHONENUMBER))", "regex": None},
    "zipcode": {"masking_value": "((ZIPCODE))", "regex": None, "processing_func": None},
    "employername": {"masking_value": "((EMPLOYERNAME))", "regex": None},
    "patient_id": {
        "masking_value": "((PATIENT_ID))",
        "regex": None,
        "processing_func": None,
    },
    "encounter_id": {
        "masking_value": "((ENCOUNTER_ID))",
        "regex": None,
        "processing_func": None,
    },
    },
    "combine": {
        "patientfullname": {
            "combine": ["ufname", "uminitial", "ulname"],
            "masking_value": "((PATIENTNAME))",
            "regex": None,
            "processing_func": None,
        },
    },
    "dob": {
        "dob": {"masking_value": "((DOB))", "regex": None, "processing_func": None},
    },
    "regex": {
        "email": {
            "masking_value": "((EMAIL))",
            "regex": [r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"],
            "processing_func": None,
        },
        "ssn": {
            "masking_value": "((SSN))",
            "regex": [r"\b\d{3}[- ]\d{2}[- ]\d{4}\b"],
            "processing_func": None,
        },
        "umobileno": {
            "masking_value": "((PHONENUMBER))",
            "regex": [
                r"\(\d{3}\)\s*\d{3}-\d{4}",  # (XXX) XXX-XXXX
                r"\b\d{3}-\d{3}-\d{4}\b",  # XXX-XXX-XXXX
                r"\+\d{1,3}\s\d{1,4}[- ]\d{3}[- ]\d{4}\b",  # International format
                r"\b\d{3} \d{3} \d{4}\b",  # XXX XXX XXXX (with spaces)
            ],
            "processing_func": None,
        },
    }
}

pii_data = {
    "patient_id": 12345,
    "encounter_id": 33333,
    "uname": "ROHIT CHOUHAN",
    "ufname": "Rohit",
    "ulname": "Chouhan",
    "mobile": 9198960554,
    "zipcode": 451442,
    "employername": "neurodiscovery.ai",
    "dob": "15/08/1999",
    "email": "rohit@gmail.com",
    "ssn": 9834234564
}
pii_deidentifier = PIIValuesMasking(
    pii_config=master_pii_dict,
    pii_data=pii_data,
    text=medical_report,
    date_parse_cache={}
)
notes_text = pii_deidentifier.deidentify()
print(notes_text)


import re
import dateparser

def normalize_date_format(date_str):
    date_str = re.sub(r"[-.]", "/", date_str)

    if "/" in date_str:
        parts = date_str.split("/")
        if len(parts) == 3:
            month, day, year = parts
            if len(year) == 2:
                year = "20" + year if int(year) < 50 else "19" + year
                date_str = f"{month}/{day}/{year}"

    return date_str

date_str = "2024-05-05"

parsed_date = dateparser.parse(
    date_str,
    settings={
        "RELATIVE_BASE": reference_date,
        "RETURN_AS_TIMEZONE_AWARE": False,
        "DATE_ORDER": "MDY",
    },
)