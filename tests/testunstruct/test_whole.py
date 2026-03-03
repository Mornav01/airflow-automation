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

from core.process.rules.unstruct.unstruct import UnstructuredDeidentification

medical_report = """
Fail - Raleigh: Date format 2013.07.18 Raleighneeds to raleigh be deidentified_Raleigh 07.19.23 08.19.23  in NOTE column Raleigh
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
    "ssn": 9834234564,
    "offset_value": 30
}
xml_config = {
    "dob": [
        {
            "tag_name": "ptDOB",
            "default_replacement": "((DOB))",
            "patterns": [
                "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%Y"
            ]
        }
    ],
    "dateoffset": [
        {
            "tag_name": "StartedAt",
            "default_replacement": "((StartedAt))",
            "replacement_type": "pattern",
            "patterns": [
                "%Y-%m-%d", "%m/%d/%Y", "%Y"
            ]
        }
    ],
    "mask": [
        {
            "tag_name": "EmployerName",
            "mask_value": "((EmployerName))",
        },
        {
            "tag_name": "JobTitle",
            "mask_value": "((JobTitle))",
        }
    ],
    "replace_value": [
        {
            "pii_key": "patient_id",
            "old_value": 7890,
            "new_value": 10001110101010
        }
    ]
}
date_offset = 5
dit = UnstructuredDeidentification()
universal_pii_data = {}
medical_report = dit.deidentify(medical_report, pii_data, {}, master_pii_dict, xml_config)
print(medical_report)