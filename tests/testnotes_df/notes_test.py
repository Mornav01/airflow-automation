import os
import django
import sys
# Set up Django environment
sys.path.append('/Users/karanchilwal/Documents/project/deidentification/deIdentification/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pandas as pd
from core.process_df.unstruct.notes import NotesRule

# Dummy DB details object to simulate config
class DummyDBDetails:
    def get_pii_config(self):
        return {
            "mask": {
                
                "PATIENT_FIRSTNAME": {
                    "regex": None,
                    "masking_value": "((PATIENT_NAME_FM))",
                    "processing_func": None,
                },
                "PATIENT_EMAIL": {
                    "regex": None,
                    "masking_value": "((PATIENT_EMAIL))",
                    "processing_func": None,
                },
                "PATIENT_LASTNAME": {
                    "regex": None,
                    "masking_value": "((PATIENT_LASTNAME_FM))",
                    "processing_func": None,
                },
                
            },
            "dob": {
                "PATIENT_DOB": {
                    "regex": None,
                    "masking_value": "((DOB))",
                    "processing_func": None,
                },
                "FATHER_DOB": {
                    "regex": None,
                    "masking_value": "((FATHERSDOB))",
                    "processing_func": None,
                },
                "MOTHER_DOB": {
                    "regex": None,
                    "masking_value": "((MOTHERSDOB))",
                    "processing_func": None,
                }
            },
            "combine": {
                "fullname": {
                    "regex": None,
                    "combine": ["PATIENT_FIRSTNAME", "PATIENT_LASTNAME"],
                    "masking_value": "((PATIENT_NAME_FC))",
                    "processing_func": None,
                }
            },
            "regex": {
                    "random_key": {
                        "masking_value": "((EMAIL_FROM_PII_CONFIG))",
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
                },
            "replace_value": [
                {
                    "old_value": "ABC Hospital",
                    "new_value": "((FACILITY_NAME))"
                }
            ]
        }

    def get_pii_db_config(self):
        # Not used in dummy setup
        return {}
    
    def get_secondary_pii_config(self):
        return {}

# Create dummy main DataFrame with text containing PII
df_main = pd.DataFrame({
    "patient_id": [100, 200, 300, 400, 500, 100, 200, 300, 400, 500],
    "_resolved_patient_id": [100, 2, 3, 4, 5, 1, 2, 3, 4, 5],
    "_resolved_nd_patient_id": [10010017890, 1002, 1003, 1004, 1005, 1001, 1002, 1003,1004, 1005],
    "note_text": [
        "1990-01-01 01 Jan 1967 31/12/1976 Karan PatientJohn visited ABC Hospital 12345678 87654321",
        "1992/02/02 31-12-1977 02/02/1992 Patient Jane's email is jane@example.com ",
        " #300 Wickjohnjohnand Wick are siblings , ",
        "<40> DOB is 1990-01-01 for Jane",
        "ABC Hospital is where John was born",
        "Send email to john@example.com",
        "Patient name: Jane",
        "ndkoandinsjian",
        "Email: john.doe@example.com",
        "Hospital: ABC Hospital",
    ]
})

# Create dummy pii_data_df that would normally come from database
dummy_pii_df = pd.DataFrame({
    "patient_id": [100, 2, 3, 4, 5, 1],
    "PATIENT_FIRSTNAME": ["John", "Jane", "John", "Jane", "John", "Karan"],
    "PATIENT_LASTNAME": ["Doe", "Smith", "Wick", "Watson", "Wayne", "Chilwal"],
    "PATIENT_EMAIL": ["john@example.com", "jane@example.com", "john2@example.com", "jane2@example.com", "john3@example.com", "karanchilwal@gmail.com"],
    "PATIENT_DOB": ["1990-01-01", "1992-02-02", "1990-01-01", "1990-01-01", "1985-05-05", "1928-12-04"],
    "FATHER_DOB": ["1967-01-01", "1992-02-02", "1990-01-01", "1990-01-01", "1985-05-05", "1928-12-04"],
    "MOTHER_DOB": ["12-31-1976", "31-12-1977", "1990-01-01", "1990-01-01", "1985-05-05", "1928-12-04"]
})

# Patch NotesRule to inject dummy PII data
class TestNotesRule(NotesRule):

    def _get_pii_data_table(self, patient_ids: list):
        # Simulate loading filtered PII data for requested patients
        self.pii_data_df = dummy_pii_df[dummy_pii_df['patient_id'].isin(patient_ids)]
        

# Instantiate and apply the rule
notes_rule = TestNotesRule(DummyDBDetails(), key_phi_columns=([], ["patient_id"], [], []))

# Apply the rule
result_df = notes_rule.apply(df_main.copy(), {'column_name': 'note_text', 
                                              'is_phi': True, 
                                              'de_identification_rule': 'GENERIC_NOTES', 
                                              'add_to_phi_table': False, 
                                              'column_name_for_phi_table': None, 
                                              'ignore_rows': {}, 
                                              'reference_mapping': {}})

# Show result
#print(df_main)
#print(dummy_pii_df)

pd.set_option('display.max_colwidth', None)     # Show full content in each column
pd.set_option('display.max_columns', None)      # Show all columns
pd.set_option('display.max_rows', None)         # Optional: show all rows
pd.set_option('display.expand_frame_repr', False)  # Prevents wrapping to next line

print(result_df[["note_text"]])

print(result_df)