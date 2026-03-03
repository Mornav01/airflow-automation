import os
import sys
import django
import pandas as pd
import random
from faker import Faker
from tqdm import tqdm
import time

# ---- Setup Django ----
sys.path.append('/Users/karanchilwal/Documents/project/deidentification/deIdentification/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from core.process_df.unstruct.notes import NotesRule

# ---- Dummy Config ----
class DummyDBDetails:
    def get_pii_config(self):
        return {
            "mask": {
                
                "PATIENT_FIRSTNAME": {"regex": None, "masking_value": "((PATIENT_NAME))", "processing_func": None},
                "PATIENT_EMAIL": {"regex": None, "masking_value": "((EMAIL_ID))", "processing_func": None},
                "PATIENT_LASTNAME": {"regex": None, "masking_value": "((PATIENT_LASTNAME))", "processing_func": None},
                
            },
            "dob": {
                "PATIENT_DOB": {"regex": None, "masking_value": "((PATIENT_DOB))", "processing_func": None},
            },
            "combine": {
                
                "fullname": {
                    "regex": None,
                    "combine": ["PATIENT_FIRSTNAME", "PATIENT_LASTNAME"],
                    "masking_value": "((PATIENT_NAME))",
                    "processing_func": None,
                }
                
            },
            
            "replace_value": [
                {"old_value": "ABC Hospital", "new_value": "((FACILITY_NAME))"}
            ]
        }
    
    def get_pii_config(self):
        return {}

    def get_pii_db_config(self):
        return {}
    
    def get_secondary_pii_config(self):
        return {}

# ---- Constants ----
DATA_PATH = "huge_text.csv"
PII_PATH = "dummy_pii.csv"
NUM_ROWS = 10_000
NUM_PATIENTS = 2000
CHUNK_SIZE = 10000

# ---- Generate or Load Data ----
if os.path.exists(DATA_PATH) and os.path.exists(PII_PATH):
    print(f"[INFO] Loading cached data from {DATA_PATH} and {PII_PATH}")
    df_main = pd.read_csv(DATA_PATH)
    dummy_pii_df = pd.read_csv(PII_PATH)
else:
    print("[INFO] Generating synthetic data...")
    fake = Faker()

    dummy_pii_df = pd.DataFrame({
        "patient_id": list(range(1, NUM_PATIENTS + 1)),
        "PATIENT_FIRSTNAME": [fake.first_name() for _ in range(NUM_PATIENTS)],
        "PATIENT_LASTNAME": [fake.last_name() for _ in range(NUM_PATIENTS)],
        "PATIENT_EMAIL": [fake.email() for _ in range(NUM_PATIENTS)],
        "PATIENT_DOB": [fake.date_of_birth(minimum_age=20, maximum_age=90).isoformat() for _ in range(NUM_PATIENTS)],
    })

    notes = []
    pids = []
    for _ in tqdm(range(NUM_ROWS), desc="Generating rows"):
        pid = random.randint(1, NUM_PATIENTS)
        row = dummy_pii_df[dummy_pii_df['patient_id'] == pid].iloc[0]
        pii_part = (
            f"Patient {row['PATIENT_FIRSTNAME']} {row['PATIENT_LASTNAME']} "
            f"visited ABC Hospital. Email: {row['PATIENT_EMAIL']}. DOB: {row['PATIENT_DOB']}.\n"
        )
        text_block = " ".join([fake.text(max_nb_chars=2000) for _ in range(20)])  # ~40,000 characters
        notes.append(pii_part + text_block)
        pids.append(pid)

    df_main = pd.DataFrame({
        "_resolved_patient_id": pids,
        "note_text": notes
    })

    print("[INFO] Saving generated data...")
    df_main.to_csv(DATA_PATH, index=False)
    dummy_pii_df.to_csv(PII_PATH, index=False)

# ---- Patch NotesRule ----
class TestNotesRule(NotesRule):
    def _get_pii_data_table(self, patient_ids: list):
        self.pii_data_df = dummy_pii_df[dummy_pii_df['patient_id'].isin(patient_ids)]

notes_rule = TestNotesRule(DummyDBDetails(), key_phi_columns=([], ["patient_id"], [], []))

# ---- Apply Masking in Batches ----
print("[INFO] Starting de-identification in chunks...")
results = []

for i in tqdm(range(0, len(df_main), CHUNK_SIZE), desc="De-identifying chunks"):
    chunk = df_main.iloc[i:i + CHUNK_SIZE].copy()

    start_time = time.perf_counter()
    result_chunk = notes_rule.apply(chunk, {
        'column_name': 'note_text',
        'is_phi': True,
        'de_identification_rule': 'GENERIC_NOTES',
        'add_to_phi_table': False,
        'column_name_for_phi_table': None,
        'ignore_rows': {},
        'reference_mapping': {}
    })
    end_time = time.perf_counter()

    print(f"[Chunk {i // CHUNK_SIZE + 1}] Processed {len(chunk)} rows in {end_time - start_time:.2f} sec")
    results.append(result_chunk)

# ---- Combine and Save ----
final_df = pd.concat(results, ignore_index=True)
print("[INFO] De-identification complete. Final shape:", final_df.shape)
print(final_df[['note_text']].head(3))
final_df.to_csv("masked_output.csv", index=False)
