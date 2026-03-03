import mysql.connector
from faker import Faker
from tqdm import tqdm
import random
from datetime import datetime, timedelta

MAX_PATIENT_ID_VALUE = 100000  # Adjust as needed
MAX_ENC_COUNT_FOR_A_PATIENT = 5

fake = Faker()
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Neuro@123",
    database="ndsource",
)
cursor = connection.cursor()

config = {}

create_table_query = """
CREATE TABLE IF NOT EXISTS enc_table (
    encounterID INT PRIMARY KEY,
    patientID INT,
    doctorID INT,
    date DATE,
    startTime TIME,
    endTime TIME,
    facilityID INT,
    reason VARCHAR(255),
    dateIn VARCHAR(50),
    dateOut VARCHAR(50),
    surgicalModifiedDate VARCHAR(50)
    );
"""
cursor.execute(create_table_query)
insert_query = """
INSERT INTO enc_table (encounterID, patientID, doctorID, date, startTime, endTime, facilityID, reason, dateIn, dateOut, surgicalModifiedDate) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
# Predefined list of medical reasons
medical_reasons = [
    "Routine Check-up",
    "Follow-up",
    "Fever",
    "Cold and Flu",
    "Allergy",
    "Headache",
    "Back Pain",
    "Stomach Ache",
    "Skin Rash",
    "High Blood Pressure",
    "Diabetes Management",
    "Cholesterol Check",
    "Injury Treatment",
    "Mental Health Counseling",
    "Vaccination",
    "Physical Therapy",
    "Pregnancy Check-up",
    "Annual Physical Exam",
    "Sleep Disorder",
    "Eye Check-up",
]
# Set to keep track of unique IDs
used_encounterIDs = set()
used_patientIDs = set()


def generate_unique_ID(existing_set, id_range):
    """Generate a unique ID within a given range."""
    while True:
        new_id = random.randint(*id_range)
        if new_id not in existing_set:
            existing_set.add(new_id)
            return new_id


def generate_random_times():
    """Generate random startTime and endTime where endTime > startTime."""
    start = datetime.strptime(
        f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}",
        "%H:%M:%S",
    )
    delta = timedelta(
        hours=random.randint(0, 5), minutes=random.randint(1, 59)
    )  # Add 0-5 hours and 1-59 minutes
    end = start + delta
    return start.time(), end.time()


def format_human_readable_date(date):
    """Convert a date to a human-readable format like '15th Nov 2024'."""
    suffix = (
        "th"
        if 11 <= date.day <= 13
        else {1: "st", 2: "nd", 3: "rd"}.get(date.day % 10, "th")
    )
    return date.strftime(f"%-d{suffix} %b %Y")


def format_surgical_modified_date(date):
    """Convert a date to a surgicalModifiedDate format like 'November 20, 2024'."""
    return date.strftime("%B %d, %Y")


current_enc_id = 1
for patient_id in tqdm(range(1, MAX_PATIENT_ID_VALUE + 1), desc="generating enc data"):
    
    enc_id_list = []
    num_encounters = random.randint(1, MAX_ENC_COUNT_FOR_A_PATIENT)
    for encounter_index in range(num_encounters):
        enc_id_list.append(current_enc_id)
        doctorID = generate_unique_ID(
            set(), (1000, 1999)
        )
        date = fake.date_between(start_date="-30y", end_date="today")
        startTime, endTime = generate_random_times()
        facilityID = random.randint(1, 10)
        reason = random.choice(medical_reasons)
        datein = fake.date_between(start_date="-1y", end_date="today")
        dateout = datein + timedelta(days=random.randint(1, 10))
        dateIn = format_human_readable_date(datein)
        dateOut = format_human_readable_date(dateout)
        surgicalModifiedDate = format_surgical_modified_date(
            fake.date_between(start_date="-1y", end_date="today")
        )
        cursor.execute(
            insert_query,
            (
                current_enc_id,
                patient_id,
                doctorID,
                date,
                startTime,
                endTime,
                facilityID,
                reason,
                dateIn,
                dateOut,
                surgicalModifiedDate,
            ),
        )
        current_enc_id += 1
    config[patient_id] = enc_id_list


connection.commit()
cursor.close()
connection.close()

import json 
with open("mapping.json", 'w') as fp:
    json.dump(config, fp)
