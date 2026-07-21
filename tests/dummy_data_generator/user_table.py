import os
import mysql.connector
from faker import Faker
from tqdm import tqdm
import random


fake = Faker()
connection = mysql.connector.connect(
    host="localhost",
    user=os.environ.get("DB_USER", ""),
    password=os.environ.get("DB_PASS", ""),
    database="ndsource",
)
MAX_ROW_COUNT = 100000

cursor = connection.cursor()
create_table_query = """
CREATE TABLE IF NOT EXISTS users (
    uid INT PRIMARY KEY,
    patient_name VARCHAR(255),
    uname VARCHAR(255),
    upwd VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    address VARCHAR(255),
    zipcode VARCHAR(20),
    dob DATE,
    email VARCHAR(255),
    ssn VARCHAR(255),
    phone VARCHAR(40),
    sex VARCHAR(10),
    register_date DATE,
    notes VARCHAR(255),
    UserType INT,
    medical_note TEXT
    );
"""
cursor.execute(create_table_query)
insert_query = """
INSERT INTO users (uid, patient_name, uname, upwd, first_name, last_name, address, city, state, zipcode, dob, email, ssn, phone, sex, register_date, notes, UserType, medical_note) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Set to keep track of unique UIDs
used_uids = set()


def generate_unique_uid():
    """Generate a unique 6-digit UID."""
    while True:
        uid = random.randint(100000, 999999)
        if uid not in used_uids:
            used_uids.add(uid)
            return uid

# uid_value = 1

for uid in tqdm(range(1, MAX_ROW_COUNT+1), desc="Inserting Patient Data"):
    # patient_id = fake.uuid4()
    patient_name = fake.name()
    uname = fake.email()
    upwd = fake.password(
        length=12, special_chars=True, digits=True, upper_case=True, lower_case=True
    )
    first_name, last_name = (
        patient_name.split(" ", 1) if " " in patient_name else (patient_name, "")
    )
    address = fake.address()
    city = fake.city()
    state = fake.state()
    zipcode = fake.zipcode()
    dob = fake.date_of_birth(minimum_age=18, maximum_age=90)
    email = uname
    ssn = fake.ssn()
    phone = fake.phone_number()
    sex = random.choice(["male", "female"])
    register_date = fake.date_this_year()
    notes = fake.text(max_nb_chars=200)
    UserType = 3 if random.random() < 0.7 else random.randint(1, 5)

    name_variations = [patient_name, first_name, last_name]
    selected_name = random.choice(name_variations)

    appointment_date = fake.date_this_year()
    follow_up_date = fake.date_between(start_date=appointment_date, end_date='+30d')

    medical_note = (
        f"Patient Name: {selected_name}\n"
        f"Date of Birth: {dob.strftime('%Y-%m-%d')}\n"
        f"Sex: {sex.capitalize()}\n"
        f"Address: {address}\n"
        f"Phone: {phone}\n"
        f"Email: {email}\n"
        f"SSN: {ssn}\n"
        f"Registration Date: {register_date.strftime('%Y-%m-%d')}\n"
        f"Appointment Date: {appointment_date.strftime('%Y-%m-%d')}\n"
        f"Follow-up Date: {follow_up_date.strftime('%Y-%m-%d')}\n"
        f"Notes: {notes}\n"
    )

    cursor.execute(
        insert_query,
        (
            uid,
            patient_name,
            uname,
            upwd,
            first_name,
            last_name,
            address,
            city,
            state,
            zipcode,
            dob,
            email,
            ssn,
            phone,
            sex,
            register_date,
            notes,
            UserType,
            medical_note
        ),
    )

    # uid += 1

connection.commit()
cursor.close()
connection.close()

print("Inserted rows into the Patient table successfully.")
