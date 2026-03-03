import mysql.connector
import random

# Database connection parameters
host = "localhost"
user = "root"
password = "Neuro@123"
database = "masterdb"

# Establish the database connection
connection = mysql.connector.connect(
    host=host, user=user, password=password, database=database
)

# Create a cursor object to interact with the database
cursor = connection.cursor()

import json
with open('mapping.json', 'r') as fp:
    config = json.load(fp)

ND_PATIENT_ID_PREFIX = 100100
ND_ENC_ID_PREFIX = 110100
try:
    create_patients_table_query = """
    CREATE TABLE IF NOT EXISTS patient_mapping_table (
        patient_id INT PRIMARY KEY,
        nd_patient_id BIGINT,
        offset INT
    )
    """
    cursor.execute(create_patients_table_query)
    create_encounters_table_query = """
    CREATE TABLE IF NOT EXISTS encounter_mapping_table (
        encounter_id INT PRIMARY KEY,
        nd_encounter_id BIGINT,
        patient_id INT
    )
    """
    cursor.execute(create_encounters_table_query)
    
    for patient_id, enc_id_list in config.items():
        ndPatientID = int(str(ND_PATIENT_ID_PREFIX) + str(patient_id))
        offset = random.randint(-38, -7) if random.choice([True, False]) else random.randint(7, 38)  # Example offset value
        cursor.execute("INSERT INTO patient_mapping_table (patient_id, nd_patient_id, offset) VALUES (%s, %s, %s)", (patient_id, ndPatientID, offset))

        for enc_id in enc_id_list:
            ndEncounterID = int(str(ND_ENC_ID_PREFIX) + str(enc_id))
            cursor.execute("INSERT INTO encounter_mapping_table (encounter_id, patient_id, nd_encounter_id) VALUES (%s, %s, %s)", (enc_id, patient_id, ndEncounterID))
            connection.commit()

except mysql.connector.Error as err:
    print(f"Error: {err}")
    connection.rollback()

finally:
    # Close the cursor and connection
    cursor.close()
    connection.close()
