import mysql.connector
from faker import Faker
from tqdm import tqdm
import random

MAX_ROW_COUNT = 100000
fake = Faker()
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Neuro@123",
    database="ndsource",
)

cursor = connection.cursor()

# Create the table with auto-increment ID and required columns
create_table_query = """
CREATE TABLE IF NOT EXISTS facilities (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255),
    Addressline1 VARCHAR(255),
    Addressline2 VARCHAR(255),
    code VARCHAR(255),
    DeleteFlag TINYINT(1),
    Possible_Addressline1Match VARCHAR(255),
    Dest_child_Abbrevations_Match VARCHAR(50),
    Complete_Possible_Addressline1Match VARCHAR(255)
);
"""
cursor.execute(create_table_query)

# Predefined list of abbreviations for the address suffix
address_abbreviations = ["Dr", "Ave", "Rd", "St", "Road", "Street", "Circle"]

insert_query = """
INSERT INTO facilities (Name, Addressline1, Addressline2, code, DeleteFlag, Possible_Addressline1Match, Dest_child_Abbrevations_Match, Complete_Possible_Addressline1Match) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


def generate_delete_flag():
    """Randomly generate DeleteFlag (0 or 1)."""
    return random.choice([0, 1])


def generate_address():
    """Generate random Addressline1, Addressline2, and code."""
    address1 = fake.street_address()
    address2 = fake.secondary_address()
    code = fake.company_suffix()  # Example: "DNG Tower"
    return address1, address2, code


def generate_possible_addressmatch(addressline1):
    """Generate Possible_Addressline1Match (partial match from Addressline1)."""
    words = addressline1.split()
    return " ".join(words[: random.randint(1, len(words))])


def generate_dest_child_abbrev():
    """Randomly pick an abbreviation from the list."""
    return random.choice(address_abbreviations)


def generate_complete_addressmatch(possible_match, abbreviation):
    """Concatenate Possible_Addressline1Match and Dest_child_Abbrevations_Match."""
    return f"{possible_match} {abbreviation}"


for _ in tqdm(range(MAX_ROW_COUNT), desc="Generating Hospital Data"):
    # Generate hospital details
    name = fake.company()  # Hospital name
    addressline1, addressline2, code = generate_address()
    delete_flag = generate_delete_flag()
    possible_match = generate_possible_addressmatch(addressline1)
    abbreviation = generate_dest_child_abbrev()
    complete_match = generate_complete_addressmatch(possible_match, abbreviation)

    # Insert into database
    cursor.execute(
        insert_query,
        (
            name,
            addressline1,
            addressline2,
            code,
            delete_flag,
            possible_match,
            abbreviation,
            complete_match,
        ),
    )

connection.commit()
cursor.close()
connection.close()

print("Inserted 1000 rows into the hospitals table successfully.")
