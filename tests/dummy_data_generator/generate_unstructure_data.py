from faker import Faker
from tqdm import tqdm
import random
import string
import csv
from tqdm import tqdm

ROWS_GLOBAL = 50000
ROWS_UNSTRUCT = 100000
COLUMNS = 25
fake = Faker()
data = []
all_global_values = []
global_rows = []
columns = [f"col{i}" for i in range(1, COLUMNS+1)]

def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_number(min_val=1000, max_val=99999):
    return random.randint(min_val, max_val)

def save_to_csv(data, filename="output.csv"):
    """Saves a list of dictionaries to a CSV file."""
    if not data:
        print("No data to write!")
        return

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()  # Write column names
        writer.writerows(data)  # Write rows

def get_random_row():
    ncols = len(columns)
    fake_generators = [
        fake.name,
        fake.email,
        fake.address,
        fake.city,
        fake.state,
        fake.zipcode,
        fake.ssn,
        fake.phone_number,
        fake.date_this_year,
        random_number,
        random_string
    ]
    selected_generators = (fake_generators * ((ncols // len(fake_generators)) + 1))[:ncols]
    values = [gen() for gen in selected_generators]

    dicto = {}
    for col, value in zip(columns, values):
        dicto[col] = value
    return dicto, values

def get_random_values(all_global_values, count=50):
    """Selects `count` random values from the given list."""
    values = random.sample(all_global_values, min(count, len(all_global_values)))
    values = [str(value) for value in values]
    return " ".join(values)

for i in tqdm(range(ROWS_GLOBAL), "progress global"):
    row, values = get_random_row()
    all_global_values.extend(values)
    global_rows.append(row)

global_data_file = "/Users/rohit.chouhan/NEDI/CODE/Dump/Project/deidentification/tests/data/global.csv"
save_to_csv(global_rows, global_data_file)

for i in tqdm(range(ROWS_UNSTRUCT), "progress unstructu"):
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
    notes = fake.text(max_nb_chars=500)

    name_variations = [patient_name, first_name, last_name]
    selected_name = random.choice(name_variations)

    appointment_date = fake.date_this_year()
    follow_up_date = fake.date_between(start_date=appointment_date, end_date='+30d')


    rows = [{col: (random_string() if i % 2 == 0 else random_number()) for i, col in enumerate(columns)} for _ in range(50000)]

    random_50_value = get_random_values(all_global_values, 50)
    random_str = " ".join(random_50_value)
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
            f"Random String: {random_str}\n"
        )
    
    data.append({"note": medical_note})

unstruct_data_file = "/Users/rohit.chouhan/NEDI/CODE/Dump/Project/deidentification/tests/data/unstruct.csv"
save_to_csv(data, unstruct_data_file)
