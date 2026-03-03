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

from core.process.rules.unstruct.xml_deidentify import XMLDeidentifier

notes_text = """
<?xml version="1.0" encoding="UTF-8"?>
<Person>
    <FirstName>John</FirstName>
    <LastName>Doe</LastName>
    <DateOfBirth>1985-05-15</DateOfBirth>
    <SocialSecurityNumber>123-45-6789</SocialSecurityNumber>
    <Email>john.doe@example.com</Email>
    <PhoneNumber>+1-555-123-4567</PhoneNumber>
    <ptDOB>15-08-2000</ptDOB>
    <StartedAt>15-08-2000</StartedAt>
    <Address>
        <Street>123 Main St</Street>
        <City>Anytown</City>
        <State>CA</State>
        <PostalCode>90210</PostalCode>
        <Country>USA</Country>
    </Address>
    <Employment>
        <EmployerName>ABC Corporation</EmployerName>
        <JobTitle>Software Engineer</JobTitle>
        <EmployeeID>7890</EmployeeID>
    </Employment>
</Person>"""
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
xml_deidentifier = XMLDeidentifier(xml_config)
notes_text = xml_deidentifier.deidentify(notes_text)
print(notes_text)
