{
    "mask": {
        "Patient_AltPatientID": {
            "regex": None,
            "masking_value": "((PATIENTID_IDENTIFIER))",
            "processing_func": None,
        },
        "Patient_OrgPatientID": {
            "regex": None,
            "masking_value": "((PATIENTID_IDENTIFIER))",
            "processing_func": None,
        },
        "Person_FirstName": {
            "regex": None,
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
        "Person_MiddleName1": {
            "regex": None,
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
        "Person_MiddleName2": {
            "regex": None,
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
        "Person_LastName": {
            "regex": None,
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
        "Person_NickName": {
            "regex": None,
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
        "Person_MaidenName": {
            "regex": None,
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
        "Person_DriverLicNum": {
            "regex": None,
            "masking_value": "((DRIVERS_LICENSE))",
            "processing_func": None,
        },
        "Person_Pager": {
            "regex": None,
            "masking_value": "((PAGER_NO))",
            "processing_func": None,
        },
        "Person_CellPhone1": {
            "regex": None,
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "Person_CellPhone2": {
            "regex": None,
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "Person_Occupation": {
            "regex": None,
            "masking_value": "((OCCUPATION))",
            "processing_func": None,
        },
        "Person_PrimaryPhone": {
            "regex": None,
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "Person_PrimaryWorkPhone": {
            "regex": None,
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "RESIDENTIAL_AddressLine1": {
            "regex": None,
            "masking_value": "((RESIDENTIAL_PATIENT_ADDRESS))",
            "processing_func": None,
        },
        "RESIDENTIAL_AddressLine2": {
            "regex": None,
            "masking_value": "((RESIDENTIAL_PATIENT_ADDRESS))",
            "processing_func": None,
        },
        "RESIDENTIAL_City": {
            "regex": None,
            "masking_value": "((RESIDENTIAL_PATIENT_CITY))",
            "processing_func": None,
        },
        "RESIDENTIAL_County": {
            "regex": None,
            "masking_value": "((RESIDENTIAL_PATIENT_COUNTY))",
            "processing_func": None,
        },
        "RESIDENTIAL_FaxNumber": {
            "regex": None,
            "masking_value": "((RESIDENTIAL_FAX))",
            "processing_func": None,
        },
        "PersonEmployers_PersonalPhoneNumber": {
            "regex": None,
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "Employers_Name": {
            "regex": None,
            "masking_value": "((EMPLOYER_NAME))",
            "processing_func": None,
        },
        "Employers_Address1": {
            "regex": None,
            "masking_value": "((EMPLOYER_ADDRESS))",
            "processing_func": None,
        },
        "Employers_Address2": {
            "regex": None,
            "masking_value": "((EMPLOYER_ADDRESS))",
            "processing_func": None,
        },
        "Employers_City": {
            "regex": None,
            "masking_value": "((EMPLOYER_CITY))",
            "processing_func": None,
        },
        "Employers_County": {
            "regex": None,
            "masking_value": "((EMPLOYER_COUNTY))",
            "processing_func": None,
        },
        "Employers_Fax": {
            "regex": None,
            "masking_value": "((EMPLOYER_FAX))",
            "processing_func": None,
        },
        "Employers_ContactName": {
            "regex": None,
            "masking_value": "((EMPLOYER_DETAILS))",
            "processing_func": None,
        },
        "BILLING_AddressLine1": {
            "regex": None,
            "masking_value": "((BILLING_PATIENT_ADDRESS))",
            "processing_func": None,
        },
        "BILLING_AddressLine2": {
            "regex": None,
            "masking_value": "((BILLING_PATIENT_ADDRESS))",
            "processing_func": None,
        },
        "BILLING_City": {
            "regex": None,
            "masking_value": "((BILLING_PATIENT_CITY))",
            "processing_func": None,
        },
        "BILLING_County": {
            "regex": None,
            "masking_value": "((BILLING_PATIENT_COUNTY))",
            "processing_func": None,
        },
        "BILLING_FaxNumber": {
            "regex": None,
            "masking_value": "((BILLING_FAX))",
            "processing_func": None,
        },
        "RESIDENTIAL_PostalCode": {
            "regex": None,
            "masking_value": "((ZIP_CODE))",
            "processing_func": None,
        },
        "BILLING_PostalCode": {
            "regex": None,
            "masking_value": "((ZIP_CODE))",
            "processing_func": None,
        },
    },
    "dob": {
        "Person_DateOfBirth": {
            "regex": None,
            "masking_value": "((PATIENT_DOB))",
            "processing_func": None,
        }
    },
    "regex": {
        "Person_EMail1": {
            "regex": ["\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b"],
            "masking_value": "((EMAIL_ID))",
            "processing_func": None,
        },
        "Person_EMail2": {
            "regex": ["\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b"],
            "masking_value": "((EMAIL_ID))",
            "processing_func": None,
        },
        "Person_SSN": {
            "regex": ["\\b\\d{3}[- ]\\d{2}[- ]\\d{4}\\b"],
            "masking_value": "((SSN))",
            "processing_func": None,
        },
        "Person_CellPhone1": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "Person_CellPhone2": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "Person_PrimaryPhone": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "Person_PrimaryWorkPhone": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "RESIDENTIAL_PhoneNum1": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((RESIDENTIAL_PHONE_NUMBER))",
            "processing_func": None,
        },
        "RESIDENTIAL_PhoneNum2": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((RESIDENTIAL_PHONE_NUMBER))",
            "processing_func": None,
        },
        "BILLING_PhoneNum1": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((BILLING_PHONE_NUMBER))",
            "processing_func": None,
        },
        "BILLING_PhoneNum2": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((BILLING_PHONE_NUMBER))",
            "processing_func": None,
        },
        "Employers_Phone": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((EMPLOYER_PHONE))",
            "processing_func": None,
        },
    },
    "combine": {
        "patientfullname": {
            "regex": None,
            "combine": [
                "Person_FirstName",
                "Person_MiddleName1",
                "Person_MiddleName2",
                "Person_LastName",
                "Person_NickName",
                "Person_MaidenName",
            ],
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
    },
    "replace_value": [
        {
            "new_value": "((FACILITYNAME))",
            "old_value": "Josephson-Wallack-Munshower",
        },
        {"new_value": "((FACILITYNAME))", "old_value": "JWM Neurology"},
        {"new_value": "((FACILITYNAME))", "old_value": "JWM"},
        {"new_value": '((FACILITYNAME))', "old_value": "Josephson Wallack Munshower"}
    ],
}
