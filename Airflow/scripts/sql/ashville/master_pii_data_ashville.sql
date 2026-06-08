SELECT
    patient_id, mrn, ext_birthsex, ext_sex, race_code, race_display, race_text,
    ethnicity_code, ethnicity_display, ethnicity_text, gender_identity_code,
    gender_identity_display, gender_identity_text, name_family, name_given_first,
    name_middle, name_text, gender, birthdate, deceased_boolean, deceased_datetime,
    marital_status_code, marital_status_display, marital_status_text,
    multiple_birth_boolean, multiple_birth_integer, telecom_system1, telecom_value1,
    telecom_use1, telecom_system2, telecom_value2, telecom_use2, telecom_system3,
    telecom_value3, telecom_use3, telecom_system4, telecom_value4, telecom_use4,
    address_use1, address_text1, address_line1, address_city1, address_state1,
    address_zip1, address_use2, address_text2, address_line2, address_city2,
    address_state2, address_zip2
FROM veradigm.patients
