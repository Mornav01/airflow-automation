SELECT
    e.patientID as patientid
    e.encounterID as encounter_id, 
    e.date as encounter_date
    FROM enc e
    ORDER BY e.date, e.encounterID;