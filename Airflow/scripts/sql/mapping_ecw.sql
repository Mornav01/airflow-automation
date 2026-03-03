SELECT 
            u.uid as patient_id, 
            e.date as encounter_date,
            e.encounterID as encounter_id, 
            u.cdate as registration_date
        FROM users u
        LEFT JOIN enc e ON u.uid = e.patientID
        WHERE u.UserType = 3
        ORDER BY u.uid, e.date, e.encounterID;