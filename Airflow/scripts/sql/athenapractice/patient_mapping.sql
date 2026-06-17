    SELECT
    pp.PatientId        AS patientid,
    pp.PatientProfileId AS profileid,
    pp.PId              AS pid,
    pp.Created          AS registration_date
FROM PatientProfile pp
ORDER BY pp.Created;