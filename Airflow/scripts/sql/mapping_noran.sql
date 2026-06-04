SELECT
    pp.PatientId        AS patientid,
    pp.PatientProfileId AS profileid,
    pp.PId              AS pid,
    pv.PatientVisitId   AS encounter_id,
    pv.Created          AS encounter_date,
    pp.Created          AS registration_date
FROM PatientProfile pp
LEFT JOIN PatientVisit pv
    ON pp.PatientProfileId = pv.PatientProfileId
