SELECT
    pv.PatientProfileId AS profileid,
    pv.PatientVisitId   AS encounter_id,
    pv.Created          AS encounter_date
FROM PatientVisit pv;