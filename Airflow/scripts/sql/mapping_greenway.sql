SELECT 
    p.PatientID AS patient_id,
    v.CreateDate AS encounter_date,
    v.VisitID AS encounter_id,
    acc.AcctOpenDate AS registration_date
FROM Patient AS p
LEFT JOIN Visit AS v
    ON p.PatientID = v.PatientID
LEFT JOIN br_Account AS acc
    ON p.PatientID = acc.PatientID
ORDER BY 
    p.PatientID,
    v.CreateDate,
    v.VisitID;