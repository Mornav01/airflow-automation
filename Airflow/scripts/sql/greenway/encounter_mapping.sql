SELECT 
    v.VisitID AS encounter_id,
    p.PatientID AS patientid,
    v.CreateDate AS encounter_date
FROM Patient AS p
LEFT JOIN Visit AS v
    ON p.PatientID = v.PatientID
LEFT JOIN br_Account AS acc
    ON p.PatientID = acc.PatientID
ORDER BY 
    p.PatientID,
    v.CreateDate,
    v.VisitID;