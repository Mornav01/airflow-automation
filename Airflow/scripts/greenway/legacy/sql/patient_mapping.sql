SELECT 
    p.PatientID AS patientid,
    acc.AcctOpenDate AS registration_date
FROM Patient AS p
LEFT JOIN br_Account AS acc
    ON p.PatientID = acc.PatientID
ORDER BY 
    p.PatientID;