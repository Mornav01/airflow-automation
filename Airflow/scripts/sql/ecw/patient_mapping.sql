    SELECT 
    u.uid as patientid,
    u.cdate as registration_date
    FROM users u
    WHERE u.UserType = 3
    ORDER BY u.cdate, u.uid