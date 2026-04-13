select a.PatientID as patient_id, 
b.InsID as InsuranceCoverage_InsID, b.PolicyName as InsuranceCoverage_PolicyName, b.EmployerName as InsuranceCoverage_EmployerName, 
b.PolicyGroupNumber as InsuranceCoverage_PolicyGroupNumber, b.PolicyNumber as InsuranceCoverage_PolicyNumber,
b.PolicyHolder as InsuranceCoverage_PolicyHolder,
c.PlanNumber as InsurancePlan_PlanNumber, c.PlanName as InsurancePlan_PlanName, c.ClaimPayorID as InsurancePlan_ClaimPayorID, 
d.ProviderName as InsuranceProviders_ProviderName, d.ElectronicPayerIDNumber as InsuranceProviders_ElectronicPayerIDNumber, 
d.ClaimPayorID as InsuranceProviders_ClaimPayorID, d.EligibilityProviderFName as InsuranceProviders_EligibilityProviderFName,
d.EligibilityProviderMName as InsuranceProviders_EligibilityProviderMName, d.EligibilityProviderLName as InsuranceProviders_EligibilityProviderLName, d.EligibilityProviderIDNumber as InsuranceProviders_EligibilityProviderIDNumber
from Patient a 
INNER JOIN InsuranceCoverage b on a.PersonID=b.PersonID
LEFT JOIN InsurancePlan c on b.PlanID=c.PlanID
LEFT JOIN InsuranceProviders d on c.PlanProviderID = d.InsuranceProviderID;