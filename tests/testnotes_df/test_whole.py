import os
import django
import sys
# Set up Django environment
sys.path.append('/Users/karanchilwal/Documents/project/deidentification/deIdentification')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


from core.process_df.main import start_de_identification_for_table
from nd_api.views.db_views import _get_default_table_details_for_ui


table_details = _get_default_table_details_for_ui(columns_names=['encounterID', 'patientID', 'doctorID', 'date', 'startTime', 'endTime',
       'facilityID', 'reason', 'dateIn', 'dateOut', 'surgicalModifiedDate'])

table_details_users = _get_default_table_details_for_ui(columns_names=['uid', 'patient_name', 'uname', 'upwd', 'first_name', 'last_name', 'city', 'state', 'address', 'zipcode', 'dob', 'email', 'ssn', 'phone', 'sex', 'register_date', 'notes', 'UserType', 'medical_note'])


table_details['columns_details'][0]['is_phi'] = True
table_details['columns_details'][0]['de_identification_rule'] = 'ENCOUNTER_ID'

table_details['columns_details'][1]['is_phi'] = True
table_details['columns_details'][1]['de_identification_rule'] = 'PATIENT_ID'

table_details['columns_details'][6]['is_phi'] = True
table_details['columns_details'][6]['de_identification_rule'] = 'MASK'
table_details['columns_details'][6]['mask_value'] = 'facilityID'

#table_details['columns_details'][9]['is_phi'] = True
#table_details['columns_details'][9]['de_identification_rule'] = 'DATE_OFFSET'

table_details['columns_details'][10]['is_phi'] = True
table_details['columns_details'][10]['de_identification_rule'] = 'DATE_OFFSET'

table_details['columns_details'][3]['is_phi'] = True
table_details['columns_details'][3]['de_identification_rule'] = 'PATIENT_DOB'
#print(table_details['columns_details'][10])



table_details_users['columns_details'][0]['is_phi'] = True
table_details_users['columns_details'][0]['de_identification_rule'] = 'PATIENT_ID'


table_details_users['columns_details'][9]['is_phi'] = True
table_details_users['columns_details'][9]['de_identification_rule'] = 'ZIP_CODE'

table_details_users['columns_details'][8]['is_phi'] = True
table_details_users['columns_details'][8]['de_identification_rule'] = 'GENERIC_NOTES'

'''
table_details_users['reference_mapping'] =  {'conditions': [{'column_name': 'encounterID',
                                                        'source_column': 'uid',
                                                        'reference_table': 'enc_table'}],
                                                    'source_table': 'users',
                                                    'destination_column': 'patientID',
                                                    'destination_column_type': 'patient_id'}
'''

#table_details_users['columns_details'][11]['is_phi'] = True
#table_details_users['columns_details'][11]['de_identification_rule'] = 'NOTES'


#table_details_users['columns_details'][16]['is_phi'] = True
#table_details_users['columns_details'][16]['de_identification_rule'] = 'NOTES'


#table_details_users['columns_details'][18]['is_phi'] = True
#table_details_users['columns_details'][18]['de_identification_rule'] = 'NOTES'
#start_de_identification_for_table(1,100000,0, table_details)
#print(table_details_users['columns_details'][18])



start_de_identification_for_table(2,100000, 0, table_details_users)
