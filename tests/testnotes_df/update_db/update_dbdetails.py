import os
import django
import sys
# Set up Django environment
sys.path.append('/Users/karanchilwal/Documents/project/deidentification/deIdentification/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


from nd_api.models import DbDetailsModel, TableDetailsModel


pii_config = {}



def update_model():
    obj, _ = DbDetailsModel.objects.get_or_create(db_name="postgres_test")
    #obj.pii_config = {...}  # Update this config
    print(obj.run_config.keys())
    print(obj.run_config['pii_db_config'].keys())
    obj.source_db_config['connection_str'] = 'mysql+pymysql://root:Neuro%40123@localhost:3306/ndsource'
    obj.destination_db_config['connection_str'] = 'mysql+pymysql://root:Neuro%40123@localhost:3306/nddestination'
    obj.save()
    '''  
    obj.run_config['pii_config'] = {}
    obj.run_config['pii_db_config'] = {}
    
    #print(obj.run_config['mapping_db_config'])
    obj.run_config['mapping_db_config'] = {}
    
    obj.run_config['mapping_db_config']['connection_str'] = 'mysql+pymysql://root:Neuro%40123@localhost:3306/masterdb'
    obj.run_config['pii_db_config']['master_connection_str'] = 'mysql+pymysql://root:Neuro%40123@localhost:3306/masterdb'
    obj.run_config['pii_db_config']['secondary_pii_connection_str'] = 'mysql+pymysql://root:Neuro%40123@localhost:3306/masterdb'
    #obj.run_config['pii_db_config'] = {}
    obj.run_config['secondary_pii_config'] = []
    obj.save()
    '''
    
def update_table_model():
    _db, _ = DbDetailsModel.objects.get_or_create(db_name="postgres_test")
    t, _ = TableDetailsModel.objects.get_or_create(table_name='users', db=_db )
    print(t.table_name, t.id)



if __name__ == "__main__":
    update_model()
    #update_table_model()