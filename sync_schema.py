import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import dataikuapi
from dataikuapi import SyncRecipeCreator

def delete_dataset(project_key, dataset_name):
    client = dataiku.api_client()
    prj=client.get_project(project_key)    
    ds=prj.get_dataset(dataset_name)
    ds.delete()
    
def create_parquet_dataset( project_key, dataset_name, connection_name, hive_db = 'default', hive_tbl = 'default', dataset_path = 'default' ):
    client = dataiku.api_client()
    prj=client.get_project(project_key)    
    ds_formatparams = {
                'parquetBlockSizeMB': 128,
                'parquetCompressionMethod': 'SNAPPY',
                'parquetFlavor': 'HIVE',
                'parquetLowerCaseIdentifiers': False,
                'representsNullFields': False
    }
    if hive_db == 'default':
        # We use the default settings, the database is stored in ${hive_db_<CONN>}    
        meta_sync = True
        hive_database = '${hive_db_'+connection_name+'}'
        if hive_tbl == 'default':
            hive_table_name = '${projectKey}_'+dataset_name
        else:
            hive_table_name = hive_tbl            
    if hive_db is None:
        hive_database = None
        hive_table_name = None
        meta_sync = False
    if dataset_path == 'default':
        path = '/${projectKey}/'+dataset_name
    else:
        path = dataset_path
    ds_params = {
        u'path': path,        
        u'connection': connection_name,
        u'notReadyIfEmpty': False,
        u'metastoreSynchronizationEnabled' : meta_sync,
        u'hiveDatabase': hive_database,
        u'hiveTableName': hive_table_name,        
        u'filesSelectionRules': {
            u'excludeRules': [],
            u'explicitFiles': [],
            u'mode': u'ALL',
            u'includeRules': []
        },
        u'timeout': 10000
    }
    return prj.create_dataset(dataset_name, 'HDFS', params=ds_params, formatType='parquet', formatParams=ds_formatparams) 

def create_sync_recipe_from_dataset( project_key, recipe_name, inp_dataset_name, out_dataset_name, connection  ):
    client = dataiku.api_client()
    prj=dataikuapi.dss.project.DSSProject(client,project_key)    
    #r = SyncRecipeCreator(recipe_name, prj).with_input(inp_dataset_name).\
    r = dataikuapi.dss.recipe.SingleOutputRecipeCreator( 'shaker', recipe_name, prj ).with_input(inp_dataset_name).\
    with_new_output(out_dataset_name, connection, format_option_id='PARQUET_HIVE').build()
    
def add_dataset_to_scenario( dataset_name, scenario, max_build_items = 15 ):
    sc_def = scenario.get_definition(with_status=False)
    found = False
    print('> add %s into %s' % (dataset_name,sc_def['name']))
    for step in sc_def['params']['steps']:
        if 'builds' in step['params'].keys():
            builds = step['params']['builds']
            for b in builds:
                if b['itemId'] == dataset_name:
                    found = True
    if not found:
        cnt=0
        for step in sc_def['params']['steps']:
            if 'builds' not in step.get('params',{}):
                continue
            print(step['name'],len(step['params']['builds']))
            if len(step['params']['builds']) < max_build_items:
                print(' > adding new build into step...')
                step['params']['builds'].append({'itemId':dataset_name,'type':'DATASET','partitionsSpec':''}) 
                scenario.set_definition(sc_def, with_status=False)
                return sc_def
            cnt+=1
        print(' > adding new step...')
        step_id = 'build_step_'+str(cnt+1)
        step_name = 'Step #'+str(cnt+1)
        step_params = {u'builds':[{u'itemId':dataset_name, u'type':'DATASET', u'partitionsSpec':''}],
                        u'jobType': 'RECURSIVE_BUILD',
                        u'proceedOnFailure': False,
                         u'refreshHiveMetastore': True}
        print(' > adding these step params',step_params)
        sc_def['params']['steps'].append({u'id':step_id, 
                                            u'name':step_name, 
                                            u'params':step_params,
                                            u'resetScenarioStatus': False,
                                            u'runConditionExpression': u"outcome=='SUCCESS' && cluster_open",
                                            u'runConditionStatuses': [u'SUCCESS', u'WARNING'],
                                            u'runConditionType': u'RUN_CONDITIONALLY',
                                            u'type': u'build_flowitem'                                         
                                         })
        scenario.set_definition(sc_def, with_status=False)
        return sc_def

# MAIN
# ----
# Connect to Dataiku DEV
client = dataiku.api_client()

# Connect to Dataiku PROD
# https://10.85.54.80
url = client.get_variables().get('prod_dss_url')
# HkG6tTIjBaQHC3MKWvJSE8Yb4QXnb6Yr
api_key = client.get_variables().get('prod_dss_api_key')
remote_client = dataikuapi.DSSClient(url, api_key)
remote_client._session.verify = False

# Fetch all the datasets that have already been imported to DEV into imported_datasets
import_prj_key = 'ANALYTICS_PROD'
import_prj = client.get_project(import_prj_key)

# Target build scenario in Dev (to be updated accordingly)
scenario = import_prj.get_scenario('ANALYTICS_PROD_BUILD_ALL')

# Datasets already synced to DEV
imported_datasets = {}
for d in import_prj.list_datasets():
    imported_datasets[d['name']] = import_prj.get_dataset(d['name'])

 # List large volume datasets to be ignored as they are syned at HDFS level, and not at recipe level
tables = import_prj.get_variables()['local'].get('large_prod_tables',[])
assert len(tables) > 0 and type(tables) == list, 'Local variables does not contain "large_prod_tables" item or is empty'

# Fetch available datasets in PROD
remote_admin = remote_client.get_project('ADMIN')
datasets = remote_admin.get_dataset('dss_datasets')

# Copy of Prod dataset list on DEV (along with their prepared prod_ counterpart)
remote_set = {}
for d in datasets.iter_rows():
    remote_set["prod_" + d[3]] = d[13]
    remote_set[d[3]] = d[13]

# New/Changed schema capture
changes = []
new=[]
nochange = []

# Identify created/Modified datasets in PROD, ignoring large tables
# Identify schema change or new dataset creation
# Create/Modify datasets in DEV
# Create new sync recipies in DEV for newly added datasets
for d in datasets.iter_rows():
    if d[0] == 'analytics' and d[13] != 'ANALYTICS_POND' and d[3] not in tables:
        print('Checking dataset for import %s.%s' % (d[13],d[3]))
        ds_name = d[3]
        remote_ds=remote_client.get_project(d[13]).get_dataset(ds_name)
        remote_schema = remote_ds.get_definition()['schema']
        remote_format_params = remote_ds.get_definition()['formatParams']
        remote_format_type = remote_ds.get_definition()['formatType']        
        if ds_name in imported_datasets:
            ds = import_prj.get_dataset(ds_name)
            schema = ds.get_definition()['schema']
            format_params = ds.get_definition()['formatParams']
            format_type = ds.get_definition()['formatType']            
            if remote_schema != schema:
                print('  > changing schema definition for the dataset '+ds_name)
                print(remote_schema)
                print(schema)
                changes.append(ds_name)
                ds_def = ds.get_definition()
                ds_def['schema'] = remote_schema
                ds_def['formatParams'] = remote_format_params
                ds_def['formatType'] = remote_format_type
                ds.set_definition(ds_def)
                ds = import_prj.get_dataset('prod_'+ds_name) 
                ds_def = ds.get_definition()
                ds_def['schema'] = remote_schema
                ds_def['formatParams'] = remote_format_params
                ds_def['formatType'] = remote_format_type
                ds.set_definition(ds_def)
            else:
                nochange.append(ds_name)
        else:
            print('  > creating new dataset '+ds_name)            
            new_ds = create_parquet_dataset('ANALYTICS_PROD',ds_name,'prod_analytics_pond',hive_db=None,dataset_path=d[12])
            print('  > configuring new dataset')
            new_ds_def = new_ds.get_definition() 
            new_ds_def['schema'] = remote_schema
            new_ds_def['formatParams'] = remote_format_params
            new_ds_def['formatType'] = remote_format_type
            new_ds.set_definition(new_ds_def)
            print('  > creating new recipe with new output') 
            try:
                create_sync_recipe_from_dataset(import_prj_key,'sync_'+ds_name, ds_name, 'prod_'+ds_name, 'analytics')
                new.append(ds_name)
            except:
                print(' > ! failed, removing '+ds_name)
                delete_dataset(import_prj_key,ds_name)

# List of dataset names available in Dev
imported_ds_keys = imported_datasets.keys()

# List of dataset names available in Prod
prod_ds_keys = remote_set.keys()

# Datasets exposed to other projects, these should be ignored from dropping
exposed_ds = []
for ds in import_prj.get_settings().get_raw()["exposedObjects"]["objects"]:
    exposed_ds.append(ds[u'localName'])

prd_ds_list = {""} # Set of datasets as of now on production
dev_ds_list = {""} # Set of datasets as of now on development
exp_ds_list = {""} # Set of datasets exposed from development
drop_ds_list = []  # List of datastes to be dropped from development

# List of datasets no longer present in Prd
# List of datasets no longer present in Prd into drop_ds_list, ignoring exposed datasets
dev_ds_list.update(imported_ds_keys)
prd_ds_list.update(prod_ds_keys)
exp_ds_list.update(exposed_ds)
for item in dev_ds_list:
    if item not in prd_ds_list: 
        if item in exp_ds_list:
            continue
        else:
            drop_ds_list.append(item) 

# Drop Dataset
for item in drop_ds_list:
    delete_dataset(import_prj_key, item)

