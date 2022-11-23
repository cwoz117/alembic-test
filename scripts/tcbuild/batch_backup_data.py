import boto3

import logging

def run_redshift(env: dict, dbname:str, query:str) -> list:
    client = boto3.client('redshift-data', region_name='us-east-1')

    response = client.execute_statement(
        WorkgroupName='tc-workgroup',
        Database=dbname,
        SecretArn=env['arn']['database_credentials'],
        Sql=query,
        StatementName='tc-test-deploy',
        WithEvent=False,
    )

    # cheking query status
    status ='PICKED'        
    while status in ['SUBMITTED','PICKED','STARTED']:
        state = client.describe_statement(Id=response['Id'])
        status = state['Status']        
    logging.warning(f"query status: {status}")

    error = None
    if 'Error' in state:
        error = state['Error']
        print(error)

    logging.warning(f"query result size: {state['ResultSize']}")
        
    values = []        
    if state['ResultSize']>0:
        result = {'NextToken':''}
        while ('NextToken' in result):            
            result = client.get_statement_result(Id=response['Id'], NextToken=result['NextToken'])
            for record in result['Records']:
                attrvalues = []
                for attr in record:
                    # need to recognize if the value is null or not
                    # format for attr is `{'longValue': 633472}` so 
                    # there is only one value in values()
                    attrvalues += [None if 'isNull' in attr else list(attr.values())[0]]
                values += [attrvalues]
    
    return values, error

def run_unloads(env,tablelist,dbname):
    # making the unload quries
    unload_table_qrys = []
    for tablename in tablelist:
        bucket_name = f's3://tc-backup-bucket/{tablename[0]}/'
        unload_query = f"""unload ('select * from myschema.{tablename[0]}')
                           to '{bucket_name}'
                           iam_role {env['arn']['iam_role']}
                           ALLOWOVERWRITE;
                        """
        unload_table_qrys.append(unload_query)
    
    unload_table_qrys = '\n'.join(unload_table_qrys)
        
    # running the unload queries    
    logging.warning('running unload queries...')
    run_redshift(env=env,dbname=dbname, query=unload_table_qrys)
    logging.warning('Done.')

def run_copies(env,tablelist,dbname,):
    # making the copy quries
    copy_table_qrys = []
    for tablename in tablelist:
        bucket_name = f's3://tc-backup-bucket/{tablename[0]}/'
        copy_query = f"""copy myschema.{tablename[0]}
                           from 's3://tc-backup-bucket/{tablename[0]}/' 
                           iam_role {env['arn']['iam_role']};
                        """
        copy_table_qrys.append(copy_query)
    
    copy_table_qrys = '\n'.join(copy_table_qrys)
        
    # running the unload queries    
    logging.warning('running copy queries...')
    run_redshift(env=env,dbname=dbname, query=copy_table_qrys)
    logging.warning('Done.')

# if __name__ == '__main__':

#     import toml, os
#     env = toml.load(f"{os.getcwd()}\scripts\env.toml")
#     dbname = 'dev_ali'

#     # getting a list of tables for the schema
#     schemaname = 'myschema'
#     listtables_qry =f"""select t.table_name
#                     from information_schema.tables t
#                     where t.table_schema = '{schemaname}'
#                     and t.table_type = 'BASE TABLE'
#                     order by t.table_name;"""
#     tablelist, _ = run_redshift(env,dbname=dbname, query=listtables_qry)

#     run_unloads(tablelist)
#     run_copies(tablelist)
    





