import toml, logging, os, boto3

def run_redshift(env: dict, dbname:str, query:str) -> list:
    client = boto3.client('redshift-data')
    response = client.execute_statement(
        WorkgroupName = env.get('names').get('cluster'),
        Database = dbname,
        SecretArn = env.get('arn').get('database_credentials'),
        Sql = query,
        WithEvent = False)

    # cheking query status
    status ='PICKED'
    while status in ['SUBMITTED','PICKED','STARTED']:
        state = client.describe_statement(Id=response['Id'])
        status = state.get('Status')
    logging.info(f"query status: {status}")

    error = None
    if 'Error' in state:
        error = state['Error']
        print(error)
    logging.info(f"query result size: {state['ResultSize']}")
    
    # Output results
    values = []
    if state.get('ResultSize') > 0:
        result = {'NextToken':''}
        while 'NextToken' in result:
            result = client.get_statement_result(Id=response['Id'], 
                                                 NextToken=result['NextToken'])
            for record in result['Records']:
                attrvalues = []
                for attr in record:
                    # need to recognize if the value is null or not
                    # format for attr is `{'longValue': 633472}` so 
                    # there is only one value in values()
                    attrvalues += [None if 'isNull' in attr else list(attr.values())[0]]
                values += [attrvalues]
    return values, error

def check_db_exists(env:dict, dbname:str):
    q = 'SELECT datname FROM pg_database;'
    dbs, _= run_redshift(env, dbname, q)
   
    return dbname in [db[0] for db in dbs]

def create_database(env:dict, dbname:str):
    q = f"create database {dbname};"
    _, error = run_redshift(env,dbname, q)
    return not bool(error)

def drop_database(env:dict, dbname:str):
    q = f"drop database {dbname};"
    _, error = run_redshift(env, dbname, q)
    return not bool(error)

def backup_schema(database, schema, bucket, iamArn):
    queries = ""
    tables = list_tables(database, schema);
    for table in tables:
        query = f"""unload ('select * from {schema}.{table}'
                    to 's3://{bucket}/{table}/'
                    iam_role {iam}
                    ALLOWOVERWRITE);
                 """
        queries += '\n' + query
    run_redshift(env, dbname, queries)

def run_copies(env, tablelist, dbname):
    copy_table_qrys = []
    for tablename in tablelist:
        copy_query = f"""copy myschema.{tablename[0]}
                         from 's3://{env.get('names').get('backup_bucket')}/{tablename[0]}/'
                         iam_role {env.get('arn').get('iam_role')};
        """
        copy_table_qrys.append(copy_query)
    copy_table_qrys = '\n'.join(copy_table_qrys)
        
    logging.info('running copy queries...')
    run_redshift(env, dbname, copy_table_qrys)
    logging.info('Done.')
