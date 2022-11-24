import boto3

class Database:
    def __init__(self, env, target):
        self.cluster    = env.get('redshift').get('cluster')
        self.database   = env.get('redshift').get(target).get('database')
        self.schema     = env.get('redshift').get('schema')
        self.secretsArn = env.get('redshift').get('credentials')
        self.client     = boto3.client('redshift-data')

    def read_results(self,response):
        # Also 3 nested for loops for what could be alot of data
        # needs to be looked at.
        result = {'NextToken':''}
        while ('NextToken' in result):
            result = self.client.get_statement_result(Id=response['Id'], NextToken=result['NextToken'])
            print(result)
            for record in result['Records']:
                attrvalues = []
                for attr in record:
                    # Attr type: longValue (633472), Cannot see null so make it None, and only grab one value
                    attrvalues += [None if 'isNull' in attr else list(attr.values())[0]]
                values += [attrvalues]
        return values
    
    def send_query(self, query):
        response = self.client.execute_statement(
            WorkgroupName=self.cluster,
            Database=self.database,
            SecretArn=self.secretsArn,
            Sql=query,
            WithEvent=False,
        )

        status ='PICKED'
        while status in ['SUBMITTED','PICKED','STARTED']:
            state = self.client.describe_statement(Id=response.get('Id'))
            status = state['Status']
        if 'Error' in state: print(state['Error'])
        #return status, state

        values = []
        if state['ResultSize'] > 0:
            values = self.read_results(response)
        return values, state

    def deploy_base_schema(self, schema_ddl):
        queries = ""
        with open(schema_ddl) as f:
            queries += f.read()

        response = self.client.batch_execute_statement(
            WorkgroupName=self.cluster,
            Database=self.database,
            SecretArn=self.secretsArn,
            Sqls = [
                queries
            ],
            WithEvent=False,
        )

        status ='PICKED'
        while status in ['SUBMITTED','PICKED','STARTED']:
            state = self.client.describe_statement(response.get('Id'))
            status = state['Status']
        if 'Error' in state: print(state['Error'])
        return status

    def list_tables(self):
        query =f"""select t.table_name
                from information_schema.tables t
                where t.table_schema = '{self.schema}'
                and t.table_type = 'BASE TABLE'
                order by t.table_name;
                """
        return self.send_query(query)

    def backup_schema(self, bucket, iam):
        tables, _ = self.list_tables()
        queries = ""
        for table in tables:
            bucket_name = f's3://{bucket}/{table[0]}/'
            query = f"""unload ('select * from {self.schema}.{table[0]}')
                        to '{bucket_name}'
                        iam_role {iam}
                        ALLOWOVERWRITE;
                    """
            queries += query + '\n'
        result, _ = self.send_query(queries)

        # NOTE: the `result` will be empty (?) as it is an unload operation
        # maybe we return error if any (??)
        return result

    def restore_schema(self, bucket, iam):
        tables, _ = self.list_tables()
        queries = ""
        for table in tables:
            bucket_name = f's3://{bucket}/{table[0]}/'
            query = f"""copy {self.schema}.{table[0]}
                        from '{bucket_name}/' 
                        iam_role {iam};
                     """
            queries += query + "\n"
        result, _ = self.send_query(queries)
        
        # NOTE: the `result` will be empty (?) as it is a copy operation
        # maybe we return error if any (??)
        return result

    def exists(self, dbName):
        query='SELECT datname FROM pg_database;'
        databases, _ = self.send_query(query)
        
        # NOTE: the returned `databases` structure would be [['dev_chris'],['dev_rich'],['dev_ali'],...] 
        # so the below syntax might not find the db even if it is in the list
        
        # suggestion (i know it is not as clean): return True if dbName in [db[0] for db in databases] else False
        return True if dbName in databases else False

    def create_database(self, dbName):
        query = f"create database {dbName};"
        _, error = self.send_query(query)
        return not bool(error)

    def drop_database(self, dbName):
        query = f"drop database {dbName};"
        _, error = self.send_query(query)
        return not bool(error)
