import boto3
import logging

class Database:
    def __init__(self, env, target):
        self.cluster    = env.get('redshift').get('cluster')
        self.database   = env.get('redshift').get('database')
        self.schema     = env.get('redshift').get('schema')
        self.secretsArn = env.get('redshift').get('credentials')
        self.client     = boto3.client('redshift-data')

    def read_results(self,response):
        # Also 3 nested for loops for what could be alot of data
        # needs to be looked at.
        result = {'NextToken':''}
        values = []
        while ('NextToken' in result):
            result = self.client.get_statement_result(Id=response['Id'], NextToken=result['NextToken'])
            for record in result['Records']:
                attrvalues = []
                for attr in record:
                    # Attr type: longValue (633472), Cannot see null so make it None, and only grab one value
                    attrvalues += [None if 'isNull' in attr else list(attr.values())[0]]
                values += [attrvalues]
        return values
    
    def send_query(self, query, default="hello"):

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
        if 'Error' in state: logging.warn(state['Error'])

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
        if 'Error' in state: logging.warn(state['Error'])
        return status

    def list_tables(self):
        query =f"""select t.table_name
                from information_schema.tables t
                where t.table_schema = '{self.schema}'
                and t.table_type = 'BASE TABLE'
                order by t.table_name;
                """
        return self.send_query(query)

    def backup_data(self, bucket, iam):
        logging.info("backup_schema: start")
        tables, _ = self.list_tables()
        queries = ""
        logging.info("backup_schema: grabbed list of tables")
        for table in tables:
            bucket_name = f's3://{bucket}/{table[0]}/'
            query = f"""unload ('select * from {self.schema}.{table[0]}')
                        to '{bucket_name}'
                        iam_role '{iam}'
                        ALLOWOVERWRITE;
                    """
            queries += query + '\n'
        logging.info("backup_schema: generated query")
        _, error  = self.send_query(queries)
        logging.info("backup_schema: executed query")
        return not bool (error)

    def restore_data(self, bucket, iam):
        tables, _ = self.list_tables()
        queries = ""
        for table in tables:
            bucket_name = f's3://{bucket}/{table[0]}'
            query = f"""copy {self.schema}.{table[0]}
                        from '{bucket_name}/' 
                        iam_role '{iam}';
                     """
            queries += query + "\n"
        _, error = self.send_query(queries)
        return not bool (error)

    def exists(self, dbName):
        logging.info(f"calling exists({dbName})")
        query='SELECT datname FROM pg_database;'
        response, _ = self.send_query(query)
        logging.info(response)
        return True if dbName in [db[0] for db in response] else False

    def create_database(self, dbName):
        query = f"create database {dbName};"
        _, error = self.send_query(query)
        return not bool(error)

    def drop_database(self, dbName):
        query = f"drop database {dbName};"
        _, error = self.send_query(query)
        return not bool(error)
