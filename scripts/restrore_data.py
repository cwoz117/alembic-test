import boto3


client = boto3.client('redshift-data')

response = client.execute_statement(
    Database='dev_chris',
    SecretArn='arn:aws:secretsmanager:us-east-1:135143936609:secret:test/redshift/password-LjFt2k',

    Sql="""
        copy myschema.tablea
        from 's3://tc-backup-bucket/tablea/' 
        iam_role 'arn:aws:iam::135143936609:role/service-role/AmazonRedshift-CommandsAccessRole-20221115T143141';

        copy myschema.tableb
        from 's3://tc-backup-bucket/tableb/' 
        iam_role 'arn:aws:iam::135143936609:role/service-role/AmazonRedshift-CommandsAccessRole-20221115T143141';

        copy myschema.tablec
        from 's3://tc-backup-bucket/tablec/' 
        iam_role 'arn:aws:iam::135143936609:role/service-role/AmazonRedshift-CommandsAccessRole-20221115T143141';

        """,
    StatementName='unload',
    WithEvent=False,
    WorkgroupName='tc-workgroup'
)


# cheking query status
status ='PICKED'        
while status in ['SUBMITTED','PICKED','STARTED']:
    state = client.describe_statement(Id=response['Id'])
    status = state['Status']        
print(f"query status: {status}")

error = None
if 'Error' in state:
    error = state['Error']
    print(error)
