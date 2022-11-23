import boto3

source_bucket = 'tc-backup-bucket'
schema_folder = ''
dest_schema = 'myschema'
iam_role = 'arn:aws:iam::135143936609:role/service-role/AmazonRedshift-CommandsAccessRole-20221115T143141'

database = 'tc-test-database'
secret_arn='arn:aws:secretsmanager:us-east-1:135143936609:secret:test/redshift/password-LjFt2k'
workgroup = 'tc-workgroup'

redshift_client = boto3.client('redshift-data')
s3_client = boto3.client('s3');

s3_response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=schema_folder)
# if (s3_response['ResponseMetadata']['HTTPStatusCode'] != 200):
#   error

# Get the individual table names.
folders = [];
for file in s3_response['Contents']:
  folder = file['Key'].split('/')
  folders.append(folder[0])
unique_folders = list(set(folders))

# Generate queries to copy data to redshift db/schema.
queries = []
for table in unique_folders:
  query = "copy " + dest_schema + "." + table + " from 's3://" + source_bucket + "/" + table + "/' iam_role '" + iam_role + "'"
  queries.append(query)

print(queries)

# Execute table copy commands.
copy_response = redshift_client.batch_execute_statement(
  WorkgroupName=workgroup,
  Database=database,
  SecretArn=secret_arn,
  Sqls=queries,
  StatementName='copy-db-data',
  WithEvent=False
)

# checking query status
status ='PICKED'        
while status in ['SUBMITTED','PICKED','STARTED']:
    state = redshift_client.describe_statement(Id=copy_response['Id'])
    status = state['Status']        
print(f"query status: {status}")

error = None
if 'Error' in state:
    error = state['Error']
    print(error)
