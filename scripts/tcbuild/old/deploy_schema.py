import os
import sys
import boto3

def run_redshift_query(query : str) -> str:
    client = boto3.client('redshift-data', region_name='us-east-1')
    queries = ""

    with open(sys.argv[1]) as f:
        queries += f.read()

    response = client.batch_execute_statement(
        WorkgroupName='tc-workgroup',
        Database='dev_ali',
        #DbUser='admin',
        SecretArn='arn:aws:secretsmanager:us-east-1:135143936609:secret:test/redshift/password-LjFt2k',
        Sqls=[
            queries
        ],
        StatementName='tc-test-deploy',
        WithEvent=False,
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

    return status, error

if __name__ == "__main__":
   run_redshift_query("") 
