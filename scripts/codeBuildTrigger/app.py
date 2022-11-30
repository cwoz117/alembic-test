import boto3

client = boto3.client('codebuild', region_name="us-west-1")

def lambda_handler(event, context):
    #client.post_comment_for_pull_request(
    #    pullRequestId=event['detail']['pullRequestId'],
    #    repositoryName=event['detail']['repositoryNames'][0],
    #    beforeCommitId=event['detail']['sourceCommit'],
    #    afterCommitId=event['detail']['destinationCommit'],
    #    content='A Pull Request was raised or changed. QA is runnig now and the results will appear here'
    #)
    #     Start the build process
    client.start_build(
        projectName="autopipe-db-migration-smoke-test",
        sourceVersion=event['detail']['sourceReference']
    )

