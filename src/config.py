import boto3

# Initialize AWS clients
s3_client = boto3.client('s3',region_name='ap-southeast-2')
dynamodb_client = boto3.client('dynamodb',region_name='ap-southeast-2')
sns_client = boto3.client('sns',region_name='ap-southeast-2')
sqs_client = boto3.client('sqs',region_name='ap-southeast-2')
logs_client = boto3.client('logs', region_name='ap-southeast-2')
cloudwatch = boto3.client('cloudwatch', region_name='ap-southeast-2')