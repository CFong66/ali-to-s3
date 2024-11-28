import boto3
from constants import REGION_NAME

# Initialize S3, secret manager and CloudWatch clients
cloudwatch_client = boto3.client('cloudwatch', region_name=REGION_NAME)
s3_client = boto3.client('s3', region_name=REGION_NAME)
secrets_client = boto3.client('secretsmanager', region_name=REGION_NAME)
sns_client = boto3.client('sns', region_name=REGION_NAME)
ssm_client = boto3.client('ssm', region_name='ap-southeast-2')