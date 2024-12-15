import boto3
from constants import *
from aliyunsdkcore.client import AcsClient

# Initialize AWS clients
s3_client = boto3.client('s3',region_name='ap-southeast-2')
dynamodb_client = boto3.client('dynamodb',region_name='ap-southeast-2')
sns_client = boto3.client('sns',region_name='ap-southeast-2')
sqs_client = boto3.client('sqs',region_name='ap-southeast-2')
Ali_client = AcsClient(ALI_ACCESS_KEY_ID, ALI_ACCESS_KEY_SECRET,ALI_VOD_REGION)
logs_client = boto3.client("logs", region_name="ap-southeast-2")