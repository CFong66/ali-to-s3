import os

# Retrieve environment variables
sns_topic_arn = os.getenv('SNS_TOPIC_ARN')
sqs_queue_url = os.getenv('SQS_QUEUE_URL')
ali_access_key_id = os.getenv('ALI_ACCESS_KEY_ID')
ali_access_key_secret = os.getenv('ALI_ACCESS_KEY_SECRET')

print(f"SNS Topic ARN: {sns_topic_arn}")
print(f"SQS Queue URL: {sqs_queue_url}")
print(f"Ali Access Key ID: {ali_access_key_id}")
print(f"Ali Access Key Secret: {ali_access_key_secret}")
print(os.environ)  # Check if SNS_TOPIC_ARN and SQS_QUEUE_URL are present

SNS_TOPIC_ARN = "arn:aws:sns:ap-southeast-2:680672213275:ali-vt-s3-notification"
SQS_QUEUE_URL = "https://sqs.ap-southeast-2.amazonaws.com/680672213275/ec2-control-queue"
