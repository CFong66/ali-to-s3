import os

# Retrieve environment variables
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')

# Check if the values are retrieved correctly
print(f"SNS_TOPIC_ARN: {SNS_TOPIC_ARN}")
print(f"SQS_QUEUE_URL: {SQS_QUEUE_URL}")
