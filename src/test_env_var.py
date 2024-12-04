import os
# from dotenv import load_dotenv

# # Load environment variables from the .env file
# load_dotenv()

SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')

print(f"SNS_TOPIC_ARN: {SNS_TOPIC_ARN}")
print(f"SQS_QUEUE_URL: {SQS_QUEUE_URL}")
