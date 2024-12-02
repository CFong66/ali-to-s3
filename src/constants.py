# Define constants
import os

# Retrieve SNS_TOPIC_ARN and SQS_QUEUE_URL from environment variables
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')

OSS_BUCKET = 'test-ali-video'
VIDEO_BUCKET = 'ali-video-storing-bucket'
LOG_BUCKET = 'ali-vt-log-bucket'
DYNAMODB_TABLE = 'video_metadata'
VIDEO_BUCKET_FOLDER = 'ali-videos'
