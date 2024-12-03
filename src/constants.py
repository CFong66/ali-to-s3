# Define constants
import os

# Retrieve SNS_TOPIC_ARN and SQS_QUEUE_URL from environment variables
SNS_TOPIC_ARN = "arn:aws:sns:ap-southeast-2:680672213275:ali-vt-s3-notification"
SQS_QUEUE_URL = "https://sqs.ap-southeast-2.amazonaws.com/680672213275/ec2-control-queue"

OSS_BUCKET = 'test-ali-video'
VIDEO_BUCKET = 'ali-video-storing-bucket'
LOG_BUCKET = 'ali-vt-log-bucket'
DYNAMODB_TABLE = 'video_metadata'
VIDEO_BUCKET_FOLDER = 'ali-videos'
