# Define constants
import os

# Retrieve SNS_TOPIC_ARN and SQS_QUEUE_URL from environment variables



OSS_BUCKET = 'test-ali-video'
AWS_VIDEO_BUCKET = 'ali-video-storing-bucket'
AWS_LOG_BUCKET = 'ali-vt-log-bucket'
DYNAMODB_TABLE = 'video_metadata'
VIDEO_BUCKET_FOLDER = 'ali-videos'
S3_METADATA_PATH = 'video-metadata/metadata.json' 


"""
flag for rclone to multipart upload
  --s3-upload-cutoff 300M \
  --s3-chunk-size 16M \
  --s3-upload-concurrency 8 \
  --transfers 4 \
  --progress
"""
