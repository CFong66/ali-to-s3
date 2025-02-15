import os
ALI_VOD_REGION = "ap-southeast-1"
AWS_LOG_BUCKET = 'jr-prod-alicloud-alicloud-sync-log-bucket'
AWS_VIDEO_BUCKET = 'jr-prod-source-videos-bucket'
COMPLETED_LOG_FILENAME = "/home/ubuntu/completed_video_count.log"
DYNAMODB_TABLE = 'AliCloudVideo_Metadata_PROD-alicloud'
FAILED_LOG_FILENAME = "transfer_failed.log"
FINAL_METADATA_LOCAL_PATH = "/home/ubuntu/final_metadata.json"
LOG_FOLDER = "log-files"
LOG_GROUP_NAME = "ali_vt_s3_ec2"
LOG_STREAM_NAME = "ali_video_transfer_2024"
METADATA_LOCAL_PATH = "/home/ubuntu/final_alicloud_metadata.json"
S3_METADATA_PATH = 'ali-video-metadata/metadata.json'
TEMP_VIDEO_LOCAL_PATH = "/data"
VIDEO_BUCKET_FOLDER = 'ali-videos'
SNS_TOPIC_ARN='arn:aws:sns:ap-southeast-2:026559016816:alicloud-sync-notification'
SQS_QUEUE_URL='https://sqs.ap-southeast-2.amazonaws.com/026559016816/jr-uat-alicloud-alicloud-sync-video-queue'
LOG_DEBUG_FILE = "/home/ubuntu/debug_log.txt"
TEST_METADATA_LOCAL_PATH = "/home/ubuntu/test_matched_metadata.json"
FILTER_API_URL = "https://api.jiangren.com.au/videos/ali-cloud/valid-ids"
PAGE_SIZE = 100
TRIGGER_TRANSCODING_API= "https://api.jiangren.com.au/s3-videos/{video_id}/transcoding"
BASE_API_URL = "https://api.jiangren.com.au/s3-videos/ali-cloud"
VIDEO_STATUS_SUCCESS = "success"
VIDEO_STATUS_NOT_FOUND = "not_found"
VIDEO_STATUS_CREATED = "created"

# Below constants will be retrieved from the Vault
UAT_API_URL = 'https://uat-api.jiangren.com.au/s3-videos/'
PROD_API_URL = 'https://api.jiangren.com.au/s3-videos/'

# Determine the environment
environment = os.getenv('ENVIRONMENT', 'UAT')  # Default to UAT if not set

# Select the table name based on the environment
if environment == 'PROD':
    API_URL = PROD_API_URL
    table_name = DYNAMODB_TABLE

else:
    table_name = DYNAMODB_TABLE
    API_URL = UAT_API_URL
