import os
import json
import boto3
import logging
import requests
from time import sleep

# Setup AWS Clients
s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')
sns_client = boto3.client('sns')
sqs_client = boto3.client('sqs')
ssm_client = boto3.client('ssm')

# Setup Ali Cloud OSS (Assuming ossfs or another OSS SDK is already installed)
OSS_ENDPOINT = "https://oss-cn-hangzhou.aliyuncs.com"  # Replace with actual OSS endpoint
OSS_BUCKET_NAME = "ali-video-storing-bucket"

# DynamoDB Table
DYNAMODB_TABLE_NAME = "video_metadata"
DYNAMODB_HASH_KEY = "video_id"

# S3 Buckets
LOG_BUCKET_NAME = "ali-vt-log-bucket"

# SNS Topic ARN
SNS_TOPIC_ARN = "arn:aws:sns:ap-southeast-2:680672213275:video-transfer-completed"

# SQS Queue URL
SQS_QUEUE_URL = "https://sqs.ap-southeast-2.amazonaws.com/680672213275/stop-ec2-queue"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Helper function to log to S3
def log_to_s3(message, log_file="video_transfer.log"):
    try:
        s3_client.put_object(
            Bucket=LOG_BUCKET_NAME,
            Key=log_file,
            Body=message.encode('utf-8')
        )
    except Exception as e:
        logger.error(f"Error logging to S3: {e}")
        
# Fetch OSS metadata
def fetch_oss_metadata():
    # Use OSS SDK or make HTTP request to fetch video metadata from Ali Cloud
    # Assuming the metadata is stored in a JSON file or accessible API endpoint
    try:
        # Simulate OSS metadata fetch (Replace with actual implementation)
        oss_metadata_url = f"{OSS_ENDPOINT}/metadata.json"  # Replace with actual URL
        response = requests.get(oss_metadata_url)
        response.raise_for_status()
        metadata = response.json()
        return metadata
    except Exception as e:
        logger.error(f"Error fetching OSS metadata: {e}")
        log_to_s3(f"Error fetching OSS metadata: {e}")
        return []

# Store metadata in DynamoDB with pending status
def store_metadata_in_dynamodb(metadata):
    for video in metadata:
        try:
            dynamodb_client.put_item(
                TableName=DYNAMODB_TABLE_NAME,
                Item={
                    DYNAMODB_HASH_KEY: {'S': video['video_id']},
                    'status': {'S': 'pending'},
                    'video_url': {'S': video['video_url']}
                }
            )
            logger.info(f"Stored metadata for video_id: {video['video_id']}")
        except Exception as e:
            logger.error(f"Error storing metadata for {video['video_id']}: {e}")
            log_to_s3(f"Error storing metadata for {video['video_id']}: {e}")

# Download video from OSS and store in S3
def download_video_from_oss(video_url, video_id):
    try:
        # Use the OSS SDK or HTTP client to download the video
        video_data = requests.get(video_url, stream=True)
        video_data.raise_for_status()
        
        # Save to S3
        s3_client.put_object(
            Bucket=OSS_BUCKET_NAME,
            Key=f"{video_id}.mp4",
            Body=video_data.content
        )
        logger.info(f"Downloaded and uploaded video {video_id} to S3")
        return True
    except Exception as e:
        logger.error(f"Error downloading video {video_id}: {e}")
        log_to_s3(f"Error downloading video {video_id}: {e}")
        return False

# Update video status in DynamoDB
def update_video_status(video_id, status):
    try:
        dynamodb_client.update_item(
            TableName=DYNAMODB_TABLE_NAME,
            Key={DYNAMODB_HASH_KEY: {'S': video_id}},
            UpdateExpression="SET #status = :status",
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': {'S': status}}
        )
        logger.info(f"Updated status of video {video_id} to {status}")
    except Exception as e:
        logger.error(f"Error updating status for {video_id}: {e}")
        log_to_s3(f"Error updating status for {video_id}: {e}")

# Send SNS notification
def send_sns_notification(message):
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message
        )
        logger.info("Sent SNS notification")
    except Exception as e:
        logger.error(f"Error sending SNS notification: {e}")
        log_to_s3(f"Error sending SNS notification: {e}")

# Send SQS message to trigger Lambda
def send_sqs_message(message):
    try:
        sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=message
        )
        logger.info("Sent SQS message to trigger Lambda")
    except Exception as e:
        logger.error(f"Error sending SQS message: {e}")
        log_to_s3(f"Error sending SQS message: {e}")

# Main function to transfer videos
def transfer_videos():
    try:
        # Step 1: Fetch metadata from Ali Cloud OSS
        metadata = fetch_oss_metadata()
        if not metadata:
            logger.error("No metadata found, aborting transfer.")
            return
        
        # Step 2: Store metadata in DynamoDB
        store_metadata_in_dynamodb(metadata)
        
        # Step 3: Process videos with 'pending' status
        for video in metadata:
            if video['status'] == 'pending':
                # Download video from OSS and upload to S3
                success = download_video_from_oss(video['video_url'], video['video_id'])
                if success:
                    # Update video status to 'completed'
                    update_video_status(video['video_id'], 'completed')
                    # Log transfer success
                    log_to_s3(f"Video {video['video_id']} transferred successfully.")
                else:
                    # Update video status to 'failed'
                    update_video_status(video['video_id'], 'failed')
                    log_to_s3(f"Video {video['video_id']} transfer failed.")
        
        # Step 4: Notify completion via SNS
        send_sns_notification("Video transfer completed successfully.")

        # Step 5: Send SQS message to stop EC2 instance
        send_sqs_message("Stop EC2 instance after video transfer completion.")

    except Exception as e:
        logger.error(f"Error in transfer process: {e}")
        log_to_s3(f"Error in transfer process: {e}")

if __name__ == "__main__":
    transfer_videos()
