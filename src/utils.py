import json
import subprocess
import sys
import time
from constants import *
from config import *
import constants
import psutil

def fetch_metadata_from_oss():
    """Fetch metadata from Ali Cloud OSS using rclone."""
    result = subprocess.run(['rclone', 'lsjson', 'aliyun:test-ali-video'], stdout=subprocess.PIPE, check=True)
    metadata = json.loads(result.stdout.decode('utf-8'))
    return metadata

def update_video_status(video_id, status, transfer_time=None):
    """Update the status and transfer time of a video in DynamoDB."""
    update_expression = 'SET #Transfer_Status = :status'
    expression_attribute_values = {':status': {'S': status}}

    # Conditionally add transfer time update
    if transfer_time is not None:
        update_expression += ', #Transfer_Time = :transfer_time'
        expression_attribute_values[':transfer_time'] = {'N': str(transfer_time)}

    # Update the item in DynamoDB
    dynamodb_client.update_item(
        TableName=DYNAMODB_TABLE,
        Key={'video_id': {'S': video_id}},
        UpdateExpression=update_expression,
        ExpressionAttributeNames={"#Transfer_Status": "Transfer_Status", "#Transfer_Time": "Transfer_Time"},
        ExpressionAttributeValues=expression_attribute_values
    )

def upload_metadata_to_dynamodb():
    """Fetch metadata from OSS and upload it to DynamoDB with initial status 'pending'."""
    metadata = fetch_metadata_from_oss()
    
    for video in metadata:
        # Update metadata to use a clean file name
        video_id = video["Path"] 

        # Add metadata to DynamoDB with initial transfer status
        dynamodb_client.put_item(
            TableName=DYNAMODB_TABLE,
            Item={
                "video_id": {"S": video_id},                      # File name as the primary key
                "Transfer_Status": {"S": "pending"},              # Default transfer status
                "Transfer_Time": {"N": "0"},                      # Initial transfer time (0 seconds, not transferred yet)
                "Path": {"S": video_id},                          # Human-readable path
                "Size": {"N": str(video["Size"])},                # Size of the video file
                "MimeType": {"S": video["MimeType"]},             # MIME type of the file
                "ModTime": {"S": video["ModTime"]},               # Last modified time
                "isDir": {"BOOL": video["IsDir"]},                # Is it a directory? (Boolean)
                "Tier": {"S": video["Tier"]}                      # Storage tier (e.g., STANDARD)
            }
        )

def save_metadata_to_s3(metadata):
    """Save metadata to S3 as a JSON file with proper encoding for Chinese characters."""
    try:
        # Use json.dumps with ensure_ascii=False to preserve Chinese characters
        s3_client.put_object(
            Bucket=AWS_LOG_BUCKET,
            Key=S3_METADATA_PATH,
            Body=json.dumps(metadata, ensure_ascii=False),  # Preserve Chinese characters
            ContentType='application/json'
        )
        print(f"Metadata successfully uploaded to S3: {S3_METADATA_PATH}")
    except Exception as e:
        print(f"Error uploading metadata to S3: {e}")
        sys.exit(1)

def download_video(video_path):
    """Download a video from OSS to S3."""
    try:
        subprocess.run(
            [
             'rclone', 
             'copy', 
             f'aliyun:{OSS_BUCKET}/{video_path}', 
             f'aws_s3:{AWS_VIDEO_BUCKET}/{VIDEO_BUCKET_FOLDER}'
            ],
            check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error during video transfer: {e}")
        return False

def send_sns_notification(percentage):
    """
    Sends an SNS notification for progress percentage.
    """
    subject = f"Video Transfer Progress: {percentage}% Complete"
    message = f"The video transfer process has reached {percentage}% completion."
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )
    print(f"SNS Notification sent: {subject}")

def send_sqs_notification(status, enable_notification=True):
    """
    Sends an SQS message to notify Lambda about the transfer process status.
    Can disable notification with `enable_notification`.
    """
    if enable_notification:
        sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=f"Video transfer process completed with status: {status}"
        )
        print(f"SQS Notification sent: {status}")
    else:
        print(f"SQS Notification skipped: {status}")

def get_pending_videos():
    """Retrieve video metadata with 'pending' status from DynamoDB."""
    response = dynamodb_client.scan(
        TableName=DYNAMODB_TABLE,
        FilterExpression="#Transfer_Status = :pending",
        ExpressionAttributeNames={'#Transfer_Status': 'Transfer_Status'},
        ExpressionAttributeValues={':pending': {'S': 'pending'}}
    )
    return response.get('Items', [])

def retry_failed_videos(failed_videos):
    """Retry downloading videos with 'failed' status."""
    for video in failed_videos:
        video_path = video['video_id']['S']
        success = download_video(video_path)
        if success:
            update_video_status(video_path, 'completed')
            # log_transfer_status(video_path, 'completed')
        else:
            update_video_status(video_path, 'failed')
            # log_transfer_status(video_path, 'failed')

# Function to log metrics to CloudWatch Log Stream
def log_to_cloudwatch(metric_name, value, video_path=None):
    """
    Push system resource metrics (CPU, memory, etc.) and video transfer progress to CloudWatch.
    Logs the metrics to a specific CloudWatch Log Group and Log Stream.
    """
    # Prepare log message
    log_message = {
        'timestamp': int(time.time() * 1000),  # Milliseconds
        'message': f"Metric: {metric_name}, Value: {value}, VideoPath: {video_path if video_path else 'N/A'}"
    }
    
    try:
        # Retrieve or create log stream
        response = logs_client.describe_log_streams(
            logGroupName=LOG_GROUP_NAME,
            logStreamNamePrefix=LOG_STREAM_NAME
        )
        
        # If log stream doesn't exist, create it
        if not response['logStreams']:
            logs_client.create_log_stream(
                logGroupName=LOG_GROUP_NAME,
                logStreamName=LOG_STREAM_NAME
            )
        
        # Push the log message to CloudWatch Log Stream
        logs_client.put_log_events(
            logGroupName=LOG_GROUP_NAME,
            logStreamName=LOG_STREAM_NAME,
            logEvents=[log_message]
        )

        print(f"Successfully logged metric: {metric_name}, Value: {value}")

    except Exception as e:
        print(f"Error logging to CloudWatch Logs: {e}")