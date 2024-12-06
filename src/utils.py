import json
import subprocess
import sys
from constants import *
from config import *
import constants

def fetch_metadata_from_oss():
    """Fetch metadata from Ali Cloud OSS using rclone."""
    result = subprocess.run(['rclone', 'lsjson', 'aliyun:test-ali-video'], stdout=subprocess.PIPE, check=True)
    metadata = json.loads(result.stdout.decode('utf-8'))
    return metadata

def update_video_status(video_id, status):
    """Update the status of a video in DynamoDB."""
    dynamodb_client.update_item(
        TableName=DYNAMODB_TABLE,
        Key={'video_id': {'S': video_id}},
        UpdateExpression='SET #Transfer_Status = :status',
        ExpressionAttributeNames={"#Transfer_Status": "Transfer_Status"},
        ExpressionAttributeValues={':status': {'S': status}}
    )

def upload_metadata_to_dynamodb():
    """Fetch metadata from OSS and upload it to DynamoDB with initial status 'pending'."""
    metadata = fetch_metadata_from_oss()
    
    for video in metadata:
        # Decode the Unicode-encoded Path field
        # video_id = decode_unicode(video["Path"])  # Use the decoded file name as video_id

        # Update metadata to use a clean file name
        video_id = video["Path"] 

        # Add metadata to DynamoDB with initial transfer status
        dynamodb_client.put_item(
            TableName=DYNAMODB_TABLE,
            Item={
                "video_id": {"S": video_id},                      # File name as the primary key
                "Transfer_Status": {"S": "pending"},              # Default transfer status
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

def send_notifications(completed=False, enable_notifications=False, message=""):
    # Check if the job is completed and SNS_TOPIC_ARN is set
    if completed:
        # Check SNS_TOPIC_ARN from constants.py
        sns_topic_arn = constants.SNS_TOPIC_ARN

        if not sns_topic_arn:
            print("Error: SNS_TOPIC_ARN in constants.py is not set.")
            sys.exit(1)

        # Notify via SNS (this will always happen when completed)
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message="Video transfer completed successfully."
        )
        print("Notification sent to SNS topic.")
    
    # Send progress updates to SNS if progress notifications are enabled
    if enable_notifications and message:
        sns_topic_arn = constants.SNS_TOPIC_ARN
        if sns_topic_arn:
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=message
            )
            print("Progress update sent to SNS.")

    # Only send SQS message if enable_notifications is True
    if enable_notifications:
        # Check SQS_QUEUE_URL from constants.py
        sqs_queue_url = constants.SQS_QUEUE_URL
        
        if not sqs_queue_url:
            print("Error: SQS_QUEUE_URL in constants.py is not set.")
            sys.exit(1)

        # Send message to SQS to trigger Lambda
        sqs_client.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody="Transfer completed"
        )
        print("Message sent to SQS queue.")
    else:
        print("SQS notification is disabled by switch. Skipping message sending.")

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