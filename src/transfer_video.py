import json
import subprocess
import sys
import boto3

# Initialize AWS and DynamoDB clients
dynamodb_client = boto3.client('dynamodb', region_name='ap-southeast-2')
sns_client = boto3.client('sns', region_name='ap-southeast-2')
sqs_client = boto3.client('sqs', region_name='ap-southeast-2')
s3_client = boto3.client('s3', region_name='ap-southeast-2')

# Configuration for your DynamoDB and S3 bucket
DYNAMODB_TABLE = 'your-dynamodb-table-name'
VIDEO_BUCKET = 'your-video-bucket-name'
VIDEO_BUCKET_FOLDER = 'your-video-folder-name'
LOG_BUCKET = 'your-log-bucket-name'

# Environment variables (replace with actual values or use environment vars)
SNS_TOPIC_ARN = 'your-sns-topic-arn'
SQS_QUEUE_URL = 'your-sqs-queue-url'


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
        UpdateExpression='SET #status = :status',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={':status': {'S': status}}
    )


def upload_metadata_to_dynamodb():
    """Fetch metadata from OSS and upload it to DynamoDB with initial status 'pending'."""
    metadata = fetch_metadata_from_oss()
    
    for video in metadata:
        video_id = video['Path']  # Assuming the video path is the unique identifier
        # Add metadata to DynamoDB and set the status to 'pending'
        dynamodb_client.put_item(
            TableName=DYNAMODB_TABLE,
            Item={
                'video_id': {'S': video_id},
                'status': {'S': 'pending'},  # Default status
                'metadata': {'S': json.dumps(video)}  # Store full metadata if needed
            }
        )


def download_video(video_path):
    """Download a video from OSS to S3."""
    try:
        subprocess.run(
            ['rclone', 'copy', f'aliyun:test-ali-video/{video_path}', f's3://{VIDEO_BUCKET}/{VIDEO_BUCKET_FOLDER}/{video_path}'],
            check=True
        )
        return True
    except subprocess.CalledProcessError:
        return False


def get_pending_videos():
    """Retrieve video metadata with 'pending' status from DynamoDB."""
    response = dynamodb_client.scan(
        TableName=DYNAMODB_TABLE,
        FilterExpression="#status = :pending",
        ExpressionAttributeNames={'#status': 'status'},
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
            log_transfer_status(video_path, 'completed')
        else:
            log_transfer_status(video_path, 'failed')


def log_transfer_status(video_path, status):
    """Log the transfer status to S3."""
    log_entry = {
        'video_id': video_path,
        'status': status,
        'message': f"Transfer {status} for {video_path}"
    }
    s3_client.put_object(
        Bucket=LOG_BUCKET,
        Key=f"logs/{video_path}.json",
        Body=json.dumps(log_entry)
    )


def send_notifications(completed=False):
    """Send notifications to SNS and optionally to SQS."""
    if not SNS_TOPIC_ARN:
        print("Error: SNS_TOPIC_ARN environment variable is not set.")
        sys.exit(1)

    # Only send SQS message if the transfer was successful
    if completed:
        if not SQS_QUEUE_URL:
            print("Error: SQS_QUEUE_URL environment variable is not set.")
            sys.exit(1)

        sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody="Transfer completed"
        )
        
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message="Video transfer completed"
    )


def transfer_videos():
    """
    Transfer videos with pending status and retry failed ones.
    Returns True if all videos are successfully transferred; False otherwise.
    """
    pending_videos = get_pending_videos()
    failed_videos = []

    for video in pending_videos:
        video_path = video['video_id']['S']
        success = download_video(video_path)

        if success:
            update_video_status(video_path, 'completed')
            log_transfer_status(video_path, 'completed')
        else:
            update_video_status(video_path, 'failed')
            log_transfer_status(video_path, 'failed')
            failed_videos.append(video)

    # Retry failed videos
    retry_failed_videos(failed_videos)

    # Return True if no videos remain in the failed_videos list
    return len(failed_videos) == 0


if __name__ == '__main__':
    # Upload metadata to DynamoDB before starting the transfer process
    upload_metadata_to_dynamodb()
    
    job_success = transfer_videos()
    
    # Only send notifications if the job is successful
    if job_success:
        send_notifications(completed=True)
    else:
        print("Video transfer failed. No notifications will be sent.")
