import subprocess
import json
from botocore.exceptions import ClientError
from constants import * 
from config import *


def fetch_metadata_from_oss():
    """Fetch metadata from Ali Cloud OSS using rclone."""
    result = subprocess.run(['rclone', 'lsjson', 'aliyun:test-ali-video'], stdout=subprocess.PIPE, check=True)
    metadata = json.loads(result.stdout.decode('utf-8'))
    return metadata


def get_pending_videos():
    """Retrieve video metadata with 'pending' status from DynamoDB."""
    response = dynamodb_client.scan(
        TableName=DYNAMODB_TABLE,
        FilterExpression="#status = :pending",
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={':pending': {'S': 'pending'}}
    )
    return response.get('Items', [])


def update_video_status(video_id, status):
    """Update the status of a video in DynamoDB."""
    dynamodb_client.update_item(
        TableName=DYNAMODB_TABLE,
        Key={'video_id': {'S': video_id}},
        UpdateExpression='SET #status = :status',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={':status': {'S': status}}
    )


def download_video(video_path):
    """Download a video from OSS to S3."""
    try:
        subprocess.run(
            ['rclone', 'copy', f'aliyun:{OSS_BUCKET}/{video_path}', f's3://{VIDEO_BUCKET}/{VIDEO_BUCKET_FOLDER}/{video_path}'],
            check=True
        )
        return True
    except subprocess.CalledProcessError:
        return False


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


def send_notifications():
    """Send notifications to SNS and SQS."""
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message="Video transfer completed"
    )
    sqs_client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody="Transfer completed"
    )


def transfer_videos():
    """Transfer videos with pending status and retry failed ones."""
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


if __name__ == '__main__':
    transfer_videos()
    send_notifications()
