import os
import subprocess
import json
import boto3
from botocore.exceptions import ClientError
from constants import * 
from config import *


def fetch_metadata_from_oss():
    # Fetch metadata from Ali Cloud OSS using rclone
    result = subprocess.run(['rclone', 'lsjson', 'aliyun:video-metadata-folder'], stdout=subprocess.PIPE)
    metadata = json.loads(result.stdout.decode('utf-8'))
    return metadata

def upload_metadata_to_dynamodb(metadata):
    for item in metadata:
        # Upload each metadata entry to DynamoDB
        dynamodb_client.put_item(
            TableName=DYNAMODB_TABLE,
            Item={
                'video_id': {'S': item['path']},
                'status': {'S': 'pending'}
            }
        )

def transfer_videos():
    # Loop through metadata and transfer videos
    metadata = fetch_metadata_from_oss()
    for item in metadata:
        video_path = item['path']
        
        # Use rclone to download video from OSS and upload to S3
        subprocess.run(['rclone', 'copy', f'aliyun:{OSS_BUCKET}/{video_path}', f's3://{S3_BUCKET}/{video_path}'])
        
        # Log success
        log_transfer_status(video_path, 'completed')

        # Update DynamoDB status to 'completed'
        dynamodb_client.update_item(
            TableName=DYNAMODB_TABLE,
            Key={'video_id': {'S': video_path}},
            UpdateExpression='SET #status = :status',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': {'S': 'completed'}}
        )

def log_transfer_status(video_path, status):
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
    # Notify completion via SNS
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message="Video transfer completed"
    )

    # Send message to SQS to trigger Lambda to stop EC2
    sqs_client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody="Transfer completed"
    )

if __name__ == '__main__':
    transfer_videos()
    send_notifications()
