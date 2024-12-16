import json
import time
import re
from datetime import datetime
import os
import sys
import requests
from constants import *
from config import *
from aliyunsdkvod.request.v20170321 import GetVideoListRequest
from aliyunsdkvod.request.v20170321 import GetMezzanineInfoRequest
from botocore.exceptions import BotoCoreError, ClientError

def fetch_metadata_batch(page_no, page_size, sort_by="CreationTime", start_time=None, end_time=None):
    """Fetch metadata in batches with optional sorting and time range filtering."""
    request = GetVideoListRequest.GetVideoListRequest()
    request.set_PageNo(page_no)  # Set the current page number
    request.set_PageSize(page_size)  # Fetch up to page_size items per page
    request.set_Status("Normal")  # Filter by status "Normal"
    request.set_SortBy(sort_by)  # Sort by creation time

    if start_time:
        request.set_StartTime(start_time)  # Set the start time for querying videos
    if end_time:
        request.set_EndTime(end_time)  # Set the end time for querying videos

    try:
        # Send the request and parse the response
        response = Ali_client.do_action_with_exception(request)
        response_dict = json.loads(response)
        return response_dict
    except Exception as e:
        print(f"Error fetching metadata from Aliyun VOD: {e}")
        return None

def fetch_first_batch():
    """Fetch the first batch of metadata (oldest videos)."""
    page_size = 100  # Max allowed by Aliyun API
    all_metadata = {}  # Dictionary to store metadata with unique keys
    page_no = 1

    while True:
        batch_response = fetch_metadata_batch(page_no, page_size, sort_by="CreationTime:Asc")
        if not batch_response:
            break  # Exit loop if an error occurs

        videos = batch_response.get("VideoList", {}).get("Video", [])
        for video in videos:
            if video.get("CateName") == "production":
                video_id = video.get("VideoId")
                if video_id not in all_metadata:
                    all_metadata[video_id] = video

        total_videos = batch_response.get("Total", 0)
        print(f"Fetched {len(all_metadata)} unique records out of {total_videos} total videos.")

        # Check if all videos have been fetched
        if len(all_metadata) >= total_videos or not videos:
            break

        # Move to the next page
        page_no += 1

    return all_metadata

def fetch_remaining_metadata(start_time):
    """Fetch the remaining metadata after the first batch."""
    page_size = 100  # Max allowed by Aliyun API
    all_metadata = {}  # Dictionary to store metadata with unique keys
    page_no = 1

    while True:
        batch_response = fetch_metadata_batch(page_no, page_size, sort_by="CreationTime:Asc", start_time=start_time)
        if not batch_response:
            break  # Exit loop if an error occurs

        videos = batch_response.get("VideoList", {}).get("Video", [])
        for video in videos:
            if video.get("CateName") == "production":
                video_id = video.get("VideoId")
                if video_id not in all_metadata:
                    all_metadata[video_id] = video

        total_videos = batch_response.get("Total", 0)
        print(f"Fetched {len(all_metadata)} unique remaining records out of {total_videos} total videos.")

        # Check if all videos have been fetched
        if len(all_metadata) >= total_videos or not videos:
            break

        # Move to the next page
        page_no += 1

    return all_metadata

def fetch_all_metadata():
    """Fetch all metadata in two steps: oldest first, then the rest."""
    print("Fetching the first batch of metadata (oldest)...")
    first_batch = fetch_first_batch()
    if not first_batch:
        print("Failed to fetch the first batch.")
        return

    # Wait for a while to ensure no new videos are uploaded
    print("Waiting for new uploads to finish...")
    time.sleep(10)

    # Fetch the remaining metadata
    print("Fetching the remaining metadata...")
    last_video_time = max(video["CreationTime"] for video in first_batch.values())
    remaining_batch = fetch_remaining_metadata(last_video_time)

    if not remaining_batch:
        print("No remaining metadata to fetch.")
        return first_batch

    # Combine both batches in time order
    print("Combining metadata...")
    all_metadata = {**remaining_batch, **first_batch}

    return all_metadata

def save_metadata_to_file(metadata, file_path):
    """
    Save metadata to a local file with unique title renaming logic.
    The naming convention for S3 key and file names is: 
    Title_CreationTime_OrderingNumber (if duplicates exist).
    All special characters are replaced with underscores for smoother file names.
    """
    try:
        # A dictionary to track duplicate titles
        title_tracker = {}

        for video in metadata:
            # Extract title, creation time, and initialize ordering number
            video_title = video.get("Title", "untitled")
            creation_time = video.get("CreateTime", "unknown")
            
            # Replace all spaces, dashes, colons, and other special characters with underscores
            cleaned_title = re.sub(r"[^a-zA-Z0-9\u4e00-\u9fff]+", "_", video_title)
            
            # Format creation time for readability and replace special characters with underscores
            try:
                formatted_creation_time = datetime.strptime(creation_time, '%Y-%m-%dT%H:%M:%S').strftime('%Y_%m_%dT%H_%M_%S')
            except ValueError:
                formatted_creation_time = re.sub(r"[^a-zA-Z0-9]+", "_", creation_time)
            
            # Construct the base key
            base_key = f"{cleaned_title}_{formatted_creation_time}"
            
            # Resolve duplicates by adding an ordering number
            if base_key in title_tracker:
                title_tracker[base_key] += 1
                unique_key = f"{base_key}_{title_tracker[base_key]}"
            else:
                title_tracker[base_key] = 1
                unique_key = base_key
            
            # Update the unique title in the metadata
            video["unique_title"] = unique_key
        
        # Save the updated metadata to the file
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(metadata, file, ensure_ascii=False, indent=2)  # Preserve Chinese characters
        print(f"Metadata saved to local file: {file_path}")
        return file_path  # Explicitly return the file path
    
    except Exception as e:
        print(f"Error saving metadata file: {e}")
        sys.exit(1)

def count_videos_in_file(file_path):
    """
    Count the number of videos in a saved metadata file.
    """
    try:
        # Read the metadata from the local file
        with open(file_path, "r", encoding="utf-8") as file:
            metadata = json.load(file)
        
        # Count the number of video entries
        video_count = len(metadata)
        print(f"Number of videos in metadata: {video_count}")
        return video_count
    
    except Exception as e:
        print(f"Error reading metadata file: {e}")
        sys.exit(1)

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

def fetch_mezzanine_info(video_id):
    """Fetch the mezzanine information for a video using its VideoId."""
    request = GetMezzanineInfoRequest.GetMezzanineInfoRequest()
    request.set_VideoId(video_id)
    request.set_AuthTimeout(7200)  # Set timeout for URL validity (optional)

    try:
        response = Ali_client.do_action_with_exception(request)
        response_dict = json.loads(response)
        file_url = response_dict.get("Mezzanine", {}).get("FileURL")
        return file_url
    
    except Exception as e:
        print(f"Error fetching mezzanine info for VideoId {video_id}: {e}")
        return None

def append_file_urls_to_metadata(file_path, total_metadata_count):
    """
    Update the metadata JSON file by appending file URLs to each video metadata.
    The updates are saved directly into the original file.
    Includes a progress tracker.
    """
    try:
        # Load metadata from file
        with open(file_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        appended_count = 0

        # Update each video with its file URL
        for index, (video_id, video_metadata) in enumerate(metadata.items(), start=1):
            file_url = fetch_mezzanine_info(video_id)
            if file_url:
                video_metadata["FileURL"] = file_url
                appended_count += 1
                print(f"Appended {appended_count}/{total_metadata_count} FileURL {file_url} for VideoId {video_id}.")
            else:
                print(f"Failed to fetch FileURL for VideoId {video_id}. Progress: {index}/{total_metadata_count}")

        # Save the updated metadata back to the original file
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        print(f"Updated metadata saved to {file_path}. Total appended: {appended_count}/{total_metadata_count}")

    except FileNotFoundError:
        print("Metadata file not found.")

    except json.JSONDecodeError:
        print("Error decoding JSON file.")

def update_video_metadata_with_final_urls(metadata_file, output_file):
    """
    Update video metadata with final download URLs and save to a new file.

    Args:
        metadata_file (str): Path to the JSON file containing video metadata.
        output_file (str): Path to save the updated metadata with final download URLs.
    """
    def generate_final_download_url(file_url, storage_location):
        """
        Generate the final download URL.

        Args:
            file_url (str): The original FileURL from the metadata.
            storage_location (str): The StorageLocation from the metadata.

        Returns:
            str: The final download URL.
        """
        if not file_url or not storage_location:
            raise ValueError("Both 'FileURL' and 'StorageLocation' must be provided.")
        
        # Extract the relative path from the FileURL
        relative_path = "/".join(file_url.split("/")[3:])
        # Construct the final URL
        return f"https://{storage_location}/{relative_path}"

    try:
        # Load the metadata from the file
        with open(metadata_file, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        # Update each video's metadata with the final download URL
        for video_id, video_data in metadata.items():
            try:
                file_url = video_data.get("FileURL")
                storage_location = video_data.get("StorageLocation")
                video_data["FinalDownloadURL"] = generate_final_download_url(file_url, storage_location)
            except ValueError as e:
                print(f"Skipping video {video_id}: {e}")

        # Save the updated metadata to the output file
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        print(f"Updated metadata saved to {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")

def upload_metadata_to_dynamodb(local_file_path):
    """Upload metadata to DynamoDB with initial transfer status, converting size to MB and duration to h:m:s format."""
    
    def bytes_to_mb(bytes_size):
        """Convert bytes to MB with 2 decimal places."""
        return round(bytes_size / (1024 * 1024), 2)

    def seconds_to_hms(seconds):
        """Convert seconds to h:m:s format."""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    try: 
        # Load metadata from JSON file
        with open(local_file_path, "r", encoding="utf-8") as file:
            metadata = json.load(file)

        for video_id, video_data in metadata.items():
            # Convert size to MB and duration to h:m:s format
            size_mb = bytes_to_mb(video_data.get("Size", 0))
            duration_hms = seconds_to_hms(video_data.get("Duration", 0))

            # Prepare the item for DynamoDB
            snapshots = video_data.get("Snapshots", {}).get("Snapshot", [])
            snapshots_json = json.dumps(snapshots)  # Serialize Snapshots array

            # Prepare item for DynamoDB
            item = {
                "video_id": {"S": video_id},                             # Outer key as primary key
                "Transfer_Status": {"S": "pending"},                     # Default status
                "Transfer_Time": {"N": "0"},                             # Default transfer time (seconds)
                "FileURL": {"S": video_data.get("FileURL", "")},         # Video file URL
                "FinalDownloadURL": {"S": video_data.get("FinalDownloadURL", "")},           # Final download URL from metadata
                "Title": {"S": video_data.get("Title", "")},             # Video title
                "unique_title": {"S": video_data.get("unique_title", "")},  # Unique title field
                "Size_MB": {"N": str(size_mb)},                          # File size in MB
                "Duration_HMS": {"S": duration_hms},                     # Duration in h:m:s format
                "CateId": {"N": str(video_data.get("CateId", 0))},       # Category ID
                "CateName": {"S": video_data.get("CateName", "")},       # Category name
                "AppId": {"S": video_data.get("AppId", "")},             # Application ID
                "Status": {"S": video_data.get("Status", "")},           # Video status
                "ModifyTime": {"S": video_data.get("ModifyTime", "")},   # Last modified time
                "CreateTime": {"S": video_data.get("CreateTime", "")},   # Creation time
                "CoverURL": {"S": video_data.get("CoverURL", "")},       # Cover image URL
                "Snapshots": {"S": snapshots_json},                      # Snapshots (JSON string)
                "StorageLocation": {"S": video_data.get("StorageLocation", "")},  # Storage location
            }

            # Add the item to DynamoDB
            dynamodb_client.put_item(
                TableName=DYNAMODB_TABLE,
                Item=item
            )
            success_message = f"Uploaded metadata for video_id: {video_id}"
            print(success_message)

    except ClientError as e:
        error_message = f"ClientError: {e.response['Error']['Message']}"
        print(error_message)

    except Exception as e:
        error_message = f"Unexpected error: {str(e)}"
        print(error_message)

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

def download_and_transfer_video(download_url, video_metadata, local_folder="/tmp"):
    """
    Download a video from Ali VOD using its file URL, upload it to S3, and clean up locally.
    
    Args:
        file_url (str): The URL of the video file in Ali VOD.
        video_metadata (dict): Metadata of the video, including the Title.
        local_folder (str): The local folder to temporarily store the downloaded video.

    Returns:
        bool: True if the video is successfully transferred to S3; False otherwise.
    """
    # Safely extract video title
    video_title = video_metadata.get("unique_title", {}).get("S", "untitled").strip()
    print(f"Video Title: {video_title}")
    local_file_path = os.path.join(local_folder, video_title)
    s3_file_key = f"{VIDEO_BUCKET_FOLDER}/{video_title}"

    try:
        # Step 1: Download the video
        print(f"Downloading video '{video_title}' from Ali VOD...")
        with requests.get(download_url, stream=True) as response:
            response.raise_for_status()
            with open(local_file_path, "wb") as video_file:
                for chunk in response.iter_content(chunk_size=8192):  # Stream in 8 KB chunks
                    video_file.write(chunk)

        print(f"Download complete for '{video_title}'.")

        # Step 2: Upload the video to S3
        print(f"Uploading video '{video_title}' to S3...")
        s3_client.upload_file(
            local_file_path,
            AWS_VIDEO_BUCKET,
            s3_file_key
        )
        print(f"Video '{video_title}' successfully uploaded to S3.")

        # Step 3: Delete the local file
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Local file '{local_file_path}' deleted after upload.")

        return True

    except requests.exceptions.RequestException as e:
        print(f"Error downloading video '{video_title}': {e}")
        return False
    except Exception as e:
        print(f"Error uploading video '{video_title}' to S3: {e}")
        return False

def send_sns_notification(percentage=None, failed_video_id=None):
    """
    Sends an SNS notification for progress percentage.
    If failed_video_id is provided, sends a failure notification.
    """
    if failed_video_id:
        # Send failure notification with video ID
        subject = "URGENT: Video Transfer Failure"
        message = f"Video {failed_video_id} failed to transfer."
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        print(f"Failure SNS Notification sent: {message}")
    elif percentage is not None:
        # Send progress notification
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

def upload_failed_log_to_s3(log_file):
    """
    Upload the failed log file to the S3 bucket.
    """
    s3_client = boto3.client("s3")
    s3_key = f"{LOG_FOLDER}/{log_file}"
    
    try:
        s3_client.upload_file(log_file, AWS_LOG_BUCKET, s3_key)
        print(f"Uploaded failed log to S3: s3://{AWS_LOG_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload log to S3: {e}")