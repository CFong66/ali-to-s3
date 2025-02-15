import json
import time
import re
import os
import sys
import requests
import hvac
from datetime import datetime, timedelta, timezone
from api import update_video_status_frontend
from constants import *
from config import *
import logging
from aliyunsdkvod.request.v20170321 import GetVideoListRequest
from aliyunsdkvod.request.v20170321 import GetMezzanineInfoRequest
from botocore.exceptions import BotoCoreError, ClientError


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transfer_videos(enable_notifications=False):
    """
    Transfer videos with pending status and retry failed ones.
    Sends SNS notifications at 10% increments and SQS notification upon completion or failure.
    Logs failed transfers in real time to S3.
    Returns True if all videos are successfully transferred; False otherwise.
    """

    with open(FAILED_LOG_FILENAME, "w") as log_file:
        log_file.write("Failed Videos Log\n")
        log_file.write("=================\n")

    # Open the file with utf-8 encoding
    with open(METADATA_LOCAL_PATH, "r", encoding="utf-8") as f:
        updated_metadata = json.load(f)

    # Save the metadata to S3, this will ensure Chinese characters are preserved in the final output
    save_metadata_to_s3(updated_metadata)

    # get the pending videos from DynamoDB
    pending_videos = get_pending_videos()
    failed_videos = []
    retries = {}
    retry_limit = 5
    total_videos = len(pending_videos)
    completed_videos = 0
    progress_threshold = 10  # Start at 10%

    # Notify that video transfer has started
    send_sns_notification(percentage=0)  # Notify the start of the process

    for video in pending_videos:
        video_path = video['video_id']['S']
        download_url = video['FinalDownloadURL']['S']
        object_key = video.get("ObjectKey", {}).get("S", "")
        s3_video_id = object_key.split('/')[-1]

        # Track the start time of the transfer
        start_time = time.time()
        
        update_video_status_frontend(s3_video_id, 'uploading')
        success = download_and_transfer_video(download_url, video, object_key, TEMP_VIDEO_LOCAL_PATH)

        # Track the end time after the transfer completes
        end_time = time.time()
        
        # Calculate transfer time
        transfer_time = f"{round(end_time - start_time, 2)}"

        if success:
            completed_videos += 1
            update_video_status(video_path, 'completed', transfer_time)
            update_video_status_frontend(s3_video_id, 'uploaded')
            print(f"Transfer of video {s3_video_id} completed successfully.")

        else:
            update_video_status(video_path, 'failed', transfer_time)
            update_video_status_frontend(s3_video_id, 'upload_failed')

            print(f"Transfer of video {s3_video_id} failed.")

            # Send SNS notification for failure
            send_sns_notification(failed_video_id=video_path)

            retries[video_path] = retries.get(video_path, 0) + 1
            if retries[video_path] > retry_limit:
                failed_videos.append(video)

            # Log failure to local file and upload to S3
            with open(FAILED_LOG_FILENAME, "a") as log_file:
                log_message = f"Video {video_path} failed to transfer after {transfer_time}\n"
                log_file.write(log_message)
            upload_log_to_s3(FAILED_LOG_FILENAME, log_type="failed")
            print(f"Video {video_path} failed to transfer. Check log in S3 for details.")


        # Calculate progress
        progress = int((completed_videos / total_videos) * 100)

        # Send SNS notification at every 10% increment
        if progress >= progress_threshold:
            send_sns_notification(progress)
            progress_threshold += 10

        # Simulate delay (optional)
        time.sleep(0.5)  # Simulate delay for each video transfer

    # Retry failed videos
    for video in failed_videos[:]:
        video_path = video['video_id']['S']
        download_url = video['FinalDownloadURL']['S']
        object_key = video.get("ObjectKey", {}).get("S", "")

        if video_path not in retries:
            retries[video_path] = 0  # Initialize retries for this video

        while retries[video_path] <= retry_limit:
            print(f"Retrying video: {video_path}")

            # Track start and end times for retries
            start_time = time.time()
            success = download_and_transfer_video(download_url, video, object_key, TEMP_VIDEO_LOCAL_PATH)
            end_time = time.time()

            # Calculate transfer time
            transfer_time = f"{round(end_time - start_time, 2)}"

            if success:
                completed_videos += 1
                failed_videos.remove(video)  # Remove from failed_videos on success
                update_video_status(video_path, 'completed', transfer_time)
                break

            retries[video_path] += 1

        # If retries exceeded the limit, keep in failed_videos
        if retries[video_path] > retry_limit:
            print(f"Failed to transfer video {video_path} after retries.")

            # Log failure to local file and upload to S3
            with open(FAILED_LOG_FILENAME, "a") as log_file:
                log_message = f"Video {video_path} failed to transfer after {transfer_time}\n"
                log_file.write(log_message)
            upload_log_to_s3(FAILED_LOG_FILENAME, log_type="failed")
            print(f"Video {video_path} failed to transfer. Check log in S3 for details.")


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

def save_metadata_to_file(metadata, file_path, object_keys):
    """
    Save metadata to a local file with unique title renaming logic.
    The naming convention for S3 key and file names is: 
    Title_CreationTime_OrderingNumber (if duplicates exist).
    All special characters are replaced with underscores for smoother file names.
    """
    try:
        # A dictionary to track duplicate titles
        title_tracker = {}

        # Iterate over each video ID and its details
        for video_id, video in metadata.items():
            # Extract title, creation time, and initialize ordering number
            video_title = video.get("Title", "untitled")
            creation_time = video.get("CreateTime", "unknown")

            # Replace all spaces, dashes, colons, and other special characters with underscores
            cleaned_title = re.sub(r"[^a-zA-Z0-9\u4e00-\u9fff]+", "_", video_title)

            # Format creation time for readability and replace special characters with underscores
            try:
                formatted_creation_time = datetime.strptime(creation_time, '%Y-%m-%d %H:%M:%S').strftime('%Y_%m_%dT%H_%M_%S')
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
            video["unique_title"] = unique_key + ".mp4"  # Append .mp4 for clarity

            # Add the object key if available
            object_key = object_keys.get(video_id)
            if object_key:
                video["object_key"] = object_key

            # Ensure the metadata is updated
            metadata[video_id] = video

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
    Includes a progress tracker and sends SNS notifications at 20% increments.
    """
    try:
        # Load metadata from file
        with open(file_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        appended_count = 0
        progress_threshold = 20  # Start at 20% increment
        failed_videos = []

        # Notify start of process
        send_sns_notification(percentage=0)
        print("Progress Notification Sent: 0%")

        # Update each video with its file URL
        for index, (video_id, video_metadata) in enumerate(metadata.items(), start=1):
            file_url = fetch_mezzanine_info(video_id)
            if file_url:
                video_metadata["FileURL"] = file_url
                appended_count += 1
                print(f"Appended {appended_count}/{total_metadata_count} FileURL {file_url} for VideoId {video_id}.")
            else:
                failed_videos.append(video_id)
                print(f"Failed to fetch FileURL for VideoId {video_id}. Progress: {index}/{total_metadata_count}")
                send_sns_notification(failed_video_id=video_id)

            # Track progress and send SNS notification at 20% increments
            progress_percentage = (index / total_metadata_count) * 100
            if progress_percentage >= progress_threshold:
                send_sns_notification(percentage=progress_threshold)
                print(f"Progress Notification Sent: {progress_threshold}%")
                progress_threshold += 20  # Next threshold

        # Save the updated metadata back to the original file
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        print(f"Updated metadata saved to {file_path}. Total appended: {appended_count}/{total_metadata_count}")

        # Send completion notification
        if not failed_videos:
            send_sns_notification(percentage=100)
            print("Progress Notification Sent: 100% - All URLs appended successfully.")
        else:
            send_sns_notification(percentage=100)
            send_sns_notification(failed_video_id=failed_videos)  # Notify about all failed videos
            print(f"Progress Notification Sent: 100% - {len(failed_videos)} failures logged.")

    except FileNotFoundError:
        print("Metadata file not found.")
        send_sns_notification(subject="Metadata Update Failed", message="Error: Metadata file not found.")

    except json.JSONDecodeError:
        print("Error decoding JSON file.")
        send_sns_notification(subject="Metadata Update Failed", message="Error: Failed to decode metadata JSON file.")

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
            
            # Include the object key from metadata
            object_key = video_data.get("object_key", "")

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
                "ObjectKey": {"S": object_key},                          # Object key
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
    # Base update expression and attributes
    update_expression = 'SET #Transfer_Status = :status'
    expression_attribute_values = {':status': {'S': status}}
    expression_attribute_names = {"#Transfer_Status": "Transfer_Status"}

    # Conditionally add transfer time update
    if transfer_time is not None:
        update_expression += ', #Transfer_Time = :transfer_time'
        expression_attribute_values[':transfer_time'] = {'N': str(transfer_time)}
        expression_attribute_names["#Transfer_Time"] = "Transfer_Time"

    # Update the item in DynamoDB
    dynamodb_client.update_item(
        TableName=DYNAMODB_TABLE,
        Key={'video_id': {'S': video_id}},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )

def download_and_transfer_video(download_url, video_metadata, object_key, local_folder="/tmp"):
    """
    Download a video from Ali VOD using its metadata, upload it to S3 with tagging, and clean up locally.
    
    Args:
        video_metadata (dict): Metadata of the video, including VideoId, Title, Size, and CreationTime.
        local_folder (str): The local folder to temporarily store the downloaded video.

    Returns:
        bool: True if the video is successfully transferred to S3; False otherwise.
    """
    # Extract relevant metadata fields
    video_id_raw = video_metadata.get("video_id", {"S": "unknown_id"})
    video_id = video_id_raw.get("S", "unknown_id") if isinstance(video_id_raw, dict) else video_id_raw

    title_raw = video_metadata.get("Title", {"S": "Untitled"})
    title = title_raw.get("S", "Untitled") if isinstance(title_raw, dict) else title_raw

    size_raw = video_metadata.get("Size_MB", {"N": "0"})  # Assuming size may be numeric
    size = float(size_raw.get("N", 0)) if isinstance(size_raw, dict) else float(size_raw)

    creation_time_raw = video_metadata.get("CreateTime", "1970-01-01T00:00:00Z")
    if isinstance(creation_time_raw, dict):
        creation_time_str = creation_time_raw.get("S", "1970-01-01T00:00:00Z")
    else:
        creation_time_str = creation_time_raw
    creation_time = datetime.strptime(creation_time_str, "%Y-%m-%d %H:%M:%S")

    download_url_raw = video_metadata.get("FinalDownloadURL", "unknown_url")
    if isinstance(download_url_raw, dict):  # Handle DynamoDB format
        download_url = download_url_raw.get("S", "unknown_url")
    else:
        download_url = download_url_raw
    
    file_extension = ".mp4"

    # Append the file extension to the video ID
    s3_file_key = f"{object_key}{file_extension}"
    
    local_file_path = os.path.join(local_folder, video_id)

    try:
        # Step 1: Download the video
        print(f"Downloading video '{video_id}' from Ali VOD...")
        with requests.get(download_url, stream=True) as response:
            response.raise_for_status()
            with open(local_file_path, "wb") as video_file:
                for chunk in response.iter_content(chunk_size=8192):  # Stream in 8 KB chunks
                    video_file.write(chunk)

        print(f"Download complete for '{video_id}'.")

        # Step 2: Upload the video to S3 with tags
        print(f"Uploading video '{video_id}' to S3...")
        s3_client.upload_file(
            local_file_path,
            AWS_VIDEO_BUCKET,
            s3_file_key
        )

        # Add tags to the uploaded file
        tags = [
            {"Key": "Title", "Value": str(title)},
            {"Key": "Size_MB", "Value": str(size)},
            {"Key": "CreateTime", "Value": str(creation_time_str)},
            {"Key": "Video_Type", "Value":"AliCloud_Video"},
        ]

        # Define invalid characters for S3 tag values
        invalid_characters = "&<>\\"
        control_characters = ''.join(chr(i) for i in range(32)) + chr(127)  # ASCII 0-31 and 127

        # Combine all invalid characters
        all_invalid_characters = set(invalid_characters + control_characters)

        # Sanitize tag values
        for tag in tags:
            # Truncate tag values to a maximum of 128 characters
            tag["Value"] = tag["Value"][:128]
            
            # Replace invalid characters with underscores
            tag["Value"] = "".join(
                char if char not in all_invalid_characters else "_" for char in tag["Value"]
            ).strip()

            # Replace spaces and invalid characters with underscores
            tag["Value"] = re.sub(r"[^\w\-_.:]", "_", tag["Value"])  # Allows alphanumeric, `_`, `-`, `.`, `:`

            
            # Ensure tag values are not empty after sanitization
            if not tag["Value"]:
                tag["Value"] = "default_value"

        try:
            s3_client.put_object_tagging(
                Bucket=AWS_VIDEO_BUCKET,
                Key=s3_file_key,
                Tagging={"TagSet": tags}
            )
            print(f"Video '{video_id}' successfully uploaded and tagged in S3.")
        except Exception as e:
            print(f"Error tagging video '{video_id}': {e}")
            # Optionally log this error or mark the video for review

        # Step 3: Delete the local file
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Local file '{local_file_path}' deleted after upload.")

        return True

    except requests.exceptions.RequestException as e:
        print(f"Error downloading video '{video_id}': {e}")
        return False
    except Exception as e:
        print(f"Error uploading video '{video_id}' to S3: {e}")
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

def upload_log_to_s3(log_file, log_type="failed"):
    """
    Upload a log file to S3. It can be used for failure logs or completed video count logs.
    Args:
        log_file (str): Path to the log file.
        log_type (str): Type of log ("failed" or "completed"). Determines the S3 folder structure.
    """
    s3_client = boto3.client("s3")
    
    # Use different S3 folders for different log types
    s3_key = f"{LOG_FOLDER}/{log_type}/{os.path.basename(log_file)}"
    
    try:
        s3_client.upload_file(log_file, AWS_LOG_BUCKET, s3_key)
        print(f"Uploaded {log_type} log to S3: s3://{AWS_LOG_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {log_type} log to S3: {e}")

# Define Melbourne timezone
MELBOURNE_TZ = timezone(timedelta(hours=11))

def get_melbourne_time() -> str:
    """
    Get the current time in Melbourne local timezone (UTC+11 during daylight saving).

    Returns:
        str: The formatted time string (e.g., "Sat Dec 21 05:05:34 2024").
    """
    return datetime.now(MELBOURNE_TZ).strftime("%a %b %d %H:%M:%S %Y")

def log_debug(message):
    """Helper function to log debug messages."""
    with open(LOG_DEBUG_FILE, "a") as log:
        log.write(f"{get_melbourne_time()} - {message}\n")
    print(message)  # Also print to console for real-time updates

def get_api_token():
    client = hvac.Client(
        vault_url= VAULT_URL,
        token=os.getenv('VAULT_TOKEN')
    )
    
    try:
        secret = client.secrets.kv.v2.read_secret_version(
            path='alicloud-sync/uat',  
            mount_point='secret'
        )
        
        return secret['data']['data']['internal_secret']  
    except Exception as e:
        log_debug(f"Error fetching secret from Vault: {e}")
        raise

def trigger_transcoding_api(video_id):
            api_url = TRIGGER_TRANSCODING_API.format(video_id=video_id)
            api_token = ""

            headers = {
                "internal_secret": api_token,
                "Content-Type": "application/json"
            }

            try:
                response = requests.patch(api_url, headers=headers)
                if response.status_code == 200 and response.json().get('success'):
                    log_debug(f"Transcoding triggered successfully for video ID: {video_id}")
                else:
                    log_debug(f"Failed to trigger transcoding for video ID: {video_id}. Response: {response.text}")
            except Exception as e:
                log_debug(f"Error calling transcoding API for video ID: {video_id}: {e}")
