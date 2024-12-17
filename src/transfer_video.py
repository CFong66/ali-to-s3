import time
from utils import *
from constants import *
from config import *

def transfer_videos(enable_notifications=True):
    """
    Transfer videos with pending status and retry failed ones.
    Sends SNS notifications at 10% increments and SQS notification upon completion or failure.
    Logs failed transfers in real time to S3.
    Returns True if all videos are successfully transferred; False otherwise.
    """

    with open(FAILED_LOG_FILENAME, "w") as log_file:
        log_file.write("Failed Videos Log\n")
        log_file.write("=================\n")

    # Fetch all metadata
    metadata = fetch_all_metadata()

    # Save the metadata (with logic to create unique file title) to local file
    metadata_file = save_metadata_to_file(metadata,METADATA_LOCAL_PATH)
    
    # Count videos in the metadata (saves the file locally in the process)
    video_count = count_videos_in_file(metadata_file)
    
    # Enrich metadata with file URLs
    append_file_urls_to_metadata(METADATA_LOCAL_PATH,video_count)

    # create final download url for the metadata
    update_video_metadata_with_final_urls(METADATA_LOCAL_PATH, FINAL_METADATA_LOCAL_PATH)
    
    # Open the file with utf-8 encoding
    with open(FINAL_METADATA_LOCAL_PATH, "r", encoding="utf-8") as f:
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

        # Track the start time of the transfer
        start_time = time.time()

        success = download_and_transfer_video(download_url, video, TEMP_VIDEO_LOCAL_PATH)

        # Track the end time after the transfer completes
        end_time = time.time()
        
        # Calculate transfer time
        transfer_time = f"{round(end_time - start_time, 2)}"

        if success:
            completed_videos += 1
            update_video_status(video_path, 'completed', transfer_time)
            print(f"Transfer of video {video_path} completed successfully.")

        else:
            update_video_status(video_path, 'failed', transfer_time)
            print(f"Transfer of video {video_path} failed.")

            # Send SNS notification for failure
            send_sns_notification(failed_video_id=video_path)

            retries[video_path] = retries.get(video_path, 0) + 1
            if retries[video_path] > retry_limit:
                failed_videos.append(video)

            # Log failure to local file and upload to S3
            with open(FAILED_LOG_FILENAME, "a") as log_file:
                log_message = f"Video {video_path} failed to transfer after {transfer_time}\n"
                log_file.write(log_message)
            upload_failed_log_to_s3(FAILED_LOG_FILENAME)
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

        if video_path not in retries:
            retries[video_path] = 0  # Initialize retries for this video

        while retries[video_path] <= retry_limit:
            print(f"Retrying video: {video_path}")

            # Track start and end times for retries
            start_time = time.time()
            success = download_and_transfer_video(download_url, video, TEMP_VIDEO_LOCAL_PATH)
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

            # Log final retry failure
            with open(FAILED_LOG_FILENAME, "a") as log_file:
                log_message = f"Retry failed for video {video_path}\n"
                log_file.write(log_message)
            upload_failed_log_to_s3(FAILED_LOG_FILENAME)
            print(f"Retry failed for video {video_path}. Check log in S3 for details.")


        # Final notification
    if len(failed_videos) > 0:
        print("Transfer completed with failures.")
        send_sqs_notification("Failed after retries", enable_notification=enable_notifications)
        return False  # Indicate that not all videos were successfully transferred

    else:
        print("All videos transferred successfully.")
        send_sqs_notification("Success", enable_notification=enable_notifications)
        return True  # Indicate success


if __name__ == '__main__':
    # Upload metadata to DynamoDB before starting the transfer process
    upload_metadata_to_dynamodb(METADATA_LOCAL_PATH)
    print("upload metadata to DB completed")

    # # Start video transfer process with notifications enabled
    # job_success = transfer_videos(enable_notifications=False)

    # # Print result
    # if job_success:
    #     print("All videos transferred successfully.")
    # else:
    #     print("Some videos failed to transfer. Check the logs for details.")