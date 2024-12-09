import time
from utils import *
from constants import *
from config import *

def transfer_videos(enable_notifications=True):
    """
    Transfer videos with pending status and retry failed ones.
    Sends SNS notifications at 10% increments and SQS notification upon completion or failure.
    Returns True if all videos are successfully transferred; False otherwise.
    """
    # Fetch metadata from OSS
    metadata = fetch_metadata_from_oss()

    # Save metadata to S3
    save_metadata_to_s3(metadata)

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

        # Track the start time of the transfer
        start_time = time.time()

        # Log system metrics (CPU, Memory, Disk usage) at the start of each transfer
        # log_to_cloudwatch('CPUUsage', psutil.cpu_percent(), video_path)
        # log_to_cloudwatch('MemoryUsage', psutil.virtual_memory().percent, video_path)
        # log_to_cloudwatch('DiskUsage', psutil.disk_usage('/').percent, video_path)

        success = download_video(video_path)

        # Track the end time after the transfer completes
        end_time = time.time()
        
        # Calculate transfer time
        transfer_time = f"{round(end_time - start_time, 2)}s"

        if success:
            completed_videos += 1
            update_video_status(video_path, 'completed', transfer_time)
            print(f"Transfer of video {video_path} completed successfully.")
        else:
            update_video_status(video_path, 'failed', transfer_time)
            print(f"Transfer of video {video_path} failed.")
            retries[video_path] = retries.get(video_path, 0) + 1
            if retries[video_path] > retry_limit:
                failed_videos.append(video)

        # Calculate progress
        progress = int((completed_videos / total_videos) * 100)

        # Send SNS notification at every 10% increment
        if progress >= progress_threshold:
            send_sns_notification(progress)
            progress_threshold += 10

        # Log transfer progress as a metric
        # log_to_cloudwatch('TransferProgress', progress, video_path)

        # Simulate delay (optional)
        time.sleep(0.5)  # Simulate delay for each video transfer

    # Retry failed videos
    for video in failed_videos[:]:
        video_path = video['video_id']['S']

        while retries.get(video_path, 0) <= retry_limit:
            print(f"Retrying video: {video_path}")

            # Track start time for retries
            start_time = time.time()
            success = download_video(video_path)
            
            # Track end time after retry completes
            end_time = time.time()
            
            # Calculate transfer time
            transfer_time = f"{round(end_time - start_time, 2)}s"
            
            if success:
                completed_videos += 1
                failed_videos.remove(video)
                update_video_status(video_path, 'completed',transfer_time)
                break

            retries[video_path] += 1

        if retries[video_path] > retry_limit:
            print(f"Failed to transfer video {video_path} after retries.")

    # Final notification
    if len(failed_videos) == 0:
        print("All videos transferred successfully.")
        send_sqs_notification("Success", enable_notification=enable_notifications)

    else:
        print("Transfer completed with failures.")
        send_sqs_notification("Failed after retries", enable_notification=enable_notifications)

    return len(failed_videos) == 0

if __name__ == '__main__':
    # Upload metadata to DynamoDB before starting the transfer process
    upload_metadata_to_dynamodb()

    # Start video transfer process with notifications enabled
    job_success = transfer_videos(enable_notifications=False)

    # Print result
    if job_success:
        print("All videos transferred successfully.")
    else:
        print("Some videos failed to transfer. Check the logs for details.")