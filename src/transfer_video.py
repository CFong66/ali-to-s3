import time
from utils import *
from constants import *
from config import *


def transfer_videos(enable_notifications=True):
    """
    Transfer videos with pending status and retry failed ones.
    Returns True if all videos are successfully transferred; False otherwise.
    """
    # Fetch metadata from OSS
    metadata = fetch_metadata_from_oss()

    # Save metadata to S3
    save_metadata_to_s3(metadata)

    pending_videos = get_pending_videos()
    failed_videos = []

    # Notify that video transfer has started
    send_notifications(
        completed=False,
        enable_notifications=enable_notifications,
        message="Video transfer has started."
    )

    for video in pending_videos:
        video_path = video['video_id']['S']
        success = download_video(video_path)

        if success:
            update_video_status(video_path, 'completed')
            send_notifications(
                completed=False,
                enable_notifications=enable_notifications,
                message=f"Transfer of video {video_path} completed successfully."
            )
        else:
            update_video_status(video_path, 'failed')
            send_notifications(
                completed=False,
                enable_notifications=enable_notifications,
                message=f"Transfer of video {video_path} failed."
            )
            failed_videos.append(video)

        # Simulate progress (optional)
        time.sleep(2)  # Simulate delay for each video transfer

    # Retry failed videos if any
    retry_failed_videos(failed_videos)

    # Send completion notification
    if len(failed_videos) == 0:
        send_notifications(completed=True, enable_notifications=enable_notifications)
    else:
        send_notifications(
            completed=True,
            enable_notifications=enable_notifications,
            message="Video transfer completed with some failed videos."
        )

    # Return True if no videos remain in the failed_videos list
    return len(failed_videos) == 0


if __name__ == '__main__':
    # Upload metadata to DynamoDB before starting the transfer process
    upload_metadata_to_dynamodb()

    # Start video transfer process with notifications enabled
    job_success = transfer_videos(enable_notifications=True)

    # Print result and send notifications based on the result
    if job_success:
        print("All videos transferred successfully.")
    else:
        print("Some videos failed to transfer. Check the logs for details.")