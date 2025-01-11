import json
from constants import *
from config import *
from utils import *

def main():
    """
    Main workflow for video transfer preparation and execution.
    """
    # Step 1: Fetch and prepare metadata
    # print("Fetching metadata...")
    # metadata = fetch_all_metadata()
    
    # print("Saving metadata to local file...")
    # metadata_file = save_metadata_to_file(metadata, METADATA_LOCAL_PATH)
    
    print("Counting videos in metadata...")
    video_count = count_videos_in_file(METADATA_LOCAL_PATH)
    
    # print("Appending file URLs to metadata...")
    # append_file_urls_to_metadata(METADATA_LOCAL_PATH, video_count)
    
    # print("Creating final download URLs...")
    # update_video_metadata_with_final_urls(METADATA_LOCAL_PATH, FINAL_METADATA_LOCAL_PATH)
    
    print("Loading final metadata...")
    with open(METADATA_LOCAL_PATH, "r", encoding="utf-8") as f:
        updated_metadata = json.load(f)
    
    print("Saving metadata to S3...")
    save_metadata_to_s3(updated_metadata)
    
    print("Uploading metadata to DynamoDB...")
    upload_metadata_to_dynamodb(METADATA_LOCAL_PATH)
    print("Metadata upload to DynamoDB completed.")

    # Step 2: Start video transfer process
    print("Starting video transfer process...")
    transfer_videos(enable_notifications=True)

    # Step 3: Verify completion and retry failed videos
    while True:
        completed_videos = count_completed_videos_in_dynamodb()
        print(f"Completed videos: {completed_videos}/{video_count}")

        # Log completed video count to local file and upload to S3
        with open(COMPLETED_LOG_FILENAME, "a") as log_file:
            log_message = f"Completed videos: {completed_videos}/{video_count} - {get_melbourne_time()}\n"
            log_file.write(log_message)

        print(f"Progress logged. Check completed video count log in S3 for details.")

        if completed_videos == video_count:
            print("All videos transferred successfully.")
            send_sns_notification(f"Total {video_count} videos transferred successfully.")
            send_sqs_notification("Success", enable_notification=True)  # Send SQS notification on success
            
            # Final step: Upload completed log to S3 after all transfers are done
            upload_log_to_s3(COMPLETED_LOG_FILENAME, log_type="completed")
            print(f"Final completed video count uploaded to S3: {COMPLETED_LOG_FILENAME}")
            break

        else:
            print("Retrying failed videos...")
            retry_failed_videos()
    
    # Final status message
    print("Workflow completed.")

if __name__ == '__main__':
    main()