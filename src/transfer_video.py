import json
from constants import *
from config import *
from utils import *

def main():
    """
    Main workflow for video transfer preparation and execution.
    """
    # Step 1: Fetch and prepare metadata
    print("Fetching metadata...")
    metadata = fetch_all_metadata()
    
    print("Saving metadata to local file...")
    metadata_file = save_metadata_to_file(metadata, METADATA_LOCAL_PATH)
    
    print("Counting videos in metadata...")
    video_count = count_videos_in_file(metadata_file)
    
    print("Appending file URLs to metadata...")
    append_file_urls_to_metadata(METADATA_LOCAL_PATH, video_count)
    
    print("Creating final download URLs...")
    update_video_metadata_with_final_urls(METADATA_LOCAL_PATH, FINAL_METADATA_LOCAL_PATH)
    
    print("Loading final metadata...")
    with open(FINAL_METADATA_LOCAL_PATH, "r", encoding="utf-8") as f:
        updated_metadata = json.load(f)
    
    print("Saving metadata to S3...")
    save_metadata_to_s3(updated_metadata)
    
    print("Uploading metadata to DynamoDB...")
    upload_metadata_to_dynamodb(METADATA_LOCAL_PATH)
    print("Metadata upload to DynamoDB completed.")

    # Step 2: Start video transfer process
    print("Starting video transfer process...")
    job_success = transfer_videos(enable_notifications=True)

    # Final status message
    if job_success:
        print("All videos transferred successfully.")
    else:
        print("Some videos failed to transfer. Check the logs for details.")

if __name__ == '__main__':
    main()
