import json
from constants import *
from config import *
from utils import *
from api import *
from filter import *

def main():
    """
    Main workflow for video transfer preparation and execution.
    Logs key steps and errors to a local debug log file for troubleshooting.
    """

    try:
        # Step 1: Fetch or load metadata
        log_debug("Loading metadata from local file...")
        try:
            with open(METADATA_LOCAL_PATH, "r", encoding="utf-8") as file:
                metadata = json.load(file)
        except FileNotFoundError as e:
            log_debug(f"Error: Local metadata file not found. {e}")
            return
        except json.JSONDecodeError as e:
            log_debug(f"Error: Failed to decode JSON from the local metadata file. {e}")
            return

        # Process metadata
        log_debug("Matching metadata against API results...")
        try:
            matched_metadata, matched_video_ids = fetch_all_docs_and_match(metadata)
            
        except Exception as e:
            log_debug(f"Error in matching metadata: {e}")
            return

        log_debug("Generating lesson IDs for videos...")
        try:
            object_key,s3_video_ids = process_video_ids(matched_video_ids)
            
        except Exception as e:
            log_debug(f"Error in generating lesson IDs: {e}")
            return

        log_debug("Saving updated metadata to local file...")
        try:
            metadata_file = save_metadata_to_file(matched_metadata, TEST_METADATA_LOCAL_PATH, object_key)
        except Exception as e:
            log_debug(f"Error in saving metadata: {e}")
            return

        log_debug("Counting videos in metadata...")
        try:
            video_count = count_videos_in_file(metadata_file)
            log_debug(f"Total videos to transfer: {video_count}")
        except Exception as e:
            log_debug(f"Error in counting videos: {e}")
            return

        # Upload metadata
        log_debug("Saving metadata to S3...")
        try:
            save_metadata_to_s3(matched_metadata)
        except Exception as e:
            log_debug(f"Error in saving metadata to S3: {e}")
            return

        log_debug("Uploading metadata to DynamoDB...")
        try:
            upload_metadata_to_dynamodb(TEST_METADATA_LOCAL_PATH)
            log_debug("Metadata upload to DynamoDB completed.")
        except Exception as e:
            log_debug(f"Error in uploading metadata to DynamoDB: {e}")
            return
        
        # Step 2: Start video transfer process
        log_debug("Starting video transfer process...")
        try:
            transfer_videos(enable_notifications=True)
        except Exception as e:
            log_debug(f"Error during video transfer process: {e}")
            return
        
        # Trigger transcoding for each video
        for s3_video_id in s3_video_ids:
            trigger_transcoding_api(s3_video_id)

    except Exception as e:
        log_debug(f"Unexpected error in main workflow: {e}")

if __name__ == "__main__":
    main()
