import requests
from constants import *
from config import *

def fetch_all_docs_and_match(metadata):
    """
    Fetch all documents from the API and match them against provided metadata.

    Args:
        metadata (dict): Metadata fetched from the source.

    Returns:
        tuple: Matched metadata and a list of video IDs.
    """
    matched_video_ids = []
    matched_metadata = {}

    # Define the year and month for filtering
    year = 2021
    month = 6

    # Build the API URL with the updated query parameters
    api_url = f"https://uat-api.jiangren.com.au/videos/ali-cloud/valid-ids?year={year}&month={month}"

    try:
        # Make the API request
        response = requests.get(api_url)
        response.raise_for_status()

        # Parse the JSON response
        valid_video_ids = response.json()  # The response is a list of video IDs

        # Match valid video IDs with provided metadata
        for video_id in valid_video_ids:
            if video_id in metadata:
                matched_metadata[video_id] = metadata[video_id]
                matched_video_ids.append(video_id)

        print(f"Fetched {len(valid_video_ids)} valid video IDs.")
        print(f"Matched metadata count: {len(matched_metadata)}")

    except Exception as e:
        print(f"Error fetching or processing API response: {str(e)}")

    return matched_metadata, matched_video_ids