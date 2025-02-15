import json
import time
import re
import os
import sys
import requests
from datetime import datetime, timedelta, timezone
from constants import *
from config import *
import logging
from aliyunsdkvod.request.v20170321 import GetVideoListRequest
from aliyunsdkvod.request.v20170321 import GetMezzanineInfoRequest
from botocore.exceptions import BotoCoreError, ClientError
from constants import API_URL

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
Updates the status of video processing to the front-end API.

Args:
    status (str): The current status of the video. Options: "uploading", "uploaded", "failed".

Returns:
    bool: True if the update was successful, False otherwise.
"""
def patch_to_api(API_URL, video_id, data, headers=None):
    try:
        # Sending a POST request to the API with the data and headers
        response = requests.patch(f"{API_URL}{video_id}", json=data, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            print(f"Frontend api was called successfully for {video_id}.")  # Parse and print JSON response if available
        else:
            print(f"Failed to call API. Status code: {response.status_code}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"Error occurred while calling frontend api: {e}")

# Update the video status for frontend
def update_video_status_frontend(video_id, status=''):
    # Update status in API
    payload = {
        "status": status
    }
    if status is not None:
        patch_to_api(API_URL, video_id, payload)

        
def generate_lesson_video_ids(video_id):
    """
    Generate lesson and video IDs based on the provided video ID.

    Args:
        video_id (str): The ID of the video.

    Returns:
        tuple: A formatted object key in the format "lesson/lessonid/videoid",
               or (None, None) if an error occurs.
    """
    api_url = f"{BASE_API_URL}/{video_id}"
    
    try:
        response = requests.post(api_url, timeout=10)  # Set a timeout for the request
    except requests.exceptions.RequestException as e:
        logger.error(f"Request Error for video ID {video_id}: {str(e)}")
        return None, None

    status_code = response.status_code
    
    if status_code == 200:
        try:
            data = response.json()
            lesson_id = data.get('lessonId')
            s3_video_id = data.get('videoId')
            if lesson_id and s3_video_id:
                object_key = f"lesson/{lesson_id}/{s3_video_id}"
                return object_key, s3_video_id, "success"
            else:
                logger.error(f"Missing lessonId or videoId in response for video ID {video_id}")
                return None, "error"
        except ValueError:
            logger.error(f"Invalid JSON response for video ID {video_id} with status 200")
            return None, "error"
        
    elif status_code == 201:
        try:
            data = response.json()
            logger.info(f"201 Created: Video ID {video_id} returned with response: {data}")
            # Handle specific cases if required (e.g., newly created resource)
            return None, "created"
        except ValueError:
            logger.error(f"Invalid JSON response for video ID {video_id} with status 201")
            return None, "error"

    elif status_code == 400:
        try:
            data = response.json()
            if data.get('message') == 'S3 video already exists':
                logger.info(f"S3 video already exists for video ID {video_id}")
                return get_existing_video_info(video_id)
            else:
                logger.error(f"400 Error for video ID {video_id}: {data.get('message', 'Unknown error')}")
                return None, "error"
        except ValueError:
            logger.error(f"Invalid JSON response for 400 status for video ID {video_id}")
            return None, "error"

    elif status_code == 404:
        try:
            data = response.json()
            message = data.get('message')
            logger.error(f"404 Error for video ID {video_id}: {message}")
            return None, "not_found"
        except ValueError:
            logger.error(f"Invalid JSON response for 404 status for video ID {video_id}")
            return None, "error"

    else:
        logger.error(f"Unexpected status code {status_code} for video ID {video_id}")
        return None, "error"

def get_existing_video_info(video_id):
    """
    Retrieve lesson and video IDs for an existing video and format them into an object key.

    Args:
        video_id (str): The ID of the existing video.

    Returns:
        tuple: A formatted object key in the format "lesson/lessonid/videoid",
               or (None, None) if an error occurs.
    """
    try:
        response = requests.get(f"{BASE_API_URL}/{video_id}", timeout=10)
        if response.status_code == 200:
            try:
                data = response.json()
                lesson_id = data.get('lessonId')
                s3_video_id = data.get('videoId')
                if lesson_id and s3_video_id:
                    object_key = f"lesson/{lesson_id}/{s3_video_id}"
                    return object_key, s3_video_id, "success"
                else:
                    logger.error(f"Missing lessonId or videoId in response for existing video ID {video_id}")
                    return None, "error"
            except ValueError:
                logger.error(f"Invalid JSON response for existing video ID {video_id}")
                return None, "error"
        logger.error(f"Unexpected status code {response.status_code} for existing video ID {video_id}")
        return None, "error"
    except requests.exceptions.RequestException as e:
        logger.error(f"Request Error in get_existing_video_info for video ID {video_id}: {str(e)}")
        return None, "error"

def process_video_ids(video_ids):
    """
    Process a list of video IDs by calling the API for each video ID.

    Args:
        video_ids (list): List of video IDs to process.

    Returns:
        dict: A mapping of video IDs to their corresponding object keys for successful cases.
    """
    object_keys = {}  # Dictionary to store successful object keys
    s3_video_ids = []

    for video_id in video_ids:
        logger.info(f"Processing video ID: {video_id}")
        object_key, s3_video_id, status = generate_lesson_video_ids(video_id)

        if status == "success":
            logger.info(f"Success: Video ID {video_id} found. Object Key: {object_key}")
            object_keys[video_id] = object_key  # Add to the mapping
            s3_video_ids.append(s3_video_id)
            
        elif status == "not_found":
            logger.warning(f"Not Found: Video ID {video_id} was not found on AliCloud.")
        elif status == "created":
            logger.info(f"Created: Video ID {video_id} resulted in a new resource creation.")
        else:
            logger.error(f"Error: Failed to process video ID {video_id}")

    return object_keys, s3_video_ids
