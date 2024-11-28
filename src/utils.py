from datetime import datetime
import json
import logging
from pymongo import MongoClient
import pymongo
import requests
import time
import boto3
import hashlib
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError, ClientError
from config import s3_client, secrets_client, sns_client, ssm_client
from constants import *
from url_builders import *


def save_log_to_s3(stage=None, message=None, status="IN_PROGRESS", error_message=None, record=None):
    """
    Logs a message or error with details to S3, allowing for different stages, statuses, and error handling.
    Creates a log entry with details like timestamp, stage, status, message, or error, and uploads it to S3.

    Parameters:
        stage (str): Current stage of the ETL process, if applicable.
        message (str): Message describing the log entry, used for normal log entries.
        status (str): Status of the log entry (e.g., "IN_PROGRESS", "ERROR", etc.).
        error_message (str): Error message describing what went wrong, used for error entries.
        record (optional): Specific record associated with an error, if any.
    """
    # Create log entry with provided details
    log_entry = {
        "timestamp": str(datetime.now()),
        "status": status,
        "stage": stage,
        "message": message,
        "error_message": error_message,
        "record": record
    }
    
    # Set the log file name based on status and message/error content
    if status == "ERROR":
        brief_error = (error_message or "error").replace(" ", "_").replace("/", "_")[:20]
        s3_key = f"logs/{datetime.now().strftime('%d-%m-%Y')}/error_{brief_error}.json"
    else:
        brief_message = (message or "log").replace(" ", "_").replace("/", "_")[:20]
        s3_key = f"logs/{datetime.now().strftime('%d-%m-%Y')}/success_{brief_message}.json"
    
    # Log to CloudWatch if necessary (example; this part is disabled by default)
    logging.error(json.dumps(log_entry)) if status == "ERROR" else logging.info(json.dumps(log_entry))
    
    # Attempt to save the log entry to S3
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(log_entry),
            ContentType="application/json"
        )
    except NoCredentialsError as e:
        print("Credentials not available for S3: ", e)

# # Function to fetch Zoho CRM API token
def get_zoho_secret(secret_name):
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# Fetch the access token
def get_access_token():
    """
    Retrieves a new access token for Zoho CRM using stored credentials.
    """
    token_url = build_access_token_url()
    response = requests.post(token_url)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve access token: {response.text}")
    return response.json()["access_token"]

# Download CA certificate for MongoDB
def download_ca_certificate():
    """
    Downloads the CA certificate for Amazon DocumentDB and saves it locally.
    """
    # Construct the URL dynamically
    url = build_ca_certificate_url(region="global")
    
    # Download the certificate
    response = requests.get(url)
    if response.status_code == 200:
        with open(CA_LAMBDA_BUNDLE_PATH, 'wb') as f:
            f.write(response.content)
        print(f"CA certificate downloaded successfully to {CA_LAMBDA_BUNDLE_PATH}")
    else:
        raise Exception(f"Failed to download CA certificate. HTTP status: {response.status_code}")

# Get MongoDB credentials from Secrets Manager
def get_mongo_credentials():
    secret_name = SECRET_NAME
    response = secrets_client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret['username'], secret['password'], secret['host'], secret['port']

# Function to get the leads collection
def get_leads_collection():
    """
    Connects to MongoDB and retrieves the 'leads' collection. Creates it if it doesn't exist.

    Returns:
        pymongo.collection.Collection: The leads collection object.
    """
    # Get MongoDB credentials
    username, password, host, port = get_mongo_credentials()

    # Build the MongoDB URI
    mongo_uri = build_mongo_uri(
        username=username,
        password=password,
        host=host,
        port=port,
        database=DATABASE,
        ca_bundle_path=CA_EC2_BUNDLE_PATH,
    )

    # Connect to MongoDB
    client = pymongo.MongoClient(mongo_uri)

    # Access the database and the 'leads' collection
    db = client[DATABASE]
    collection_name = "leads"

    # Check if the 'leads' collection exists; if not, create it
    if collection_name not in db.list_collection_names():
        print(f"Creating collection '{collection_name}' in MongoDB.")
        db.create_collection(collection_name)
    else:
        print(f"Collection '{collection_name}' already exists in MongoDB.")

    # Return the collection object
    return db[collection_name]

# Retrieve leads from MongoDB
def get_mongo_leads():
    leads_collection = get_leads_collection()
    leads = list(leads_collection.find({}, {"_id": 0}))
    return {lead["Email"]: lead for lead in leads}

def calculate_md5(data):
    """Helper function to calculate MD5 checksum of a JSON-like data structure."""
    md5 = hashlib.md5()
    md5.update(json.dumps(data, sort_keys=True).encode('utf-8'))
    return md5.hexdigest()

# backup data loaded to MongoDB to S3 bucket
def backup_mongo_data_to_s3():
    """
    Backup MongoDB data (leads collection) to S3 bucket.
    """
    try:
        # Initialize the S3 client
        s3_client = boto3.client('s3')

        # Fetch leads data from MongoDB
        print("Fetching leads data from MongoDB...")
        leads_collection = get_leads_collection()
        mongo_leads = list(leads_collection.find({}, {"_id": 0}))  # Exclude the '_id' field from MongoDB
        print(f"Fetched {len(mongo_leads)} records from MongoDB.")

        # Convert leads data to JSON
        mongo_data_json = json.dumps(mongo_leads)

        # Upload JSON data to S3
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=MONGO_BACKUP_DATA_KEY,
            Body=mongo_data_json,
            ContentType='application/json'
        )
        print(f"MongoDB backup saved to S3: {MONGO_BACKUP_DATA_KEY}")

        # Log successful backup
        save_log_to_s3(
            stage="Backup",
            status="SUCCESS",
            message="MongoDB backup to S3 completed successfully",
            record={"s3_key": MONGO_BACKUP_DATA_KEY, "record_count": len(mongo_leads)}
        )
    except Exception as e:
        print("An error occurred while backing up MongoDB data to S3.")
        save_log_to_s3(
            stage="Backup",
            status="ERROR",
            message="MongoDB backup to S3 failed",
            error_message=str(e)
        )
        raise e

# compare backup data from zoho and mongodb
def compare_backup_data_from_s3(zoho_backup_data_key, mongo_backup_data_key):
    try:
        # Download Zoho backup data
        zoho_backup_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=zoho_backup_data_key)
        zoho_backup_data = json.loads(zoho_backup_obj['Body'].read().decode('utf-8'))

        # Download MongoDB backup data
        mongo_backup_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=mongo_backup_data_key)
        mongo_backup_data = json.loads(mongo_backup_obj['Body'].read().decode('utf-8'))

        # Step 1: Compare record counts
        zoho_data_count = len(zoho_backup_data)
        mongo_data_count = len(mongo_backup_data)
        print(f"Zoho backup data count: {zoho_data_count}")
        print(f"MongoDB backup data count: {mongo_data_count}")

        if zoho_data_count != mongo_data_count:
            print(f"Record counts do not match! Zoho: {zoho_data_count}, MongoDB: {mongo_data_count}")
            save_log_to_s3(
                stage="Validation",
                status="ERROR",
                message="Record count mismatch",
                record={
                    "zoho_count": zoho_data_count,
                    "mongo_count": mongo_data_count
                }
            )

        # Step 2: Compare checksums
        zoho_data_md5 = calculate_md5(zoho_backup_data)
        mongo_data_md5 = calculate_md5(mongo_backup_data)
        print(f"Zoho MD5: {zoho_data_md5}")
        print(f"MongoDB MD5: {mongo_data_md5}")

        if zoho_data_md5 != mongo_data_md5:
            print("Data integrity mismatch!")
            save_log_to_s3(
                stage="Validation",
                status="ERROR",
                message="Data integrity mismatch",
                record={
                    "zoho_md5": zoho_data_md5,
                    "mongo_md5": mongo_data_md5
                }
            )
        else:
            print("Data integrity match.")
            save_log_to_s3(
                stage="Validation",
                status="SUCCESS",
                message="Data integrity match",
                record={
                    "zoho_md5": zoho_data_md5,
                    "mongo_md5": mongo_data_md5
                }
            )

        # Step 3: Field-level comparison for discrepancies
        discrepancies = []
        required_fields = ["Last_Name", "First_Name", "Email", "Phone"]

        # Create dictionaries for efficient lookup by email
        mongo_data_dict = {record.get("Email", "").strip().lower(): record for record in mongo_backup_data}
        zoho_data_dict = {record.get("Email", "").strip().lower(): record for record in zoho_backup_data}

        # Check each Zoho record against MongoDB
        for email, zoho_record in zoho_data_dict.items():
            mongo_record = mongo_data_dict.get(email)

            if not mongo_record:
                discrepancies.append({
                    "Email": email,
                    "error": "Missing in MongoDB",
                    "zoho_record": zoho_record
                })
                continue

            for field in required_fields:
                zoho_value = zoho_record.get(field, "Not Found")
                mongo_value = mongo_record.get(field, "Not Found")
                if zoho_value != mongo_value:
                    discrepancies.append({
                        "Email": email,
                        "field": field,
                        "zoho_value": zoho_value,
                        "mongo_value": mongo_value
                    })

        # Log discrepancies
        if discrepancies:
            print(f"Discrepancies found: {len(discrepancies)}")
            s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=DATA_DISCREPANCIES_KEY, Body=json.dumps(discrepancies))
            save_log_to_s3(
                stage="Validation",
                status="ERROR",
                message="Data mismatch found",
                record={"discrepancies": discrepancies}
            )
        else:
            print("Data match.")
            save_log_to_s3(
                stage="Validation",
                status="SUCCESS",
                message="Backup data match"
            )

    except Exception as e:
        print(f"Error in comparing backup data: {str(e)}")
        save_log_to_s3(
            stage="Validation",
            status="ERROR",
            message="Exception during backup comparison",
            error_message=str(e)
        )
        raise

# Function to get the sns topic arn
def get_sns_topic_arn(parameter_name):
    try:
        response = ssm_client.get_parameter(Name=parameter_name)
        return response['Parameter']['Value']
    except ClientError as e:
        logging.error(f"Failed to retrieve SNS Topic ARN: {e}")
        return None

# Function to send a notification (email or SMS) to the team
def send_notification(message):
    topic_arn = get_sns_topic_arn('sns_topic_arn')  # Replace with your parameter name
    if not topic_arn:
        logging.error("SNS Topic ARN not found.")
        return

    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject='ETL Process Completed'
        )
        logging.info(f"Notification sent: {response}")
    except ClientError as e:
        logging.error(f"Failed to send notification: {e}")

def load_zoho_backup_data_from_s3():
    """Load the backup JSON data file from S3."""
    response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY_BACKUP_LEADS)
    return json.loads(response['Body'].read())

# function to validate the datas in both DB
def validate_data():
    try:
        # Validate backup data from S3
        print("Validating backup data from S3...")
        compare_backup_data_from_s3(
            zoho_backup_data_key=S3_KEY_BACKUP_LEADS,
            mongo_backup_data_key=MONGO_BACKUP_DATA_KEY
        )

    except Exception as e:
        print("An error occurred during validation.")
        save_log_to_s3(
            stage="Validation",
            status="ERROR",
            message="Exception during validation",
            error_message=str(e)
        )
        raise e
    
# Function to send metrics to cloudwatch
# def send_metrics_to_cloudwatch(
#     metric_name, 
#     value, 
#     unit=UNIT, 
#     namespace=NAMESPACE,
#     dimension_name=DIMENSION_NAME, 
#     dimension_value=DIMENSION_VALUE
# ):
#     """
#     Sends a custom metric to Amazon CloudWatch.

#     Parameters:
#     - metric_name (str): The name of the metric.
#     - value (float): The value of the metric.
#     - unit (str): The unit of the metric value (e.g., "Count", "Seconds").
#     - namespace (str): The CloudWatch namespace for grouping metrics.
#     - dimension_name (str): The name of the metric dimension.
#     - dimension_value (str): The value for the metric dimension.
#     """
#     try:
#         response = cloudwatch_client.put_metric_data(
#             Namespace = namespace,
#             MetricData=[
#                 {
#                     'MetricName': metric_name,
#                     'Dimensions': [
#                         {
#                             'Name': dimension_name,
#                             'Value': dimension_value
#                         }
#                     ],
#                     'Value': value,
#                     'Unit': unit
#                 },
#             ]
#         )
#         print(f"Metric {metric_name} sent to CloudWatch successfully:", response)
#     except ClientError as e:
#         print(f"Failed to send metric to CloudWatch: {e}")

# Check if ETL process should run
# def load_etl_status_from_s3():
#     try:
#         response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=STATUS_KEY)
#         status = json.loads(response['Body'].read().decode('utf-8'))
#     except s3_client.exceptions.NoSuchKey:
#         status = {"run_etl": True}
#     return status

# def update_etl_status_in_s3(run_etl):
#     """
#     Updates the status of the ETL process in S3 to indicate whether it is running or completed.
#     Stores the status of the ETL process in a specified S3 bucket and key to provide a record of ETL progress.

#     Parameters:
#         run_etl (bool): A boolean value indicating whether the ETL process is currently running.
#     """
#     status = {"run_etl": run_etl}
#     s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=STATUS_KEY, Body=json.dumps(status))