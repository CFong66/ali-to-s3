import json
import time
import requests
from datetime import datetime
from utils import *
from constants import *
from config import *

# Fetch Zoho leads
def fetch_leads(max_records=10000):
    access_token = get_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    leads, page, per_page = [], 1, 200
    params = {"fields": "First_Name,Last_Name,Email,Phone,Company,Industry,Lead_Status", 
              "per_page": per_page}

    while len(leads) < max_records:
        params["page"] = page
        response = requests.get(ZOHO_BASE_URL, headers=headers, params=params)
        data = response.json()
        if 'data' in data:
            leads.extend(data['data'])
            # send_metrics_to_cloudwatch("RecordsProcessed", len(data["data"]))
            page += 1

            # Stop if max_records is reached
            if len(leads) >= max_records:
                leads = leads[:max_records]
                break

        # Check for rate limiting error
        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))  # Default retry after 60 seconds if header is missing
            print(f"Rate limit reached. Waiting for {retry_after} seconds before retrying...")
            time.sleep(retry_after)
            continue  # Retry the same page after waiting
        
        else:
            break

    # Save leads to S3
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY_BACKUP_LEADS, Body=json.dumps(leads))
    save_log_to_s3({
        "stage": "Extraction", 
        "timestamp": str(datetime.now()), 
        "record_count": len(leads), 
        "status": "Data fetched"})
    
    return leads

# Incremental load new data into MongoDB
def incremental_load(leads):
    existing_emails = get_mongo_leads().keys()
    new_leads = [lead for lead in leads if lead.get("Email") not in existing_emails]

    leads_collection = get_leads_collection()
    
    if new_leads:
        leads_collection.insert_many(new_leads)
        save_log_to_s3(
            stage="Incremental Load",
            status="SUCCESS",
            message=f"Inserted {len(new_leads)} new leads into DocumentDB",
            record={"inserted_leads_count": len(new_leads)}
        )
    else:
        save_log_to_s3(
            stage="Incremental Load",
            status="SUCCESS",
            message="No new leads to insert",
            record={"new_leads_count": 0}
        )

# Main entry point for the ETL process
def main():
    try:
        # Start of ETL
        print("ETL process started.")
        save_log_to_s3(
            stage="ETL Start", 
            status="IN_PROGRESS", 
            message="Starting ETL process"
        )

        # Step 1: Fetch data from Zoho CRM
        print("Fetching leads data from Zoho CRM...")
        leads = fetch_leads(NUM_FETCH_DATA)
        print(f"Fetched {len(leads)} records from Zoho CRM.")

        # Step 2: Perform incremental load to MongoDB
        print("Performing incremental load to MongoDB...")
        incremental_load(leads)
        print("Incremental load to MongoDB complete.")

        # Step 3: Backup MongoDB data to S3
        print("Backing up MongoDB data to S3...")
        backup_mongo_data_to_s3()
        print("MongoDB backup to S3 complete.")

        # Step 4: Validate data between Zoho and MongoDB backups
        print("Validating data between Zoho and MongoDB backups...")
        validate_data()
        print("Data validation complete.")

        # Step 5: Log ETL success
        save_log_to_s3(
            stage="ETL Success", 
            status="SUCCESS", 
            message="ETL process completed successfully"
        )
        print("ETL process completed successfully.")
    except Exception as e:
        # Log ETL failure
        print("An error occurred during the ETL process.")
        save_log_to_s3(
            stage="ETL Failure", 
            status="ERROR", 
            message="ETL process failed",
            error_message=str(e)
        )
        raise e


if __name__ == "__main__":
    main()