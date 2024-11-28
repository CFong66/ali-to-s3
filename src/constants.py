from s3_key_builders import (
    build_count_discrepancies_key,
    build_data_discrepancies_key,
    build_s3_key_backup_leads,
    build_mongo_backup_data_key
)

SECRET_NAME = "zohocrmmig"  
DATABASE = "zoho_crm"  
COLLECTION_NAME = "leads"  
UNIT = "count"  

NAMESPACE = "zohocrm_mongodb_migration"  
DIMENSION_NAME = "migrationproject"  
DIMENSION_VALUE = "zohotomongodb" 
REGION_NAME = 'ap-southeast-2'

CA_EC2_BUNDLE_PATH = "/home/ubuntu/etl/global-bundle.pem"
CA_LAMBDA_BUNDLE_PATH = "/tmp/global-bundle.pem"
CA_CERTIFICATE_BASE_URL = "https://truststore.pki.rds.amazonaws.com"
CA_CERTIFICATE_REGION = "global"

CLUSTER_IDENTIFIER = "docdb-cluster"
NUM_FETCH_DATA = 4000
S3_BUCKET_NAME = "zoho-mig-mgdb-cf-log"
STATUS_KEY = "etl_status/elt_status.JSON"

# S3 Keys generated dynamically
COUNT_DISCREPANCIES_KEY = build_count_discrepancies_key()
DATA_DISCREPANCIES_KEY = build_data_discrepancies_key()
S3_KEY_BACKUP_LEADS = build_s3_key_backup_leads()
MONGO_BACKUP_DATA_KEY = build_mongo_backup_data_key()

ZOHO_BASE_URL = "https://www.zohoapis.com.au/crm/v2/Leads"
ZOHO_CRM_CREDENTIAL = "zoho_crm_credentials"  
ZOHO_REFRESH_TOKEN = "ZOHO_REFRESH_TOKEN"
ZOHO_CLIENT_ID = "ZOHO_CLIENT_ID"
ZOHO_SECRET = "ZOHO_SECRET"


