from datetime import datetime

def build_count_discrepancies_key(date=None):
    """Generate the S3 key for count discrepancies."""
    date = date or datetime.now()
    return f"count/count-discrepancies-{date.strftime('%d-%m-%Y')}.json"

def build_data_discrepancies_key(date=None):
    """Generate the S3 key for data discrepancies."""
    date = date or datetime.now()
    return f"data_disrepancies/discrepancies-{date.strftime('%d-%m-%Y')}.json"

def build_s3_key_backup_leads(date=None):
    """Generate the S3 key for Zoho leads backup."""
    date = date or datetime.now()
    return f"zoho-backup/leads-{date.strftime('%d-%m-%Y')}.json"

def build_mongo_backup_data_key(date=None):
    """Generate the S3 key for MongoDB leads backup."""
    date = date or datetime.now()
    return f"mongo-backup/mongo-leads-backup-{date.strftime('%d-%m-%Y')}.json"