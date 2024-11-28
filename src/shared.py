import json
import boto3
from config import secrets_client

# Function to fetch Zoho CRM API secret from AWS Secrets Manager
def get_zoho_secret(secret_name):
    """
    Fetches the Zoho CRM API credentials from AWS Secrets Manager.
    
    Args:
        secret_name (str): The name of the secret containing Zoho CRM credentials.
        
    Returns:
        dict: The Zoho CRM credentials (e.g., refresh token, client ID, etc.).
    """
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])
