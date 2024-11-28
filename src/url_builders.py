from shared import get_zoho_secret
from constants import ZOHO_CRM_CREDENTIAL, ZOHO_BASE_URL, ZOHO_REFRESH_TOKEN, ZOHO_CLIENT_ID, ZOHO_SECRET, CA_CERTIFICATE_BASE_URL, CA_CERTIFICATE_REGION

# Build the token URL as a global variable
def build_access_token_url():
    """
    Dynamically constructs the token URL using constants and secrets.
    """
    credentials = get_zoho_secret(ZOHO_CRM_CREDENTIAL)
    return (
        f"{ZOHO_BASE_URL}/oauth/v2/token?"
        f"refresh_token={credentials[ZOHO_REFRESH_TOKEN]}&"
        f"client_id={credentials[ZOHO_CLIENT_ID]}&"
        f"client_secret={credentials[ZOHO_SECRET]}&"
        f"grant_type=refresh_token"
    )

def build_ca_certificate_url():
    return f"{CA_CERTIFICATE_BASE_URL}/{CA_CERTIFICATE_REGION}/global-bundle.pem"

# Function to construct the MongoDB URI
def build_mongo_uri(username, password, host, port, database, ca_bundle_path):
    """
    Constructs a MongoDB URI with the provided parameters.

    Args:
        username (str): MongoDB username.
        password (str): MongoDB password.
        host (str): MongoDB host address.
        port (str): MongoDB port.
        database (str): Database name.
        ca_bundle_path (str): Path to the CA certificate bundle for TLS.

    Returns:
        str: The constructed MongoDB URI.
    """
    return (
        f"mongodb://{username}:{password}@{host}:{port}/{database}?"
        f"tls=true&retryWrites=false&tlsCAFile={ca_bundle_path}"
    )
