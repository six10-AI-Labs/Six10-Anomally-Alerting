# get_oauth_token.py
# One-time interactive OAuth token generation for Gmail API.
# Run this once to get a refresh token, then the pipeline uses it automatically.

import json
import os
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

SCOPES = ['https://www.googleapis.com/auth/gmail.send']
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
TOKEN_FILE = os.path.join(SCRIPT_DIR, 'gmail_token.json')
DEFAULT_CREDENTIALS_FILES = [
    os.path.join(SCRIPT_DIR, 'oauth_credentials.json'),
    os.path.join(SCRIPT_DIR, 'credentials_gmail.json'),
    os.path.join(PROJECT_ROOT, 'oauth_credentials.json'),
    os.path.join(PROJECT_ROOT, 'credentials_gmail.json'),
]


def _resolve_credentials_file():
    """
    Resolve OAuth client secret file from supported paths.
    """
    env_path = os.getenv("GMAIL_OAUTH_CREDENTIALS_FILE")
    if env_path:
        expanded = os.path.abspath(os.path.expanduser(env_path))
        if os.path.exists(expanded):
            return expanded
    for path in DEFAULT_CREDENTIALS_FILES:
        if os.path.exists(path):
            return path
    return None


def get_oauth_token():
    """
    Generate an OAuth refresh token interactively.
    
    Run this script once. It opens your browser, you sign in to Google,
    approve access, and the refresh token is saved to gmail_token.json.
    
    After this, the pipeline will use the token automatically.
    """
    creds = None

    # If token already exists, use it
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        if creds and not creds.expired:
            print(f"✓ Valid token already exists in {TOKEN_FILE}")
            return
        elif creds and creds.expired and creds.refresh_token:
            print(f"✓ Token exists but expired. Refreshing...")
            req = Request()
            creds.refresh(req)
            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
            print(f"✓ Token refreshed and saved to {TOKEN_FILE}")
            return

    # Generate new token
    credentials_file = _resolve_credentials_file()
    if not credentials_file:
        print("ERROR: OAuth credentials file not found!")
        print("Please download OAuth credentials from Google Cloud Console:")
        print("  1. Go to APIs & Services → Credentials")
        print("  2. Click 'Create Credentials' → 'OAuth 2.0 Client ID' → 'Desktop application'")
        print("  3. Download JSON and save as 'oauth_credentials.json' or 'credentials_gmail.json'")
        print("  4. Place it in the project root or anomaly_alerting/alerting/")
        print("  5. Run this script again")
        return

    flow = InstalledAppFlow.from_client_secrets_file(
        credentials_file, SCOPES)
    creds = flow.run_local_server(port=0)

    # Save the credentials for future runs
    with open(TOKEN_FILE, 'w') as token:
        token.write(creds.to_json())

    print(f"✓ OAuth token saved to {TOKEN_FILE}")
    print("✓ You can now run the pipeline normally — no more prompts needed!")


if __name__ == "__main__":
    get_oauth_token()
