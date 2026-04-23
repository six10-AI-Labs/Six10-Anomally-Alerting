# alerting/email_sender.py
# Layer 5 — send alerts via Gmail API using OAuth 2.0 with automatic token refresh.

import base64
import re
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List

import config
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
TOKEN_FILE_CANDIDATES = [
    os.path.join(SCRIPT_DIR, 'gmail_token.json'),
    os.path.abspath(os.path.join(SCRIPT_DIR, '..', 'gmail_token.json')),
    os.path.join(PROJECT_ROOT, 'gmail_token.json'),
]


def validate_email(email: str) -> bool:
    """Basic check that a string looks like an email address."""
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email))


def load_oauth_token():
    """
    Load OAuth token from gmail_token.json.
    
    If token is expired, automatically refresh it using the refresh token.
    This allows the pipeline to run unattended without re-authentication.
    
    Returns:
        Credentials object, or None if token file doesn't exist.
    """
    SCOPES = ['https://www.googleapis.com/auth/gmail.send']
    token_file = next((path for path in TOKEN_FILE_CANDIDATES if os.path.exists(path)), None)

    if not token_file:
        return None
    
    try:
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        
        # If token is expired, refresh it automatically
        if creds.expired and creds.refresh_token:
            print("  [OAuth] Token expired, refreshing...")
            req = Request()
            creds.refresh(req)
            # Save refreshed token for future runs
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
            print("  [OAuth] Token refreshed and saved.")
        
        return creds
    except Exception as e:
        print(f"  Failed to load OAuth token: {e}")
        return None


def get_gmail_service():
    """
    Authenticate with Gmail API using OAuth 2.0.
    
    Returns:
        Gmail API service object authenticated with OAuth credentials.
        
    Raises:
        RuntimeError if OAuth token is not available.
    """
    SCOPES = ['https://www.googleapis.com/auth/gmail.send']
    
    try:
        creds = load_oauth_token()
        if not creds:
            raise RuntimeError(
                "OAuth token not found. Run this first to set up:\n"
                "  cd anomaly_alerting/alerting\n"
                "  python get_oauth_token.py\n"
                "Expected token locations:\n"
                f"  - {TOKEN_FILE_CANDIDATES[0]}\n"
                f"  - {TOKEN_FILE_CANDIDATES[1]}\n"
                "Then come back and run the pipeline."
            )
        
        service = build('gmail', 'v1', credentials=creds)
        return service
    except Exception as e:
        print(f"  Failed to authenticate with OAuth: {e}")
        raise


def send_email(recipients: List[str], subject: str, body: str,
               content_type: str = "html") -> bool:
    """
    Send the alert digest email via Gmail API using service account.

    Args:
        recipients: List of recipient email addresses.
        subject: Email subject line.
        body: Email body (HTML or plain text).
        content_type: "html" or "plain".

    Returns:
        True if sent successfully, False on error.
    """
    try:
        service = get_gmail_service()
        sender = config.GMAIL_CONFIG["sender_email"]

        message = MIMEMultipart()
        message["From"] = sender
        message["To"] = ", ".join(recipients)
        message["Subject"] = subject
        message.attach(MIMEText(body, content_type))

        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        message_body = {'raw': raw_message}

        service.users().messages().send(userId="me", body=message_body).execute()
        print(f"  Email sent successfully to: {', '.join(recipients)}")
        return True

    except HttpError as error:
        print(f"  Gmail API error: {error}")
        return False
    except Exception as e:
        print(f"  Email send error: {e}")
        return False


def log_send_result(success: bool, recipients: List[str], run_date: str) -> None:
    """Log whether the email was sent or failed."""
    recipient_str = ", ".join(recipients)
    if success:
        print(f"  [OK]   Alert digest sent -> {recipient_str}  ({run_date})")
    else:
        print(f"  [FAIL] Could not send alert digest → {recipient_str}  ({run_date})")
        print("         Check OAuth token/credentials setup and email settings in config.py.")
