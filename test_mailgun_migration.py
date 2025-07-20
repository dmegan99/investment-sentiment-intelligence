#!/usr/bin/env python3
"""
Quick Mailgun Test for GitHub Environment
Tests the Mailgun integration with the exact same setup as interest_match.py
"""

import json
import requests
import logging
from google.cloud import secretmanager

# Configuration
PROJECT_ID = "smart-434318"

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_secret(secret_id, project_id=PROJECT_ID):
    """Fetch secret from Google Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_google_credentials():
    """Get Google credentials from Secret Manager"""
    service_account_key_json = get_secret('smart-service-account-key')
    credentials = json.loads(service_account_key_json)
    return credentials

def get_mailgun_api_key():
    """Get Mailgun API key from Secret Manager"""
    try:
        api_key = get_secret("MAILGUN_API_KEY")
        logger.info("✅ Successfully retrieved Mailgun API key from Secret Manager")
        return api_key
    except Exception as e:
        logger.error(f"❌ Error fetching MAILGUN_API_KEY from Secret Manager: {e}")
        return None

def get_mailgun_domain():
    """Get Mailgun domain from Secret Manager"""
    try:
        domain = get_secret("MAILGUN_DOMAIN")
        logger.info("✅ Successfully retrieved Mailgun domain from Secret Manager")
        return domain
    except Exception as e:
        logger.error(f"❌ Error fetching MAILGUN_DOMAIN from Secret Manager: {e}")
        return None

def test_mailgun_connection():
    """Test Mailgun API connection and send test email"""
    logger.info("🧪 Starting Mailgun connection test...")
    
    # Get credentials
    api_key = get_mailgun_api_key()
    domain = get_mailgun_domain()
    
    if not api_key or not domain:
        logger.error("❌ Missing Mailgun credentials")
        return False
    
    logger.info(f"📧 Using domain: {domain}")
    
    # Build API URL
    api_url = f"https://api.mailgun.net/v3/{domain}/messages"
    
    # Test email data
    from_email = f'Dave\'s News Intelligence - TEST <test@{domain}>'
    test_data = {
        'from': from_email,
        'to': 'dmegan@gmail.com',
        'subject': 'Mailgun Migration Test - Success!',
        'html': '''
        <html>
        <body>
        <h2>🎉 Mailgun Migration Successful!</h2>
        <p><strong>Your news intelligence pipeline is now using Mailgun!</strong></p>
        <ul>
        <li>✅ API connection working</li>
        <li>✅ Domain configured properly</li>
        <li>✅ Secrets retrieved from Google Secret Manager</li>
        <li>✅ Email sending functional</li>
        </ul>
        <p><em>This test email confirms your migration from SendGrid to Mailgun is complete.</em></p>
        <hr>
        <p><small>Sent from GitHub Actions - News Intelligence Pipeline</small></p>
        </body>
        </html>
        '''
    }
    
    try:
        logger.info("📤 Sending test email...")
        response = requests.post(
            api_url,
            auth=('api', api_key),
            data=test_data
        )
        
        if response.status_code == 200:
            logger.info("🎉 SUCCESS! Test email sent successfully!")
            logger.info(f"Response: {response.json()}")
            return True
        else:
            logger.error(f"❌ Failed to send email. Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Exception during email send: {e}")
        return False

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("MAILGUN MIGRATION TEST")
    logger.info("=" * 60)
    
    success = test_mailgun_connection()
    
    if success:
        logger.info("🎉 MAILGUN MIGRATION TEST: PASSED")
        logger.info("Your news pipeline is ready to use Mailgun!")
    else:
        logger.error("❌ MAILGUN MIGRATION TEST: FAILED")
        logger.error("Check the logs above for details.")
    
    logger.info("=" * 60)
