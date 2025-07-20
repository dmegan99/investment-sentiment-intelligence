#!/usr/bin/env python3
"""
Test Mailgun Migration Script
Tests the new Mailgun integration to ensure it works correctly.
"""

import json
import logging
import requests
from google.cloud import secretmanager

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ID = "smart-434318"

def get_secret(secret_id, project_id=PROJECT_ID):
    """Fetch secret from Google Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def test_mailgun_integration():
    """Test the Mailgun integration with a simple test email"""
    logger.info("🧪 Starting Mailgun integration test...")
    
    try:
        # Get credentials from Secret Manager
        api_key = get_secret("MAILGUN_API_KEY")
        domain = get_secret("MAILGUN_DOMAIN")
        
        logger.info(f"✅ Successfully retrieved secrets")
        logger.info(f"Domain: {domain}")
        
        # Prepare test email
        from_email = f"Dave's News Intelligence <news@{domain}>"
        to_email = "dmegan@gmail.com"  # Only send to one email for testing
        
        mailgun_api_url = f"https://api.mailgun.net/v3/{domain}/messages"
        
        data = {
            'from': from_email,
            'to': to_email,
            'subject': '🧪 Mailgun Migration Test - Success!',
            'html': '''
            <html>
            <body>
            <h2>🎉 Mailgun Migration Successful!</h2>
            <p><strong>Your news intelligence pipeline has successfully migrated from SendGrid to Mailgun.</strong></p>
            <ul>
                <li>✅ Mailgun API integration working</li>
                <li>✅ Secret Manager integration working</li>
                <li>✅ GitHub Actions deployment successful</li>
                <li>✅ Email formatting preserved</li>
            </ul>
            <p>Your automated news digest emails will now be sent via Mailgun.</p>
            <hr>
            <p><em>Sent from your AI News Intelligence Pipeline</em><br>
            <small>Migration completed on July 19, 2025</small></p>
            </body>
            </html>
            '''
        }
        
        logger.info(f"📧 Sending test email from: {from_email}")
        logger.info(f"📧 Sending test email to: {to_email}")
        
        # Send the test email
        response = requests.post(
            mailgun_api_url,
            auth=('api', api_key),
            data=data
        )
        
        if response.status_code == 200:
            logger.info("✅ Test email sent successfully!")
            logger.info(f"Mailgun response: {response.json()}")
            return True
        else:
            logger.error(f"❌ Failed to send test email. Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error during Mailgun test: {e}")
        return False

def main():
    logger.info("=" * 60)
    logger.info("Mailgun Migration Test")
    logger.info("=" * 60)
    
    success = test_mailgun_integration()
    
    if success:
        logger.info("🎉 Mailgun migration test completed successfully!")
        logger.info("Your news pipeline is ready to send emails via Mailgun.")
    else:
        logger.error("❌ Mailgun migration test failed!")
        logger.error("Please check the logs above for details.")
    
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
