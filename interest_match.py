# interest_match.py

import csv
import json
import logging
import time 
import requests
import sys
from datetime import datetime, timedelta
from pytz import timezone
from google.cloud import storage, secretmanager

# Import configuration settings
PROJECT_ID = "smart-434318"
RSS_BUCKET = "smart-434318-news-bbg-rss"
RSS_FILE = "news_bbg_rss.csv"
SENT_ARTICLES_FILE = "sent_articles.txt"

# FORCE PRINT TO ENSURE WE SEE OUTPUT
print("=" * 80)
print("🚀 INTEREST_MATCH.PY STARTING - FORCE PRINT TEST")
print("=" * 80)
sys.stdout.flush()

# Enhanced logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# Set up logging
logging.getLogger('google.cloud.storage').setLevel(logging.WARNING)

def debug_print(message):
    """Force print to both stdout and logger for maximum visibility"""
    print(f"🔍 DEBUG: {message}")
    sys.stdout.flush()
    logger.info(f"DEBUG: {message}")

# Function to fetch secrets from Google Secret Manager
def get_secret(secret_id, project_id=PROJECT_ID):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Mailgun configuration - now pulls domain from environment/secrets
def get_mailgun_domain():
    """Get Mailgun domain from Secret Manager"""
    try:
        domain = get_secret("MAILGUN_DOMAIN")
        debug_print(f"Successfully retrieved Mailgun domain: {domain}")
        return domain
    except Exception as e:
        debug_print(f"Error fetching MAILGUN_DOMAIN from Secret Manager: {e}")
        return None

# Fetch Google Application Credentials from Secret Manager
def get_google_credentials():
    service_account_key_json = get_secret('smart-service-account-key')
    credentials = json.loads(service_account_key_json)
    return credentials

# Initialize Google Cloud Storage client
def initialize_gcs_client():
    credentials = get_google_credentials()
    return storage.Client.from_service_account_info(credentials)

# Initialize Google Cloud Storage client
debug_print("Initializing Google Cloud Storage client...")
storage_client = initialize_gcs_client()
debug_print("Google Cloud Storage client initialized successfully")

def read_csv_from_gcs(bucket_name, blob_name):
    debug_print(f"Reading CSV from GCS: {bucket_name}/{blob_name}")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_text()
    csv_reader = csv.DictReader(content.splitlines())
    articles = list(csv_reader)
    debug_print(f"Successfully read {len(articles)} articles from CSV")
    return articles

def filter_articles_by_css(articles, threshold=0.615):
    debug_print(f"Filtering {len(articles)} articles with CSS threshold {threshold}")
    filtered_articles = []
    high_score_count = 0
    
    for article in articles:
        try:
            css_value = article.get('CSS')
            # Check for None explicitly before empty string
            if css_value is None or css_value == '':
                debug_print(f"Article with empty CSS value: {article.get('Title', 'Unknown Title')}")
                # Don't add articles with no CSS score to filtered list
            else:
                css_score = float(css_value)
                if css_score >= threshold:
                    filtered_articles.append((article, css_score))
                    high_score_count += 1
                    if high_score_count <= 5:  # Show first 5 high-scoring articles
                        debug_print(f"High CSS article {high_score_count}: {css_score:.3f} - {article.get('Title', '')[:50]}...")
        except (ValueError, KeyError) as e:
            debug_print(f"Invalid CSS value for article: {article.get('Title', 'Unknown Title')}. Error: {str(e)}")
    
    debug_print(f"Found {len(filtered_articles)} articles above CSS threshold {threshold}")
    return filtered_articles

def parse_date(date_string):
    """
    Parse date strings with enhanced support for TrendForce format
    """
    if not date_string:
        return None

    # Convert to UTC timezone if not already set
    utc = timezone('UTC')
    
    # Handle TrendForce special format
    try:
        date_str = date_string.strip()
        if "TrendForce" in date_str:
            # Extract the actual date part after "TrendForce"
            date_str = date_str.split("TrendForce", 1)[1].strip()
            # Parse with standard format
            dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=utc)
            return dt
    except (ValueError, IndexError) as e:
        debug_print(f"Error parsing TrendForce date: {e}")
    
    # Try regular date formats
    date_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ"
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_string, fmt)
            if parsed_date.tzinfo is None:
                parsed_date = parsed_date.replace(tzinfo=utc)
            return parsed_date
        except ValueError:
            continue
    
    # Try dateutil parser as fallback
    try:
        from dateutil import parser
        return parser.parse(date_string).replace(tzinfo=utc)
    except Exception:
        debug_print(f"Unable to parse date: {date_string}")
        return None

def is_within_last_48_hours(published_at):
    """
    Check if the article's publish date is within the last 48 hours,
    with special handling for TrendForce articles
    """
    if not published_at:
        return False
    
    parsed_date = parse_date(published_at)
    if not parsed_date:
        return False
    
    now = datetime.now(timezone('UTC'))
    time_diff = now - parsed_date
    
    # For TrendForce articles, use a more lenient window (72 hours)
    if "TrendForce" in published_at:
        return time_diff <= timedelta(hours=72)
    
    # Standard 48-hour window for other sources
    return time_diff <= timedelta(hours=48)

def get_sent_articles():
    debug_print("Checking for previously sent articles")
    bucket = storage_client.bucket(RSS_BUCKET)
    blob = bucket.blob(SENT_ARTICLES_FILE)
    if not blob.exists():
        debug_print("No previous sent articles file found")
        return set()
    content = blob.download_as_text()
    sent_articles = set(content.splitlines())
    debug_print(f"Found {len(sent_articles)} previously sent articles")
    return sent_articles

def update_sent_articles(new_articles):
    debug_print(f"Updating sent articles with {len(new_articles)} new URLs")
    bucket = storage_client.bucket(RSS_BUCKET)
    blob = bucket.blob(SENT_ARTICLES_FILE)
    current_sent = get_sent_articles()
    updated_sent = current_sent.union(new_articles)
    blob.upload_from_string('\n'.join(updated_sent))
    debug_print(f"Updated sent articles file. Total: {len(updated_sent)} articles")

# Fetch Mailgun API key from Secret Manager
def get_mailgun_api_key():
    try:
        api_key = get_secret("MAILGUN_API_KEY")
        debug_print(f"Successfully retrieved Mailgun API key (length: {len(api_key)})")
        return api_key
    except Exception as e:
        debug_print(f"Error fetching MAILGUN_API_KEY from Secret Manager: {e}")
        return None

def generate_email_content(articles):
    debug_print(f"Generating email content for {len(articles)} articles")
    html_content = """
    <html>
    <body>
    <h1>Matched Articles</h1>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%;">
    <tr style="background-color: #f2f2f2;">
        <th style="width: 10%;">CSS</th>
        <th style="width: 40%;">Article</th>
        <th style="width: 40%;">Short Summary</th>
        <th style="width: 10%;">Date</th>
    </tr>
    """
    
    for article, css_score in articles:
        source = article['Source']
        title = article['Title']
        url = article['URL']
        summary = article['Short_Summary']
        date = article['Published At']
        
        html_content += f"""
        <tr>
            <td>{css_score:.3f}</td>
            <td>{source}: <a href="{url}">{title}</a></td>
            <td>{summary}</td>
            <td>{date}</td>
        </tr>
        """
    
    html_content += """
    </table>
    </body>
    </html>
    """
    return html_content

def send_email_notification(matched_articles):
    debug_print(f"🚀 STARTING EMAIL NOTIFICATION for {len(matched_articles)} articles")
    
    mailgun_api_key = get_mailgun_api_key()
    mailgun_domain = get_mailgun_domain()

    if not mailgun_api_key:
        debug_print("❌ CRITICAL: Mailgun API key not found!")
        return False
        
    if not mailgun_domain:
        debug_print("❌ CRITICAL: Mailgun domain not found!")
        return False
    
    # Build the API URL dynamically
    mailgun_api_url = f"https://api.mailgun.net/v3/{mailgun_domain}/messages"
    debug_print(f"📧 Mailgun API URL: {mailgun_api_url}")

    # Use the sender name you specified
    from_email = f'Dave\'s News Intelligence <news@{mailgun_domain}>'
    to_emails = ['dmegan@gmail.com', 'dave.egan@columbiathreadneedle.com', 'egandave@icloud.com']
    
    ny_time = datetime.now(timezone('America/New_York'))
    timestamp = ny_time.strftime('%Y-%m-%d %I:%M %p')
    
    email_content = generate_email_content(matched_articles)
    debug_print(f"📧 From: {from_email}")
    debug_print(f"📧 Subject: Matches {timestamp}")
    debug_print(f"📧 Recipients: {to_emails}")

    email_sent = False
    for to_email in to_emails:
        # Prepare the data for Mailgun API
        data = {
            'from': from_email,
            'to': to_email,
            'subject': f'Matches {timestamp}',
            'html': email_content
        }

        try:
            debug_print(f"📤 Attempting to send email to {to_email}...")
            response = requests.post(
                mailgun_api_url,
                auth=('api', mailgun_api_key),
                data=data
            )
            
            debug_print(f"📧 Response status code: {response.status_code}")
            debug_print(f"📧 Response text: {response.text}")
            
            if response.status_code == 200:
                debug_print(f"✅ Email sent successfully to {to_email}")
                debug_print(f"📧 Mailgun response: {response.json()}")
                email_sent = True
            else:
                debug_print(f"❌ Failed to send email to {to_email}. Status: {response.status_code}")
                debug_print(f"❌ Response: {response.text}")
            
            time.sleep(1)  # Add 1 second delay between sends to avoid rate limiting
            
        except Exception as e:
            debug_print(f"❌ Exception sending email to {to_email}: {str(e)}")
            time.sleep(5)  # Add longer delay if there's an error

    return email_sent

def main():
    debug_print("🚀 STARTING article matching and email notification process")
    
    try:
        # Read and parse the CSV file
        debug_print("📖 Step 1: Reading articles from CSV")
        articles = read_csv_from_gcs(RSS_BUCKET, RSS_FILE)
        debug_print(f"📊 Read {len(articles)} total articles from CSV")

        # Filter articles based on CSS threshold
        debug_print("🔍 Step 2: Filtering articles by CSS threshold")
        css_filtered_articles = filter_articles_by_css(articles)
        debug_print(f"📊 Found {len(css_filtered_articles)} articles matching the CSS threshold")

        # Filter articles based on publication date and not previously sent
        debug_print("⏰ Step 3: Filtering by date and sent status")
        sent_articles = get_sent_articles()
        matched_articles = [
            (article, css_score) for article, css_score in css_filtered_articles
            if is_within_last_48_hours(article['Published At']) and article['URL'] not in sent_articles
        ]
        debug_print(f"📊 Found {len(matched_articles)} articles within last 48 hours and not previously sent")

        # Sort matched articles by CSS score (highest to lowest)
        matched_articles.sort(key=lambda x: x[1], reverse=True)

        if matched_articles:
            debug_print(f"📧 Step 4: Sending emails for {len(matched_articles)} matched articles")
            # Show top articles
            for i, (article, css_score) in enumerate(matched_articles[:5]):
                debug_print(f"📰 Top article {i+1}: {css_score:.3f} - {article.get('Title', '')[:60]}...")
            
            email_sent = send_email_notification(matched_articles)
            if email_sent:
                debug_print("✅ Emails sent successfully, updating sent articles list")
                update_sent_articles([article['URL'] for article, _ in matched_articles])
            else:
                debug_print("❌ No emails were sent successfully")
        else:
            debug_print("❌ No new matched articles found. Skipping email notification.")
            debug_print(f"📊 Summary: {len(articles)} total → {len(css_filtered_articles)} above threshold → {len(matched_articles)} final matches")

        debug_print("✅ Article matching and email notification process completed")
        
    except Exception as e:
        debug_print(f"💥 CRITICAL ERROR in main(): {str(e)}")
        import traceback
        debug_print(f"💥 Full traceback: {traceback.format_exc()}")
        sys.exit(1)

print("🔍 About to call main() function...")
sys.stdout.flush()

if __name__ == "__main__":
    main()

print("🔍 main() function completed")
sys.stdout.flush()
