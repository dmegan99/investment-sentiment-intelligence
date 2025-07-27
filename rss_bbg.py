# rss_bbg.py (Part 1)

import feedparser
import pandas as pd
import requests
from dateutil import parser
from datetime import datetime, time, timedelta
from dateutil import tz
from email.utils import parsedate_to_datetime
from google.cloud import storage, secretmanager
from google.api_core import retry
from google.cloud.exceptions import GoogleCloudError
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from urllib.parse import urlparse
import json
import os
import logging
import re
import time
import warnings
import pytz
from atproto import Client, models
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Suppress the MarkupResemblesLocatorWarning
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TIMEZONE_INFO = {
    'EST': tz.gettz('America/New_York'),
    'EDT': tz.gettz('America/New_York'),
    'CST': tz.gettz('America/Chicago'),
    'CDT': tz.gettz('America/Chicago'),
    'MST': tz.gettz('America/Denver'),
    'MDT': tz.gettz('America/Denver'),
    'PST': tz.gettz('America/Los_Angeles'),
    'PDT': tz.gettz('America/Los_Angeles'),
}

# Project ID for Google Cloud
project_id = 'smart-434318'

# File to track downloaded articles
downloaded_articles_file = 'news_bbg_rss.csv'

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# YouTube channel IDs to monitor
YOUTUBE_CHANNELS = [
    ('Moore\'s Law Is Dead', 'UCRPdsCVuH53rcbTcEkuY4uQ'),
    ('Cognitive Revolution', 'UCjNRVMBVI30Sak_p6HRWhIA'),
    ('Weights & Biases', 'UCBp3w4DCEC64FZr4k9ROxig'),
    ('TechTechPotato', 'UC1r0DG-KEPyqOeW6o79PByw'),
    ('All-in Podcast', 'UCESLZhusAkFfsNsApnjF_Cg'),
    ('HBR', 'UCWo4IA01TXzBeGJJKWHOG9g'),
    ('No Priors', 'UCSI7h9hydQ40K5MJHnCrQvw'),
    ('BG2 Pod', 'UC-yRDvpR99LUc5l7i7jLzew'),
    ('Alex Ziskind', 'UCajiMK_CY9icRhLepS8_3ug'),
    ('A16Z', 'UC9cn0TuPq4dnbTY-CBsm8XA'),
    ('Nvidia', 'UCL-g3eGJi1omSDSz48AML-g'),
    ('Masters of Scale', 'UCiemDAS1bXMBTx3jIIOukFg'),
    ('Google DeepMind', 'UCP7jMXSY2xbc3KCAE0MHQ-A')
]

# List of RSS feeds
rss_feeds = [
    "https://feeds.bloomberg.com/technology/news.rss",
    "https://feeds.bloomberg.com/markets/news.rss",
    "https://feeds.bloomberg.com/economics/news.rss",
    "https://feeds.bloomberg.com/industries/news.rss",
    "https://feeds.bloomberg.com/green/news.rss",
    "https://feeds.bloomberg.com/bview/news.rss",
    "https://www.ft.com/news-feed?format=rss",
    "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "https://feeds.a.dj.com/rss/RSSWSJD.xml",
    "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
    "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
    "https://www.reutersagency.com/feed/?best-topics=tech&post_type=best",
    "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/EnergyEnvironment.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Economy.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Dealbook.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/MediaandAdvertising.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/PersonalTech.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Climate.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Space.xml",
    "https://www.scmp.com/rss/92/feed",
    "https://www.scmp.com/rss/10/feed",
    "https://www.scmp.com/rss/318421/feed",
    "https://www.scmp.com/rss/12/feed",
    "https://www.businesstimes.com.sg/rss/top-stories",
    "https://www.businesstimes.com.sg/rss/companies-markets",
    "https://www.businesstimes.com.sg/rss/technology",
    "https://www.economist.com/business/rss.xml",
    "https://www.economist.com/finance-and-economics/rss.xml",
    "https://www.economist.com/china/rss.xml",
    "https://www.economist.com/science-and-technology/rss.xml",
    "https://www.newyorker.com/feed/news",
    "https://www.theverge.com/rss/index.xml",
    "https://www.trendforce.com/feed/Semiconductors.html",
    "https://www.nextplatform.com/feed/",
    "https://www.theregister.com/off_prem/edge_iot/headlines.atom",
    "https://www.theregister.com/software/ai_ml/headlines.atom",
    "https://www.theregister.com/on_prem/hpc/headlines.atom",
    "https://www.theregister.com/on_prem/networks/headlines.atom",
    "https://www.theregister.com/off_prem/saas/headlines.atom",
    "https://www.theregister.com/off_prem/paas_iaas/headlines.atom",
    "https://www.theregister.com/off_prem/channel/headlines.atom",
    "https://www.tomshardware.com/feeds/all",
    "https://feeds.arstechnica.com/arstechnica/index",
    "https://www.newscientist.com/feed/home/",
    "https://www.wired.com/feed/rss",
    "https://techcrunch.com/feed/",
    "https://spectrum.ieee.org/rss",
    "https://www.scientificamerican.com/platform/syndication/rss/",
    "https://www.nature.com/nature.rss",
    "https://www.sciencedaily.com/rss/all.xml",
    "http://www.sciencemag.org/rss/current.xml",
    "https://www.digitimes.com/rss/daily.xml",
    "https://asia.nikkei.com/rss/feed/nar",
    "https://www.korea.net/rss/news.xml",  
    "https://en.yna.co.kr/RSS/economy-finance.xml",
    "https://hacker-news.firebaseio.com/v0/topstories.json"  # Hacker News API
]

def get_secret(secret_id, project_id=project_id):
    """Fetch secret from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_google_credentials():
    """Get Google Cloud credentials from service account key."""
    service_account_key_json = get_secret('smart-service-account-key')
    return json.loads(service_account_key_json)

def initialize_gcs_client():
    """Initialize Google Cloud Storage client."""
    credentials = get_google_credentials()
    return storage.Client.from_service_account_info(credentials)

def clean_text(text):
    """Clean and sanitize text content."""
    if text is None or isinstance(text, (int, float)):
        return ''
    if re.match(r'https?://|www\.|file://', text):
        return text
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, module='bs4')
        soup = BeautifulSoup(text, 'html.parser')
    cleaned_text = soup.get_text()
    return re.sub(r'[^\x00-\x7F]+', '', cleaned_text)

def clean_dataframe(df):
    """Clean and standardize DataFrame content."""
    df_cleaned = df.fillna('')
    for column in df_cleaned.columns:
        df_cleaned[column] = df_cleaned[column].astype(str)
    df_cleaned = df_cleaned.replace('nan', '', regex=True)
    return df_cleaned

def is_video_long_enough(duration):
    """Check if video duration meets minimum length requirement (>0 minute)."""
    try:
        # Extract minutes and hours from PT format
        minutes = int(re.search(r'(\d+)M', duration).group(1)) if 'M' in duration else 0
        hours = int(re.search(r'(\d+)H', duration).group(1)) if 'H' in duration else 0
        
        # Consider video long enough if > 1 minute
        return hours > 0 or minutes > 0
    except:
        # If we can't parse duration, default to including the video
        return True

def standardize_timestamp(entry):
    """
    Standardize timestamp with improved error handling and timezone support.
    """
    try:
        if isinstance(entry, (int, float)):
            return datetime.fromtimestamp(entry).strftime('%Y-%m-%d %H:%M:%S')
        
        if isinstance(entry, dict):
            timestamp = entry.get('published') or entry.get('updated') or entry.get('pubDate')
        else:
            timestamp = entry

        if not timestamp:
            logging.debug("Empty timestamp received")
            return ''

        # Log the timestamp we're trying to parse
        logging.debug(f"Attempting to parse timestamp: {timestamp}")

        # Handle different timestamp formats based on type
        if hasattr(timestamp, 'timetuple'):
            return time.strftime('%Y-%m-%d %H:%M:%S', timestamp.timetuple())

        # Try parsing with dateutil and timezone info
        try:
            dt = parser.parse(timestamp, tzinfos=TIMEZONE_INFO)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            pass

        # Try parsing email format
        try:
            dt = parsedate_to_datetime(timestamp)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            pass

        logging.error(f"Failed to parse timestamp: {timestamp}")
        return ''
    except Exception as e:
        logging.error(f"Error in standardize_timestamp: {str(e)}")
        return ''

def is_within_last_24_hours(timestamp_str):
    """Check if a timestamp is within the last 24 hours."""
    try:
        article_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        cutoff_time = datetime.now() - timedelta(hours=48)
        return article_time > cutoff_time
    except (ValueError, TypeError):
        logging.error(f"Error parsing timestamp: {timestamp_str}")
        return False

def extract_source_name(rss_url):
    """Extract clean source name from RSS URL."""
    if "ft.com" in rss_url:
        return "Financial Times"
    elif "dj.com" in rss_url:
        return "Wall Street Journal"
    elif "nytimes.com" in rss_url:
        return "New York Times"
    elif "bloomberg.com" in rss_url:
        try:
            feed_name = rss_url.split('/')[3]
            return f"Bloomberg {feed_name.capitalize()}"
        except IndexError:
            return "Bloomberg Unknown"
    elif "reuters.com" in rss_url or "reutersagency.com" in rss_url:
        return "Reuters"
    elif "techcrunch.com" in rss_url:
        return "TechCrunch"
    elif "theverge.com" in rss_url:
        return "The Verge"
    else:
        parsed_url = urlparse(rss_url)
        base_name = parsed_url.netloc.split('.')[-2]
        return base_name.replace('-', ' ').title()

def extract_from_rss(rss_url):
    """Extract articles from RSS feed."""
    feed = feedparser.parse(rss_url)
    articles = []
    
    logging.info(f"Processing feed: {rss_url}")
    
    for entry in feed.entries:
        try:
            title = clean_text(entry.get('title', ''))
            url = entry.get('link', '')
            published_at = standardize_timestamp(entry)
            
            # Skip if article is older than 24 hours
            if not is_within_last_24_hours(published_at):
                continue
            
            source = extract_source_name(rss_url)
            author = clean_text(entry.get('dc:creator', entry.get('author', '')))
            description = clean_text(entry.get('description', entry.get('summary', '')))
            content = clean_text(entry.get('content', [{'value': ''}])[0]['value'])
            
            # Generate a short summary
            short_summary = description if content == '' else f"{description} // {content}"
            
            article = {
                'Source': source,
                'Author': author,
                'Title': title,
                'Short_Summary': short_summary,
                'Description': description,
                'Content': content,
                'Published At': published_at,
                'URL': url
            }
            
            articles.append(article)
            
        except Exception as e:
            logging.error(f"Error processing entry from {rss_url}. Error: {str(e)}")
            continue
    
    if articles:
        logging.info(f"Found {len(articles)} new articles from {rss_url}")
    
    return articles

def load_existing_csv(bucket_name, blob_name):
    """Load existing CSV from GCS."""
    client = initialize_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if blob.exists():
        blob.download_to_filename(downloaded_articles_file)
        return pd.read_csv(downloaded_articles_file)
    else:
        logging.info(f"CSV file {blob_name} not found, creating a new one.")
        return pd.DataFrame(columns=['Source', 'Author', 'Title', 'Short_Summary', 'Description', 'Content', 'Published At', 'URL'])

def save_csv_to_gcs(df, bucket_name, blob_name, max_retries=5, base_delay=1):
    """Save updated CSV to GCS."""
    df_cleaned = clean_dataframe(df)
    df_cleaned.drop_duplicates(subset='URL', keep='first', inplace=True)
    df_cleaned.to_csv(downloaded_articles_file, index=False, na_rep='')
    
    client = initialize_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    @retry.Retry(predicate=retry.if_exception_type(GoogleCloudError, ConnectionError, TimeoutError))
    def upload_with_retry():
        blob.upload_from_filename(downloaded_articles_file, timeout=300)

    for attempt in range(max_retries):
        try:
            upload_with_retry()
            logging.info(f"Updated CSV uploaded to GCS bucket {bucket_name} as {blob_name}")
            return
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"Failed to upload CSV after {max_retries} attempts.")
                raise

def process_rss_feed(rss_feeds, bucket_name, blob_name, client, downloaded_articles_file):
    """Main function to process and update the CSV file."""
    logging.info(f"Starting processing with {len(rss_feeds)} RSS feeds")
    
    existing_df = load_existing_csv(bucket_name, blob_name)
    all_new_articles = []

    # Process RSS feeds
    for idx, rss_url in enumerate(rss_feeds, 1):
        try:
            logging.info(f"Processing RSS feed {idx}/{len(rss_feeds)}: {rss_url}")
            new_articles = extract_from_rss(rss_url)
            
            if new_articles:
                # Check for duplicates
                filtered_articles = []
                for article in new_articles:
                    if existing_df[existing_df['URL'] == article['URL']].empty:
                        filtered_articles.append(article)
                
                if filtered_articles:
                    all_new_articles.extend(filtered_articles)
                    logging.info(f"Successfully added {len(filtered_articles)} new articles from {rss_url}")
                else:
                    logging.info(f"All {len(new_articles)} articles from {rss_url} were duplicates")
            else:
                logging.info(f"No articles found from {rss_url}")
            
        except Exception as e:
            logging.error(f"Error processing feed {rss_url}: {str(e)}")
            continue

    if all_new_articles:
        # Create new DataFrame with only new articles
        new_df = pd.DataFrame(all_new_articles)
        
        # Concatenate with existing DataFrame
        updated_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Save updated DataFrame
        save_csv_to_gcs(updated_df, bucket_name, blob_name)
        logging.info(f"Added {len(all_new_articles)} new articles to {blob_name}")
    else:
        logging.info("No new articles found")

if __name__ == "__main__":
    bucket_name = "smart-434318-news-bbg-rss"
    blob_name = "news_bbg_rss.csv"
    client = initialize_gcs_client()
    downloaded_articles_file = 'news_bbg_rss.csv'

    try:
        process_rss_feed(rss_feeds, bucket_name, blob_name, client, downloaded_articles_file)
    except Exception as e:
        logging.error(f"An error occurred during script execution: {str(e)}")
