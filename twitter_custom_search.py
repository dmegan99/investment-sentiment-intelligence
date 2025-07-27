# twitter_custom_search.py

import os
import logging
import sys
import csv
import io
import json
import requests
import re
import difflib
from google.cloud import firestore, storage, secretmanager
from datetime import datetime, timedelta
import time
from google.oauth2 import service_account
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_secret(secret_id, project_id="smart-434318"):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error(f"Error getting secret {secret_id}: {str(e)}")
        raise

def get_google_credentials():
    try:
        service_account_key_json = get_secret('smart-service-account-key')
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(service_account_key_json)
        )
        return credentials
    except Exception as e:
        logging.error(f"Error getting Google credentials: {str(e)}")
        raise

def get_firestore_config():
    try:
        db = firestore.Client(credentials=get_google_credentials())
        doc_ref = db.collection('config').document('settings')
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        else:
            raise ValueError("Firestore document 'settings' not found")
    except Exception as e:
        logging.error(f"Error fetching Firestore config: {str(e)}")
        raise

def get_twitter_data(bucket_name):
    try:
        storage_client = storage.Client(credentials=get_google_credentials())
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob('twitter-handle.csv')
        content = blob.download_as_text()
        csv_reader = csv.reader(io.StringIO(content))
        next(csv_reader)  # Skip header row
        return [(row[0], row[1]) for row in csv_reader]
    except Exception as e:
        logging.error(f"Error reading Twitter data from Cloud Storage: {str(e)}")
        raise

def normalize_tweet_url(url):
    """Normalize tweet URLs to use x.com consistently."""
    clean_url = re.sub(r'\?.*$', '', url.rstrip('/'))
    normalized_url = clean_url.replace('twitter.com', 'x.com')
    return normalized_url

def search_tweets_with_api(name, handle, api_key, cse_id):
    try:
        logging.info(f"Using CSE ID: {cse_id}")
        base_url = "https://www.googleapis.com/customsearch/v1"
        
        handle_clean = handle.replace('@', '')
        
        search_queries = [
            f"site:twitter.com/{handle_clean}",
            f"site:x.com/{handle_clean}"
        ]
        
        all_tweets = []
        seen_urls = set()  # Track normalized URLs we've already processed
        
        for query in search_queries:
            try:
                params = {
                    'key': api_key,
                    'cx': cse_id,
                    'q': query,
                    'num': 10,
                    'dateRestrict': 'w1'
                }
                
                response = requests.get(base_url, params=params)
                response.raise_for_status()
                search_results = response.json()
                
                if 'items' in search_results:
                    for item in search_results['items']:
                        try:
                            url = item.get('link', '')
                            if not ('status' in url or 'statuses' in url):
                                continue
                                
                            normalized_url = normalize_tweet_url(url)
                            if normalized_url in seen_urls:
                                continue
                            
                            seen_urls.add(normalized_url)
                            
                            author_match = re.search(r'(?:twitter\.com|x\.com)/([^/]+)/status', url)
                            if not author_match:
                                continue
                                
                            actual_author = author_match.group(1).lower()
                            if actual_author != handle_clean.lower():
                                continue
                            
                            # Extract and clean tweet content
                            content = item.get('title', '')
                            content = re.sub(r'^.*?\s*(?:on\s+X|·|:)\s*', '', content)
                            content = re.sub(r'\s*\d+\s*(?:minute|hour|day|week|month|year)s?\s*ago\s*', '', content)
                            content = re.sub(r'\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}\s*', '', content)
                            content = re.sub(r'[^\w\s@#$%.,!?&()-]', ' ', content)
                            content = re.sub(r'\s+', ' ', content).strip()
                            
                            # Extract timestamp (simplified)
                            published_at = datetime.now().strftime('%-m/%-d/%Y %-I:%M:%S %p')
                            
                            tweet = {
                                'Source': name,
                                'Author': f"@{actual_author}",
                                'Tweet Content': content,
                                'Short_Summary': '',
                                'Description': '',
                                'Content': '',
                                'Published At': published_at,
                                'URL': normalized_url
                            }
                            
                            all_tweets.append(tweet)
                            logging.info(f"Added tweet from @{actual_author}: {content[:50]}...")
                            
                        except Exception as e:
                            logging.error(f"Error processing individual tweet: {str(e)}")
                            continue
                
                time.sleep(1)
                
            except requests.exceptions.RequestException as e:
                logging.error(f"API request failed for {query}: {str(e)}")
                continue
                
        return all_tweets if all_tweets else None
    
    except Exception as e:
        logging.error(f"Error in search_tweets_with_api: {str(e)}")
        raise

def clean_dataframe(df):
    try:
        df_cleaned = df.fillna('')
        for column in df_cleaned.columns:
            df_cleaned[column] = df_cleaned[column].astype(str).apply(lambda x: x.strip())
            df_cleaned[column] = df_cleaned[column].str.replace('\0', '')
            df_cleaned[column] = df_cleaned[column].apply(lambda x: ''.join(char for char in x if ord(char) >= 32 or char == '\n'))
        return df_cleaned.drop_duplicates()
    except Exception as e:
        logging.error(f"Error cleaning dataframe: {str(e)}")
        raise

def load_existing_csv(bucket_name, blob_name):
    try:
        storage_client = storage.Client(credentials=get_google_credentials())
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if blob.exists():
            content = blob.download_as_text()
            try:
                df = pd.read_csv(
                    io.StringIO(content),
                    dtype=str,
                    on_bad_lines='warn',
                    encoding='utf-8',
                    quoting=csv.QUOTE_ALL,
                    escapechar='\\'
                )
                return clean_dataframe(df)
            except Exception as e:
                logging.warning(f"Error reading CSV: {str(e)}")
                return pd.DataFrame(columns=['Source', 'Author', 'Title', 'Short_Summary', 
                                          'Description', 'Content', 'Published At', 'URL'])
        else:
            return pd.DataFrame(columns=['Source', 'Author', 'Title', 'Short_Summary', 
                                       'Description', 'Content', 'Published At', 'URL'])
    except Exception as e:
        logging.error(f"Error loading existing CSV: {str(e)}")
        raise

def save_csv_to_gcs(df, bucket_name, blob_name, max_retries=3, timeout=300):
    df_cleaned = clean_dataframe(df)
    csv_buffer = io.StringIO()
    df_cleaned.to_csv(
        csv_buffer,
        index=False,
        na_rep='',
        quoting=csv.QUOTE_ALL,
        escapechar='\\',
        encoding='utf-8'
    )
    content = csv_buffer.getvalue()

    for attempt in range(max_retries):
        try:
            storage_client = storage.Client(credentials=get_google_credentials())
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.upload_from_string(content, content_type='text/csv', timeout=timeout)
            logging.info(f"Successfully uploaded CSV on attempt {attempt + 1}")
            return
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed to save CSV after {max_retries} attempts: {str(e)}")
                raise
            logging.warning(f"Upload attempt {attempt + 1} failed, retrying... Error: {str(e)}")
            time.sleep(2 ** attempt)  # Exponential backoff

def process_twitter_data(config):
    try:
        # Get API credentials from Secret Manager
        api_key = get_secret('GOOGLE_CUSTOM_SEARCH_API_KEY')
        cse_id = get_secret('GOOGLE_CUSTOM_SEARCH_ENGINE_ID')
        
        twitter_data = get_twitter_data(config['TWITTER_HANDLE'])
        news_bucket_name = "smart-434318-news-bbg-rss"
        news_blob_name = "news_bbg_rss.csv"

        # Load existing news CSV
        existing_df = load_existing_csv(news_bucket_name, news_blob_name)
        new_tweets = []

        for name, handle in twitter_data:
            try:
                tweets = search_tweets_with_api(name, handle, api_key, cse_id)
                
                if tweets:
                    for tweet in tweets:
                        # Check if tweet already exists in our database
                        if existing_df[(existing_df['Title'] == tweet['Tweet Content']) & 
                                     (existing_df['URL'] == tweet['URL'])].empty:
                            news_entry = {
                                'Source': tweet['Source'],
                                'Author': tweet['Author'],
                                'Title': tweet['Tweet Content'],
                                'Short_Summary': '',
                                'Description': '',
                                'Content': '',
                                'Published At': tweet['Published At'],
                                'URL': tweet['URL']
                            }
                            new_tweets.append(news_entry)
                            logging.info(f"Added new tweet from {tweet['Author']}: '{tweet['Tweet Content'][:50]}...'")
                        else:
                            logging.debug(f"Skipping duplicate tweet from {tweet['Author']}")
                else:
                    logging.debug(f"No tweets found for {handle}")

            except Exception as e:
                logging.error(f"Error processing {handle} ({name}): {str(e)}")
                continue
                
            time.sleep(2)  # Rate limiting between different handles

        # Add new tweets to existing DataFrame
        if new_tweets:
            new_df = pd.DataFrame(new_tweets)
            updated_df = pd.concat([existing_df, new_df], ignore_index=True)
            save_csv_to_gcs(updated_df, news_bucket_name, news_blob_name)
            logging.info(f"Successfully updated {len(new_tweets)} tweets")
        else:
            logging.info("No new tweets to add.")
            
    except Exception as e:
        logging.error(f"Error in process_twitter_data: {str(e)}")
        raise

def main():
    try:
        logging.info("Starting script...")
        config = get_firestore_config()
        process_twitter_data(config)
    except Exception as e:
        logging.error(f"Unexpected error in main: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
