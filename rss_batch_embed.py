# rss_batch_embed.py

import os
import csv
import json
import logging
import time
from io import StringIO, BytesIO
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import secretmanager
from google.auth.transport.requests import Request, AuthorizedSession
import google.auth
import requests
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from google.resumable_media import requests as resumable_requests
import pandas as pd
from multiprocessing import Pool, cpu_count

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
PROJECT_ID = 'smart-434318'
CSV_BUCKET = 'smart-434318-news-bbg-rss'
CSV_FILE = 'news_bbg_rss.csv'
EMBEDDINGS_BUCKET = 'smart-434318-vector-db'
EMBEDDINGS_FILE = 'interests_only_embeddings.json'
BATCH_SIZE = 40
MAX_RETRIES = 5
RETRY_DELAY = 5

def get_secret(secret_id, project_id=PROJECT_ID):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_google_credentials():
    service_account_key_json = get_secret('smart-service-account-key')
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(service_account_key_json),
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return credentials

def read_csv_from_gcs(bucket_name, file_name):
    logging.info(f"Attempting to read file: {file_name} from bucket: {bucket_name}")
    storage_client = storage.Client(credentials=get_google_credentials(), project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    try:
        content = blob.download_as_text()
        logging.info(f"Successfully downloaded file content. Size: {len(content)} characters")
        
        csv_reader = csv.DictReader(StringIO(content))
        entries = list(csv_reader)
        logging.info(f"Parsed {len(entries)} entries from the CSV file")
        return entries
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return None

def clean_dataframe(df):
    # Replace NaN values with an empty string
    df_cleaned = df.fillna('')
    
    # Ensure all columns are strings, except for 'CSS' and 'Embeddings'
    for column in df_cleaned.columns:
        if column not in ['CSS', 'Embeddings']:
            df_cleaned[column] = df_cleaned[column].astype(str)
    
    # Remove any remaining NaN or 'nan' strings
    df_cleaned = df_cleaned.replace('nan', '', regex=True)
    
    return df_cleaned

def write_csv_to_gcs(bucket_name, file_name, data):
    storage_client = storage.Client(credentials=get_google_credentials(), project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Convert data to DataFrame and clean it
    df = pd.DataFrame(data)
    df_cleaned = clean_dataframe(df)

    output = StringIO()
    df_cleaned.to_csv(output, index=False)
    content = output.getvalue().encode('utf-8')
    content_length = len(content)

    credentials = get_google_credentials()
    transport = AuthorizedSession(credentials)

    chunk_size = 256 * 1024  # 256 KB chunks
    url = f"https://www.googleapis.com/upload/storage/v1/b/{bucket_name}/o?uploadType=resumable"
    
    resumable_upload = resumable_requests.ResumableUpload(
        upload_url=url,
        chunk_size=chunk_size
    )

    metadata = {
        "name": file_name,
        "content-type": "text/csv",
    }

    stream = BytesIO(content)
    resumable_upload.initiate(
        transport=transport,
        content_type="text/csv",
        stream=stream,
        metadata=metadata,
        total_bytes=content_length
    )

    total_uploaded = 0
    while not resumable_upload.finished:
        response = resumable_upload.transmit_next_chunk(transport)
        total_uploaded += chunk_size
        logging.info(f"Uploaded {min(total_uploaded, content_length)}/{content_length} bytes ({(min(total_uploaded, content_length)/content_length)*100:.2f}%)")

    logging.info(f"Updated CSV file uploaded to {bucket_name}/{file_name}")

def load_interest_embeddings():
    storage_client = storage.Client(credentials=get_google_credentials(), project=PROJECT_ID)
    bucket = storage_client.bucket(EMBEDDINGS_BUCKET)
    blob = bucket.blob(EMBEDDINGS_FILE)
    content = blob.download_as_text()
    interest_embeddings = json.loads(content)
    
    # Convert to numpy array and ensure it's 2D
    embeddings_array = np.array(list(interest_embeddings.values()))
    if embeddings_array.ndim == 1:
        embeddings_array = embeddings_array.reshape(1, -1)
    
    logging.info(f"Loaded interest embeddings. Shape: {embeddings_array.shape}")
    return embeddings_array

def generate_embeddings_batch(texts, max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY):
    url = f"https://us-central1-aiplatform.googleapis.com/v1/projects/{PROJECT_ID}/locations/us-central1/publishers/google/models/text-embedding-004:predict"
    
    credentials = get_google_credentials()
    credentials.refresh(Request())
    
    headers = {
        "Authorization": f"Bearer {credentials.token}",
        "Content-Type": "application/json"
    }
    
    instances = [{"task_type": "SEMANTIC_SIMILARITY", "content": text} for text in texts]
    data = {"instances": instances}
    
    for attempt in range(max_retries):
        try:
            logging.info(f"Sending request to {url}")
            response = requests.post(url, headers=headers, json=data, timeout=60)
            logging.info(f"Received response with status code: {response.status_code}")
            response.raise_for_status()
            result = response.json()
            return [item['embeddings']['values'] for item in result['predictions']]
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed on attempt {attempt + 1}: {str(e)}")
            if attempt == max_retries - 1:
                logging.error(f"Failed to generate embeddings after {max_retries} attempts")
                raise
            logging.warning(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

def process_entries(entries, interest_embeddings):
    updated_entries = []
    total_entries = len(entries)
    
    for i in range(0, total_entries, BATCH_SIZE):
        batch = entries[i:i+BATCH_SIZE]
        
        texts = []
        for entry in batch:
            try:
                text = f"{entry['Source']} {entry['Title']} {entry['Short_Summary']}"
                texts.append(text)
            except KeyError as e:
                logging.warning(f"Missing key in entry: {e}. Skipping this entry.")
                continue
        
        try:
            batch_embeddings = generate_embeddings_batch(texts)
            
            if batch_embeddings:
                batch_embeddings = np.array(batch_embeddings)
                similarities = cosine_similarity(batch_embeddings, interest_embeddings)
                
                for j, entry in enumerate(batch):
                    try:
                        entry['CSS'] = float(max(similarities[j]))
                        entry['Embeddings'] = json.dumps(batch_embeddings[j].tolist())
                        updated_entries.append(entry)
                    except IndexError:
                        logging.warning(f"Error processing entry {j} in batch. Skipping.")
            else:
                logging.warning(f"Failed to generate embeddings for batch. Skipping batch.")
        except Exception as e:
            logging.error(f"Error processing batch: {e}")
        
        logging.info(f"Processed {min(i + BATCH_SIZE, total_entries)}/{total_entries} entries")
        
        # Save intermediate results every 500 entries
        if len(updated_entries) % 500 == 0:
            save_intermediate_results(updated_entries)
    
    return updated_entries

def save_intermediate_results(entries):
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    filename = f"intermediate_results_{timestamp}.json"
    
    storage_client = storage.Client(credentials=get_google_credentials(), project=PROJECT_ID)
    bucket = storage_client.bucket(CSV_BUCKET)
    blob = bucket.blob(filename)
    
    blob.upload_from_string(json.dumps(entries), content_type='application/json')
    logging.info(f"Saved intermediate results to {filename}")

def main():
    logging.info("Starting news processing script")
    
    # Load interest embeddings once at the start
    interest_embeddings = load_interest_embeddings()
    
    # Read entries from CSV
    entries = read_csv_from_gcs(CSV_BUCKET, CSV_FILE)
    if not entries:
        logging.error("Failed to read CSV file")
        return
    
    logging.info(f"Processing {len(entries)} entries")
    
    # Process entries
    updated_entries = process_entries(entries, interest_embeddings)
    
    logging.info(f"Processed {len(updated_entries)} entries")
    
    # Write back to GCS
    write_csv_to_gcs(CSV_BUCKET, CSV_FILE, updated_entries)
    logging.info("Script completed successfully")

if __name__ == "__main__":
    main()
