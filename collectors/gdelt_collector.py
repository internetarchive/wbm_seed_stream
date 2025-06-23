import requests
import zipfile
import io
import csv
import time
import logging
import urllib3
from urllib.parse import urlparse
import hashlib
import re
from datetime import datetime
import os

DEBUG_MODE = True

GDELT_BASE = "https://data.gdeltproject.org/gdeltv2/"
LAST_UPDATE_URL = GDELT_BASE + "lastupdate.txt"
LAST_UPDATE_TRANSLATION_URL = GDELT_BASE + "lastupdate-translation.txt"
INGEST_API_URL = "http://localhost:8000/api/ingest" 

ingested_urls = set()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
csv.field_size_limit(30 * 1024 * 1024)

handlers = []
handlers.append(logging.StreamHandler())

if DEBUG_MODE:
    os.makedirs("logs", exist_ok=True)
    log_filename = f"logs/gdelt_ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    handlers.append(logging.FileHandler(log_filename))

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s:%(name)s:%(message)s',
    handlers=handlers
)

def fetch_last_update_files(update_url):
    try:
        resp = requests.get(update_url, verify=False)
        resp.raise_for_status()
        filenames = []
        for line in resp.text.strip().splitlines():
            parts = line.split()
            url_part = next((p for p in parts if p.startswith("http")), None)
            if url_part:
                filenames.append(url_part.split('/')[-1])
        return filenames
    except Exception as e:
        logging.error(f"Error fetching update files from {update_url}: {e}")
        return []

def download_and_extract_csv(zip_filename):
    url = GDELT_BASE + zip_filename
    
    try:
        resp = requests.get(url, verify=False)
        resp.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            csv_files = z.namelist()
            if not csv_files:
                return []
                
            with z.open(csv_files[0]) as f:
                text_data = io.TextIOWrapper(f, 'utf-8', errors='ignore')
                reader = csv.reader(text_data, delimiter='\t')
                return list(reader)
                
    except Exception as e:
        logging.error(f"Error downloading/extracting {zip_filename}: {e}")
        return []

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False

def extract_urls_from_text(text):
    url_pattern = r'https?://[^\s<>"\{\}|\\^`\[\]]+[^\s<>"\{\}|\\^`\[\].,;:!?]'
    urls = re.findall(url_pattern, text)
    return [url for url in urls if is_valid_url(url)]

def ingest_url(url, meta=None):
    url_hash = hashlib.md5(url.encode()).hexdigest()
    
    if url_hash in ingested_urls:
        return
    
    payload = {
        "url": url,
        "source": "gdelt",
        "meta": meta or {},
        "priority": 1
    }
    
    try:
        resp = requests.post(INGEST_API_URL, json=payload, timeout=30)
        if resp.ok:
            ingested_urls.add(url_hash)
            logging.info(f"Successfully ingested: {url}")
        else:
            logging.warning(f"Failed to ingest {url}: HTTP {resp.status_code}")
    except requests.exceptions.Timeout:
        logging.error(f"Timeout ingesting {url}")
    except Exception as e:
        logging.error(f"Error ingesting {url}: {e}")

def extract_urls_from_rows(rows):
    urls = set()
    
    for row in rows:
        for cell in row:
            if isinstance(cell, str) and cell.strip():
                if ';' in cell and 'http' in cell.lower():
                    potential_urls = cell.split(';')
                    for potential_url in potential_urls:
                        potential_url = potential_url.strip()
                        if potential_url.startswith("http") and is_valid_url(potential_url):
                            urls.add(potential_url)
                elif cell.startswith("http") and is_valid_url(cell):
                    urls.add(cell)
                elif "http" in cell.lower():
                    cell_urls = extract_urls_from_text(cell)
                    for url in cell_urls:
                        urls.add(url)
    
    return urls

def process_gdelt_feed(update_url, source_type):
    logging.info(f"Processing {source_type} feed")
    
    filenames = fetch_last_update_files(update_url)
    if not filenames:
        logging.warning(f"No files found for {source_type} feed")
        return
    
    for filename in filenames:
        logging.info(f"Processing file: {filename}")
        rows = download_and_extract_csv(filename)
        
        if not rows:
            continue
            
        urls = extract_urls_from_rows(rows)
        logging.info(f"Found {len(urls)} URLs in {filename}")
        
        for url in urls:
            ingest_url(url, meta={
                "source": "gdelt",
                "feed_type": source_type,
                "filename": filename
            })

def run_ingestion():
    logging.info("Starting GDELT ingestion cycle")
    
    process_gdelt_feed(LAST_UPDATE_URL, "main")
    process_gdelt_feed(LAST_UPDATE_TRANSLATION_URL, "translation")
    
    logging.info("Completed GDELT ingestion cycle")

def main():
    logging.info("Starting GDELT ingestion service")
    logging.info(f"Ingesting to: {INGEST_API_URL}")
    
    while True:
        try:
            run_ingestion()
        except KeyboardInterrupt:
            logging.info("Shutting down...")
            break
        except Exception as e:
            logging.error(f"Pipeline error: {e}")
        
        logging.info("Waiting 15 minutes...")
        time.sleep(15 * 60)

if __name__ == "__main__":
    main()
    
"""
Notes:
- Need some way to ignore certain domains like youtube.com/sdlks/?v= or something perhaps?
- You don't need to be not archiving youtube pages apparently?
"""