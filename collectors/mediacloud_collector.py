import requests
import time
import logging
import gzip
import json
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse
import re
from dotenv import load_dotenv

load_dotenv('../.env')

INGEST_API_URL = "http://localhost:8000/api/ingest"
MEDIACLOUD_BASE_URL = os.getenv('MEDIACLOUD_URL')
DAYS_TO_INGEST = 1
FAILED_INGESTS_LOG = "failed_ingests.txt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def log_failed_ingest(url, meta, status_code, response_text, error_details=None):
    timestamp = datetime.now().isoformat()
    
    with open(FAILED_INGESTS_LOG, "a", encoding="utf-8") as f:
        f.write(f"FAILED INGEST - {timestamp}\n")
        f.write(f"URL: {url}\n")
        f.write(f"Status Code: {status_code}\n")
        f.write(f"Response: {response_text}\n")
        f.write(f"Meta: {json.dumps(meta, indent=2)}\n")
        if error_details:
            f.write(f"Error Details: {error_details}\n")
        f.write("-" * 80 + "\n\n")

def get_date_range(days_back):
    dates = []
    start_date = datetime.now() - timedelta(days=1)
    
    for i in range(days_back):
        date = start_date - timedelta(days=i)
        dates.append(date.strftime("%Y-%m-%d"))
    
    return dates

def fetch_mediacloud_rss(date):
    url = MEDIACLOUD_BASE_URL.format(date=date) 
    
    try:
        logging.info(f"Fetching MediaCloud RSS for {date}")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        
        decompressed_data = gzip.decompress(resp.content)
        return decompressed_data.decode('utf-8')
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching MediaCloud RSS for {date}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error decompressing RSS data for {date}: {e}")
        return None

def parse_rss_content(rss_content):
    articles = []
    
    try:
        item_pattern = r'<item>.*?</item>'
        items = re.findall(item_pattern, rss_content, re.DOTALL)
        
        for item_str in items:
            link_match = re.search(r'<link>(.*?)</link>', item_str)
            if not link_match:
                continue
                
            url = link_match.group(1).replace('&amp;', '&')
            
            title_match = re.search(r'<title>(.*?)</title>', item_str)
            pubdate_match = re.search(r'<pubDate>(.*?)</pubDate>', item_str)
            domain_match = re.search(r'<domain>(.*?)</domain>', item_str)
            source_match = re.search(r'<source url="([^"]*)"[^>]*mcFeedId="([^"]*)"[^>]*mcSourceId="([^"]*)"', item_str)
            
            article_data = {
                'url': url,
                'title': title_match.group(1) if title_match else '',
                'pub_date': pubdate_match.group(1) if pubdate_match else '',
                'domain': domain_match.group(1) if domain_match else '',
                'source_url': source_match.group(1) if source_match else '',
                'mc_feed_id': source_match.group(2) if source_match else '',
                'mc_source_id': source_match.group(3) if source_match else ''
            }
            
            articles.append(article_data)
            
    except Exception as e:
        logging.error(f"Error parsing RSS content: {e}")
    
    return articles

def should_ingest_url(url):
    parsed_url = urlparse(url)
    
    if parsed_url.netloc in ["web.archive.org", "archive.today", "archive.is"]:
        return False
    
    return True

def ingest_url(url, meta, priority=0, source="mediacloud"):
    if not should_ingest_url(url):
        logging.info(f"Skipped (filtered): {url}")
        return
    
    payload = {
        "url": url,
        "source": source,
        "meta": meta,
        "priority": priority
    }
    
    try:
        resp = requests.post(INGEST_API_URL, json=payload, timeout=5)
        if resp.status_code == 201:
            logging.info(f"Ingested: {url}")
        else:
            logging.warning(f"Failed to ingest {url}: {resp.status_code} {resp.text}")
            log_failed_ingest(url, meta, resp.status_code, resp.text)
    except Exception as e:
        logging.error(f"Error posting to ingest API: {e}")
        log_failed_ingest(url, meta, None, None, e)

def process_date(date):
    logging.info(f"Processing MediaCloud data for {date}")
    
    rss_content = fetch_mediacloud_rss(date)
    if not rss_content:
        logging.error(f"Failed to fetch RSS data for {date}")
        return 0
    
    articles = parse_rss_content(rss_content)
    logging.info(f"Found {len(articles)} articles for {date}")
    
    ingested_count = 0
    for article in articles:
        meta = {
            "date": date,
            "title": article['title'],
            "pub_date": article['pub_date'],
            "domain": article['domain'],
            "source_url": article['source_url'],
            "mc_feed_id": article['mc_feed_id'],
            "mc_source_id": article['mc_source_id'],
            "source": "mediacloud"
        }
        
        ingest_url(article['url'], meta, priority=0, source="mediacloud")
        ingested_count += 1
    
    return ingested_count

def run_collector():
    logging.info(f"Starting MediaCloud collector for {DAYS_TO_INGEST} days")
    
    dates = get_date_range(DAYS_TO_INGEST)
    total_ingested = 0
    
    for date in dates:
        try:
            count = process_date(date)
            total_ingested += count
            logging.info(f"Completed processing {date}: {count} articles")
            
            time.sleep(2)
            
        except Exception as e:
            logging.error(f"Error processing date {date}: {e}")
            continue
    
    logging.info(f"MediaCloud collection completed. Total articles ingested: {total_ingested}")

if __name__ == "__main__":
    try:
        run_collector()
    except KeyboardInterrupt:
        logging.info("Shutting down MediaCloud collector.")