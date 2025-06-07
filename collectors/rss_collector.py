import requests
import time
import logging
import hashlib
from urllib.parse import urlparse
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET

INGEST_API_URL = "http://localhost:8000/api/ingest"
SLEEP_INTERVAL = 1800
CACHE_FILE = "rss_cache.txt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

RSS_FEEDS = [
    "http://rss.cnn.com/rss/cnn_topstories.rss",
    "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
    "https://feeds.washingtonpost.com/rss/national",
    "https://feeds.nbcnews.com/nbcnews/public/news",
    "https://abcnews.go.com/abcnews/topstories",
    "https://www.cbsnews.com/latest/rss/main",
    "https://feeds.npr.org/1001/rss.xml",
    "https://feeds.foxnews.com/foxnews/latest",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://moxie.foxnews.com/google-publisher/latest.xml",
    "https://www.aljazeera.com/xml/rss/all.xml",
    "https://www.theguardian.com/world/rss",
    "https://rss.dw.com/rdf/rss-en-all",
    "https://www.france24.com/en/rss",
    "https://www.cbc.ca/cmlink/rss-world"
]

def load_seen_items():
    try:
        with open(CACHE_FILE, 'r') as f:
            return set(line.strip() for line in f)
    except FileNotFoundError:
        return set()

def save_seen_item(item_hash):
    with open(CACHE_FILE, 'a') as f:
        f.write(f"{item_hash}\n")

def get_item_hash(item):
    unique_string = f"{item.get('link', '')}{item.get('pubDate', '')}{item.get('title', '')}"
    return hashlib.md5(unique_string.encode()).hexdigest()

def parse_rss_feed(feed_url):
    try:
        resp = requests.get(feed_url, timeout=10)
        resp.raise_for_status()
        
        root = ET.fromstring(resp.content)
        items = []
        
        # Handle different RSS formats
        if root.tag == 'rss':
            # Standard RSS 2.0
            for item in root.findall('.//item'):
                items.append(parse_rss_item(item))
        elif root.tag.endswith('RDF') or 'rdf' in root.tag.lower():
            # RDF/RSS 1.0
            for item in root.findall('.//{http://purl.org/rss/1.0/}item'):
                items.append(parse_rdf_item(item))
        
        return items
    except Exception as e:
        logging.error(f"Error parsing RSS feed {feed_url}: {e}")
        return []

def parse_rss_item(item):
    return {
        'title': get_text(item.find('title')),
        'link': get_text(item.find('link')),
        'description': get_text(item.find('description')),
        'pubDate': get_text(item.find('pubDate')),
        'guid': get_text(item.find('guid')),
        'author': get_text(item.find('author')),
        'category': get_text(item.find('category'))
    }

def parse_rdf_item(item):
    """Parse an RDF/RSS 1.0 item"""
    return {
        'title': get_text(item.find('.//{http://purl.org/rss/1.0/}title')),
        'link': get_text(item.find('.//{http://purl.org/rss/1.0/}link')),
        'description': get_text(item.find('.//{http://purl.org/rss/1.0/}description')),
        'pubDate': get_text(item.find('.//{http://purl.org/dc/elements/1.1/}date')),
        'guid': item.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about', ''),
        'author': get_text(item.find('.//{http://purl.org/dc/elements/1.1/}creator')),
        'category': get_text(item.find('.//{http://purl.org/dc/elements/1.1/}subject'))
    }

def get_text(element):
    return element.text if element is not None else ''

def should_ingest_url(url):
    if not url:
        return False
    
    parsed_url = urlparse(url)
    skip_domains = ['web.archive.org', 'archive.today']
    if parsed_url.netloc in skip_domains:
        return False
    
    return True

def ingest_url(url, meta, priority=0, source="rss"):
    if not should_ingest_url(url):
        logging.info(f"Skipped (filtered): {url}")
        return

    # Try to get Last-Modified header
    last_modified = None
    try:
        head_resp = requests.head(url, timeout=5, allow_redirects=True)
        last_mod_header = head_resp.headers.get("Last-Modified")
        if last_mod_header:
            last_modified = parsedate_to_datetime(last_mod_header).isoformat()
    except Exception as e:
        logging.warning(f"Failed to fetch Last-Modified for {url}: {e}")
        last_modified = None

    payload = {
        "url": url,
        "source": source,
        "meta": meta,
        "priority": priority,
        "last_modified": last_modified
    }

    try:
        resp = requests.post(INGEST_API_URL, json=payload, timeout=5)
        if resp.status_code in (200, 201):
            logging.info(f"Ingested: {url}")
        else:
            logging.warning(f"Failed to ingest {url}: {resp.status_code} {resp.text}")
    except Exception as e:
        logging.error(f"Error posting to ingest API: {e}")

def process_feed(feed_url, seen_items):
    logging.info(f"Processing feed: {feed_url}")
    
    items = parse_rss_feed(feed_url)
    new_items_count = 0
    
    for item in items:
        item_hash = get_item_hash(item)
        
        if item_hash not in seen_items:
            # New item found
            article_url = item.get('link', '')
            if article_url:
                meta = {
                    "rss_feed": feed_url,
                    "title": item.get('title', ''),
                    "description": item.get('description', ''),
                    "pubDate": item.get('pubDate', ''),
                    "author": item.get('author', ''),
                    "category": item.get('category', ''),
                    "guid": item.get('guid', ''),
                    "source": "rss"
                }
                
                ingest_url(article_url, meta, priority=1, source="rss")
                seen_items.add(item_hash)
                save_seen_item(item_hash)
                new_items_count += 1
    
    if new_items_count > 0:
        logging.info(f"Found {new_items_count} new items in {feed_url}")
    else:
        logging.info(f"No new items in {feed_url}")

def run_collector():
    seen_items = load_seen_items()
    logging.info(f"Loaded {len(seen_items)} previously seen items from cache")
    
    while True:
        start_time = time.time()
        logging.info("Starting RSS feed collection cycle")
        
        for feed_url in RSS_FEEDS:
            try:
                process_feed(feed_url, seen_items)
            except Exception as e:
                logging.error(f"Error processing feed {feed_url}: {e}")
        
        elapsed_time = time.time() - start_time
        logging.info(f"Collection cycle completed in {elapsed_time:.2f} seconds")
        
        # Sleep until next cycle
        time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    try:
        logging.info("Starting RSS Feed Collector")
        logging.info(f"Monitoring {len(RSS_FEEDS)} RSS feeds")
        logging.info(f"Check interval: {SLEEP_INTERVAL} seconds")
        run_collector()
    except KeyboardInterrupt:
        logging.info("Shutting down RSS collector.")


"""
Notes:
- Breakdown of how NPR's RSS Feeds Work: https://www.reddit.com/r/NPR/comments/16t5102/npr_rss_news_feeds/
- All of Fox News' RSS Feeds can be found here: https://www.foxnews.com/story/foxnews-com-rss-feeds
"""