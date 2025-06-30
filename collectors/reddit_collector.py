import requests
import time
import logging
import hashlib
from urllib.parse import urlparse

INGEST_API_URL = "http://localhost:8000/api/ingest"
SLEEP_INTERVAL = 1800  
CACHE_FILE = "reddit_cache.txt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
SUBREDDITS = [
    "worldnews",
    "news",
    "politics",
    "technology",
    "science",
    "business",
    "economics",
    "UkrainianConflict",
    "geopolitics",
    "Economics",
    "stocks",
    "investing",
    "personalfinance",
    "ClimateChange",
    "environment",
    "AskScience",
    "explainlikeimfive",
    "DataIsBeautiful",
    "Futurology"
]

HEADERS = {
    'User-Agent': 'RSS_Collector/1.0 (by /u/akshithio)'  
}

def load_seen_items():
    try:
        with open(CACHE_FILE, 'r') as f:
            return set(line.strip() for line in f)
    except FileNotFoundError:
        return set()

def save_seen_item(item_hash):
    with open(CACHE_FILE, 'a') as f:
        f.write(f"{item_hash}\n")

def get_item_hash(post):
    unique_string = f"{post.get('id', '')}{post.get('created_utc', '')}{post.get('title', '')}"
    return hashlib.md5(unique_string.encode()).hexdigest()

def fetch_subreddit_posts(subreddit, limit=25, sort='hot'):
    try:
        url = f"https://www.reddit.com/r/{subreddit}/{sort}.json"
        params = {
            'limit': limit,
            'raw_json': 1
        }
        
        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
        resp.raise_for_status()
        
        data = resp.json()
        posts = []
        
        if 'data' in data and 'children' in data['data']:
            for child in data['data']['children']:
                if child['kind'] == 't3':  
                    posts.append(child['data'])
        
        return posts
        
    except Exception as e:
        logging.error(f"Error fetching subreddit r/{subreddit}: {e}")
        return []

def should_ingest_url(url):
    if not url:
        return False
    
    parsed_url = urlparse(url)
    

    skip_domains = [
        'web.archive.org', 
        'archive.today',
        'reddit.com',  
        'redd.it',     
        'v.redd.it',   
        'i.redd.it',
        'old.reddit.com',
        'new.reddit.com',
        'www.reddit.com'
    ]
    
    if parsed_url.netloc in skip_domains:
        return False
        
    if not parsed_url.netloc or parsed_url.netloc == '':
        return False
    
    return True

def ingest_url(url, meta, priority=0, source="reddit"):
    if not should_ingest_url(url):
        logging.info(f"Skipped (filtered): {url}")
        return

    payload = {
        "url": url,
        "source": source,
        "meta": meta,
        "priority": priority,
        "last_modified": None 
    }

    try:
        resp = requests.post(INGEST_API_URL, json=payload, timeout=5)
        if resp.status_code in (200, 201):
            logging.info(f"Ingested: {url}")
        else:
            logging.warning(f"Failed to ingest {url}: {resp.status_code} {resp.text}")
    except Exception as e:
        logging.error(f"Error posting to ingest API: {e}")

def process_subreddit(subreddit, seen_items):
    logging.info(f"Processing subreddit: r/{subreddit}")
    
    posts = fetch_subreddit_posts(subreddit)
    new_items_count = 0
    
    for post in posts:
        post_hash = get_item_hash(post)
        
        if post_hash not in seen_items:
            post_url = post.get('url', '')
            if post_url and should_ingest_url(post_url):
                meta = {
                    "reddit_subreddit": f"r/{subreddit}",
                    "reddit_id": post.get('id', ''),
                    "reddit_permalink": f"https://reddit.com{post.get('permalink', '')}",
                    "title": post.get('title', ''),
                    "selftext": post.get('selftext', ''),
                    "author": post.get('author', ''),
                    "score": post.get('score', 0),
                    "upvote_ratio": post.get('upvote_ratio', 0),
                    "num_comments": post.get('num_comments', 0),
                    "created_utc": post.get('created_utc', 0),
                    "subreddit": subreddit,
                    "domain": post.get('domain', ''),
                    "flair": post.get('link_flair_text', ''),
                    "nsfw": post.get('over_18', False),
                    "stickied": post.get('stickied', False),
                    "source": "reddit"
                }
                
                
                ingest_url(post_url, meta, priority=1, source="reddit")
                new_items_count += 1
            
            seen_items.add(post_hash)
            save_seen_item(post_hash)
    
    if new_items_count > 0:
        logging.info(f"Found {new_items_count} new items in r/{subreddit}")
    else:
        logging.info(f"No new items in r/{subreddit}")

def run_collector():
    seen_items = load_seen_items()
    logging.info(f"Loaded {len(seen_items)} previously seen items from cache")
    
    while True:
        start_time = time.time()
        logging.info("Starting Reddit collection cycle")
        
        for subreddit in SUBREDDITS:
            try:
                process_subreddit(subreddit, seen_items)
                time.sleep(1) 
            except Exception as e:
                logging.error(f"Error processing subreddit r/{subreddit}: {e}")
        
        elapsed_time = time.time() - start_time
        logging.info(f"Collection cycle completed in {elapsed_time:.2f} seconds")
        logging.info(f"Sleeping for {SLEEP_INTERVAL} seconds until next cycle")
        time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    try:
        logging.info("Starting Reddit Collector")
        logging.info(f"Monitoring {len(SUBREDDITS)} subreddits")
        logging.info(f"Check interval: {SLEEP_INTERVAL} seconds")
        run_collector()
    except KeyboardInterrupt:
        logging.info("Shutting down Reddit collector.")

"""
Notes:
- Uses Reddit's public JSON API (no authentication required)
- Respects Reddit's rate limits with delays between requests
- Filters out Reddit internal links and focuses on external content
- Priority scoring based on post engagement (score and comments)
- I think Reddit's public JSON API requires a UserAgent
- Reddit API documentation: https://www.reddit.com/dev/api/
- JSON endpoints: You can just add .json to any Reddit URL to get JSON data
"""