import requests
import time
import logging
import json
import hashlib
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup, Tag
import re

INGEST_API_URL = "http://localhost:8000/api/ingest"
SLEEP_INTERVAL = 300
CACHE_FILE = "web_crawler_cache.json"
USER_AGENT = "WebCrawler/1.0 (+http://localhost:8000/bot)"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

CRAWL_CONFIG = {
    "news.ycombinator.com": {
        "start_urls": ["https://news.ycombinator.com/", "https://news.ycombinator.com/newest"],
        "follow_patterns": [r"/item\?id=\d+"],
        "extract_external": True,
        "max_depth": 2,
        "delay": 1.0,
        "priority": 2
    },
    "lobste.rs": {
        "start_urls": ["https://lobste.rs/"],
        "follow_patterns": [r"/s/[^/]+"],
        "extract_external": True,
        "max_depth": 2,
        "delay": 1.0,
        "priority": 2
    },
    "reddit.com": {
        "start_urls": [
            "https://old.reddit.com/r/programming/",
            "https://old.reddit.com/r/technology/",
            "https://old.reddit.com/r/MachineLearning/"
        ],
        "follow_patterns": [r"/r/\w+/comments/[^/]+"],
        "extract_external": True,
        "max_depth": 2,
        "delay": 2.0,
        "priority": 2
    },
    
    "producthunt.com": {
        "start_urls": ["https://www.producthunt.com/"],
        "follow_patterns": [r"/posts/[^/]+"],
        "extract_external": True,
        "max_depth": 2,
        "delay": 1.0,
        "priority": 2
    }
}

def load_cache():
    try:
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "seen_pages": {},
            "seen_links": set(),
            "last_crawl": {},
            "robots_cache": {}
        }

def save_cache(cache):
    cache_copy = cache.copy()
    cache_copy["seen_links"] = list(cache["seen_links"])
    cache_copy["last_updated"] = datetime.now(timezone.utc).isoformat()
    
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache_copy, f, indent=2)

def normalize_url(url):
    parsed = urlparse(url)
    normalized = urlunparse((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        parsed.path,
        parsed.params,
        parsed.query,
        ''
    ))
    return normalized

def get_robots_txt(domain, cache):
    if domain in cache.get("robots_cache", {}):
        return cache["robots_cache"][domain]
    
    try:
        robots_url = f"https://{domain}/robots.txt"
        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        
        cache.setdefault("robots_cache", {})[domain] = {
            "parser": rp,
            "checked": datetime.now(timezone.utc).isoformat()
        }
        return cache["robots_cache"][domain]
    except Exception as e:
        logging.warning(f"Could not fetch robots.txt for {domain}: {e}")
        return None

def can_fetch(url, user_agent, cache):
    parsed = urlparse(url)
    domain = parsed.netloc
    
    robots_info = get_robots_txt(domain, cache)
    if robots_info and robots_info.get("parser"):
        return robots_info["parser"].can_fetch(user_agent, url)
    
    return True

def fetch_page(url, delay=1.0):
    headers = {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }
    
    try:
        time.sleep(delay)
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def extract_links(html_content, base_url):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        links = []
        
        for link in soup.find_all('a', href=True):
            if isinstance(link, Tag):
                href_attr = link.get('href')
                if href_attr:
                    href = str(href_attr).strip() if isinstance(href_attr, list) else href_attr.strip()
                    if href and not href.startswith('
                        absolute_url = urljoin(base_url, href)
                        title_attr = link.get('title')
                        title = str(title_attr) if title_attr else ''
                        
                        links.append({
                            'url': absolute_url,
                            'text': link.get_text(strip=True),
                            'title': title,
                            'source_element': 'a'
                        })
        
        for element in soup.find_all(['iframe', 'embed'], src=True):
            if isinstance(element, Tag):
                src_attr = element.get('src')
                if src_attr:
                    src = str(src_attr).strip() if isinstance(src_attr, list) else src_attr.strip()
                    if src:
                        absolute_url = urljoin(base_url, src)
                        title_attr = element.get('title')
                        title = str(title_attr) if title_attr else ''
                        
                        links.append({
                            'url': absolute_url,
                            'text': '',
                            'title': title,
                            'source_element': element.name
                        })
        
        return links
    except Exception as e:
        logging.error(f"Error parsing HTML from {base_url}: {e}")
        return []

def should_follow_link(url, follow_patterns, domain):
    parsed = urlparse(url)
    
    if parsed.netloc != domain:
        return False
    
    if follow_patterns:
        for pattern in follow_patterns:
            if re.search(pattern, url):
                return True
        return False
    
    return True

def should_ingest_url(url, extract_external=True):
    if not url:
        return False
    
    parsed_url = urlparse(url)
    
    skip_domains = ['web.archive.org', 'archive.today']
    if parsed_url.netloc in skip_domains:
        return False
    
    skip_extensions = ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.css', '.js', 
                      '.ico', '.xml', '.rss', '.zip', '.tar', '.gz']
    if any(parsed_url.path.lower().endswith(ext) for ext in skip_extensions):
        return False
    
    return True

def ingest_url(url, meta, priority=0, source="web_sources"):
    if not should_ingest_url(url):
        return False

    last_modified = None
    try:
        head_resp = requests.head(url, timeout=5, allow_redirects=True)
        last_mod_header = head_resp.headers.get("Last-Modified")
        if last_mod_header:
            from email.utils import parsedate_to_datetime
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
            return True
        else:
            logging.warning(f"Failed to ingest {url}: {resp.status_code} {resp.text}")
    except Exception as e:
        logging.error(f"Error posting to ingest API: {e}")
    
    return False

def crawl_domain(domain, config, cache):
    logging.info(f"Starting crawl of {domain}")
    
    seen_pages = cache.setdefault("seen_pages", {}).setdefault(domain, set())
    seen_links = cache.setdefault("seen_links", set())
    
    if isinstance(seen_pages, list):
        seen_pages = set(seen_pages)
        cache["seen_pages"][domain] = seen_pages
    
    if isinstance(seen_links, list):
        seen_links = set(seen_links)
        cache["seen_links"] = seen_links
    
    to_crawl = [(url, 0) for url in config["start_urls"]]
    crawled_count = 0
    ingested_count = 0
    
    while to_crawl and crawled_count < 100:
        url, depth = to_crawl.pop(0)
        normalized_url = normalize_url(url)
        
        if normalized_url in seen_pages or depth > config.get("max_depth", 2):
            continue
        
        if not can_fetch(url, USER_AGENT, cache):
            logging.info(f"Robots.txt disallows crawling {url}")
            continue
        
        resp = fetch_page(url, config.get("delay", 1.0))
        if not resp:
            continue
        
        seen_pages.add(normalized_url)
        crawled_count += 1
        
        links = extract_links(resp.content, url)
        
        for link_info in links:
            link_url = link_info['url']
            link_hash = hashlib.md5(link_url.encode()).hexdigest()
            
            if link_hash in seen_links:
                continue
            
            seen_links.add(link_hash)
            parsed_link = urlparse(link_url)
            
            if parsed_link.netloc != domain and config.get("extract_external", True):
                meta = {
                    "source_page": url,
                    "source_domain": domain,
                    "link_text": link_info['text'],
                    "link_title": link_info['title'],
                    "source_element": link_info['source_element'],
                    "crawl_depth": depth,
                    "source": "web_sources"
                }
                
                if ingest_url(link_url, meta, priority=config.get("priority", 3), source="web_sources"):
                    ingested_count += 1
            
            elif (parsed_link.netloc == domain and 
                  should_follow_link(link_url, config.get("follow_patterns"), domain) and
                  depth < config.get("max_depth", 2)):
                to_crawl.append((link_url, depth + 1))
    
    logging.info(f"Crawled {crawled_count} pages, ingested {ingested_count} links from {domain}")
    
    if len(seen_pages) > 10000:
        seen_pages = set(list(seen_pages)[-5000:])
        cache["seen_pages"][domain] = seen_pages

def run_crawler():
    cache = load_cache()
    
    if isinstance(cache.get("seen_links"), list):
        cache["seen_links"] = set(cache["seen_links"])
    
    logging.info(f"Loaded cache with {len(cache.get('seen_links', []))} seen links")
    
    while True:
        start_time = time.time()
        logging.info("Starting web crawl cycle")
        
        for domain, config in CRAWL_CONFIG.items():
            try:
                crawl_domain(domain, config, cache)
            except Exception as e:
                logging.error(f"Error crawling {domain}: {e}")
        
        save_cache(cache)
        
        elapsed_time = time.time() - start_time
        logging.info(f"Crawl cycle completed in {elapsed_time:.2f} seconds")
        
        time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    try:
        logging.info("Starting Web Domain Crawler")
        logging.info(f"Configured domains: {list(CRAWL_CONFIG.keys())}")
        logging.info(f"Crawl interval: {SLEEP_INTERVAL} seconds")
        run_crawler()
    except KeyboardInterrupt:
        logging.info("Shutting down web crawler.")