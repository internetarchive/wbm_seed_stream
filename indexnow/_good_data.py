#!/usr/bin/env python3
import requests
import time
import logging
import gzip
import json
import os
import csv
import argparse
from datetime import datetime, timedelta
from urllib.parse import urlparse
import re
from dotenv import load_dotenv

load_dotenv('../.env')

WIKI_API_URL = "https://en.wikipedia.org/w/api.php"
MEDIACLOUD_BASE_URL = os.getenv('MEDIACLOUD_URL')
DAYS_TO_PROCESS = 1
SLEEP_INTERVAL = 2

parser = argparse.ArgumentParser(description='Collect URLs from Wikipedia and MediaCloud')
parser.add_argument('--date', type=str, help='Date to process (YYYY-MM-DD format). Defaults to yesterday.')
parser.add_argument('--days', type=int, default=1, help='Number of days to process (default: 1)')
args = parser.parse_args()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

output_dir = "data/_good_data"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    logging.info(f"Created output directory: {output_dir}")

if args.date:
    try:
        target_date = datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError:
        logging.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD format.")
        exit(1)
else:
    target_date = datetime.now() - timedelta(days=1)

output_filename = f"combined-links-{target_date.strftime('%Y-%m-%d')}.tsv"
output_path = os.path.join(output_dir, output_filename)

logging.info(f"Starting combined collector for date: {target_date.strftime('%Y-%m-%d')}")
logging.info(f"Output will be saved to: {output_path}")

total_processed = 0

with open(output_path, 'w', newline='', encoding='utf-8') as tsvfile:
    writer = csv.writer(tsvfile, delimiter='\t')
    writer.writerow(['url', 'source_uri', 'timestamp', 'user_text', 'source_type', 'metadata'])
    
    wiki_filename = f"wiki-eventstream-added-links-{target_date.strftime('%Y-%m-%d')}.tsv"
    wiki_filepath = os.path.join(output_dir, wiki_filename)
    
    if os.path.exists(wiki_filepath):
        logging.info(f"Found existing wiki eventstream file: {wiki_filename}")
        wikipedia_count = 0
        
        try:
            with open(wiki_filepath, 'r', encoding='utf-8') as wiki_file:
                for line_num, line in enumerate(wiki_file, 1):
                    line = line.strip()
                    if not line:
                        continue
                        
                    parts = line.split('\t')
                    if len(parts) < 4:
                        logging.warning(f"Skipping malformed line {line_num} in wiki file: {line}")
                        continue
                    
                    url = parts[0]
                    source_uri = parts[1]
                    timestamp = parts[2]
                    user_text = parts[3]
                    
                    metadata = {
                        "source_article": source_uri,
                        "original_source": "wiki_eventstream"
                    }
                    
                    row = [url, source_uri, timestamp, user_text, "wikipedia", json.dumps(metadata)]
                    writer.writerow(row)
                    wikipedia_count += 1
                    
        except Exception as e:
            logging.error(f"Error processing wiki eventstream file: {e}")
            
        logging.info(f"Processed {wikipedia_count} URLs from wiki eventstream file")
        total_processed += wikipedia_count
        
    else:
        logging.info(f"No wiki eventstream file found for {target_date.strftime('%Y-%m-%d')}")
        logging.info("Processing Wikipedia recent changes...")
        
        rccontinue = None
        wikipedia_count = 0
        
        for batch in range(10):
            params = {
                "action": "query",
                "list": "recentchanges",
                "rcprop": "title|ids|sizes|flags|timestamp|user",
                "rclimit": 50,
                "format": "json",
            }
            if rccontinue:
                params["rccontinue"] = rccontinue
            
            try:
                resp = requests.get(WIKI_API_URL, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logging.error(f"Error fetching Wikipedia changes: {e}")
                break

            if not data:
                break

            changes = data.get('query', {}).get('recentchanges', [])
            for change in changes:
                title = change['title']
                user = change.get('user', 'Unknown')
                timestamp = change['timestamp']
                
                wiki_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
                wiki_meta = {
                    "change_id": change.get('rcid'),
                    "old_len": change.get('oldlen'),
                    "new_len": change.get('newlen'),
                    "original_source": "recent_changes_api"
                }
                
                parsed_url = urlparse(wiki_url)
                if not (parsed_url.path.startswith("/wiki/Category:") or parsed_url.path.startswith("/wiki/User:")):
                    row = [wiki_url, wiki_url, timestamp, user, "wikipedia", json.dumps(wiki_meta)]
                    writer.writerow(row)
                    wikipedia_count += 1

                elcontinue = None
                while True:
                    el_params = {
                        "action": "query",
                        "titles": title,
                        "prop": "extlinks",
                        "ellimit": "max",
                        "format": "json"
                    }
                    if elcontinue:
                        el_params["elcontinue"] = elcontinue

                    try:
                        el_resp = requests.get(WIKI_API_URL, params=el_params, timeout=10)
                        el_resp.raise_for_status()
                        el_data = el_resp.json()
                    except Exception as e:
                        logging.error(f"Error fetching external links for {title}: {e}")
                        break

                    pages = el_data.get("query", {}).get("pages", {})
                    for page_id, page_data in pages.items():
                        extlinks = page_data.get("extlinks", [])
                        for link_obj in extlinks:
                            ext_link = link_obj.get('*')
                            if ext_link:
                                parsed_ext = urlparse(ext_link)
                                if parsed_ext.netloc not in ["web.archive.org", "archive.today", "archive.is"]:
                                    ext_meta = {
                                        "source_article": wiki_url,
                                        "article_title": title,
                                        "original_source": "recent_changes_api"
                                    }
                                    row = [ext_link, wiki_url, timestamp, user, "wikipedia", json.dumps(ext_meta)]
                                    writer.writerow(row)
                                    wikipedia_count += 1

                    if "continue" in el_data and "elcontinue" in el_data["continue"]:
                        elcontinue = el_data["continue"]["elcontinue"]
                    else:
                        break

            rccontinue = data.get('continue', {}).get('rccontinue')
            if not rccontinue:
                break
                
            time.sleep(SLEEP_INTERVAL)
        
        logging.info(f"Processed {wikipedia_count} Wikipedia URLs from recent changes")
        total_processed += wikipedia_count

    logging.info("Processing MediaCloud data...")
    
    start_date = target_date
    dates = []
    for i in range(args.days):
        date = start_date - timedelta(days=i)
        dates.append(date.strftime("%Y-%m-%d"))
    
    mediacloud_count = 0
    
    for date in dates:
        url = MEDIACLOUD_BASE_URL.format(date=date) 
        
        try:
            logging.info(f"Fetching MediaCloud RSS for {date}")
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            
            decompressed_data = gzip.decompress(resp.content)
            rss_content = decompressed_data.decode('utf-8')
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching MediaCloud RSS for {date}: {e}")
            continue
        except Exception as e:
            logging.error(f"Error decompressing RSS data for {date}: {e}")
            continue

        if not rss_content:
            logging.error(f"Failed to fetch RSS data for {date}")
            continue
        
        try:
            item_pattern = r'<item>.*?</item>'
            items = re.findall(item_pattern, rss_content, re.DOTALL)
            
            for item_str in items:
                link_match = re.search(r'<link>(.*?)</link>', item_str)
                if not link_match:
                    continue
                    
                article_url = link_match.group(1).replace('&amp;', '&')
                
                title_match = re.search(r'<title>(.*?)</title>', item_str)
                pubdate_match = re.search(r'<pubDate>(.*?)</pubDate>', item_str)
                domain_match = re.search(r'<domain>(.*?)</domain>', item_str)
                source_match = re.search(r'<source url="([^"]*)"[^>]*mcFeedId="([^"]*)"[^>]*mcSourceId="([^"]*)"', item_str)
                
                parsed_mc = urlparse(article_url)
                if parsed_mc.netloc not in ["web.archive.org", "archive.today", "archive.is"]:
                    meta = {
                        "title": title_match.group(1) if title_match else '',
                        "domain": domain_match.group(1) if domain_match else '',
                        "mc_feed_id": source_match.group(2) if source_match else '',
                        "mc_source_id": source_match.group(3) if source_match else ''
                    }
                    
                    timestamp = pubdate_match.group(1) if pubdate_match else datetime.now().isoformat()
                    source_uri = source_match.group(1) if source_match else "mediacloud"
                    
                    row = [article_url, source_uri, timestamp, "mediacloud", "mediacloud", json.dumps(meta)]
                    writer.writerow(row)
                    mediacloud_count += 1
                
        except Exception as e:
            logging.error(f"Error parsing RSS content: {e}")
        
        time.sleep(SLEEP_INTERVAL)
    
    logging.info(f"Processed {mediacloud_count} MediaCloud URLs")
    total_processed += mediacloud_count

if wiki_filepath and os.path.exists(wiki_filepath):
    try:
        os.remove(wiki_filepath)
        logging.info(f"Deleted wiki eventstream file: {wiki_filepath}")
    except Exception as e:
        logging.error(f"Error deleting wiki eventstream file: {e}")

logging.info(f"Collection completed. Total URLs processed: {total_processed}")
logging.info(f"Output saved to: {output_path}")

try:
    with open(output_path, 'r', encoding='utf-8') as f:
        lines = sum(1 for line in f) - 1
        logging.info(f"Final TSV file contains {lines} data rows")
        
except Exception as e:
    logging.error(f"Error counting rows in file: {e}")