import requests
import time
import gzip
import json
import os
import csv
import argparse
from datetime import datetime, timedelta, date
from urllib.parse import urlparse
import re
from dotenv import load_dotenv

from .profiler_integration import get_active_profiler, profile_spark_stage, log_file_operation

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

WIKI_API_URL = "https://en.wikipedia.org/w/api.php"
MEDIACLOUD_BASE_URL = os.getenv('MEDIACLOUD_URL')
SLEEP_INTERVAL = 2

def parse_args():
    parser = argparse.ArgumentParser(description="URL Collector Script")
    parser.add_argument('--wiki', type=int, default=1, help="Number of days of Wikipedia data to process (0 to skip)")
    parser.add_argument('--mediacloud', type=int, default=1, help="Number of days of MediaCloud data to process (0 to skip)")
    args = parser.parse_args()
    return args.wiki, args.mediacloud

def get_mediacloud_data_for_date(target_date, writer):
    mediacloud_count = 0

    if not MEDIACLOUD_BASE_URL:
        print("MEDIACLOUD_URL not set in environment")
        return 0

    date_str = target_date.strftime("%Y-%m-%d")
    url = MEDIACLOUD_BASE_URL.format(date=date_str)

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        decompressed_data = gzip.decompress(resp.content)
        rss_content = decompressed_data.decode('utf-8')

        items = re.findall(r'<item>.*?</item>', rss_content, re.DOTALL)
        for item_str in items:
            link_match = re.search(r'<link>(.*?)</link>', item_str)
            if not link_match:
                continue

            article_url = link_match.group(1).replace('&amp;', '&')
            if "wikipedia.org" in article_url:
                continue

            parsed_mc = urlparse(article_url)
            if parsed_mc.netloc in ["web.archive.org", "archive.today", "archive.is"]:
                continue

            if not parsed_mc.scheme in ['http', 'https']:
                continue

            title_match = re.search(r'<title>(.*?)</title>', item_str)
            pubdate_match = re.search(r'<pubDate>(.*?)</pubDate>', item_str)
            domain_match = re.search(r'<domain>(.*?)</domain>', item_str)
            source_match = re.search(r'<source url="([^"]*)"[^>]*mcFeedId="([^"]*)"[^>]*mcSourceId="([^"]*)"', item_str)

            meta = {
                "title": title_match.group(1) if title_match else '',
                "domain": domain_match.group(1) if domain_match else '',
                "mc_feed_id": source_match.group(2) if source_match else '',
                "mc_source_id": source_match.group(3) if source_match else '',
            }

            timestamp = pubdate_match.group(1) if pubdate_match else target_date.isoformat()
            source_uri = source_match.group(1) if source_match else "mediacloud"

            row = [article_url, source_uri, timestamp, "mediacloud", "mediacloud", json.dumps(meta)]
            writer.writerow(row)
            mediacloud_count += 1

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"MediaCloud data not available for {date_str} (404)")
        else:
            print(f"HTTP error fetching MediaCloud data for {date_str}: {e}")
    except Exception as e:
        print(f"Error processing MediaCloud data for {date_str}: {e}")

    return mediacloud_count

def get_wikipedia_data_for_date(target_date, writer):
    wikipedia_count = 0

    start_date = target_date.strftime('%Y-%m-%dT00:00:00Z')
    end_date = (target_date + timedelta(days=1)).strftime('%Y-%m-%dT00:00:00Z')

    rccontinue = None
    for batch in range(10):
        params = {
            "action": "query",
            "list": "recentchanges",
            "rcprop": "title|ids|sizes|flags|timestamp|user",
            "rclimit": 100,
            "format": "json",
            "rcstart": end_date,
            "rcend": start_date,
            "rcdir": "older"
        }
        if rccontinue:
            params["rccontinue"] = rccontinue

        try:
            resp = requests.get(WIKI_API_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"Error fetching changes for {target_date}: {e}")
            break

        changes = data.get('query', {}).get('recentchanges', [])
        if not changes:
            break

        for change in changes:
            title = change['title']
            user = change.get('user', 'Unknown')
            timestamp = change['timestamp']

            if title.startswith('Category:') or title.startswith('User:') or title.startswith('Talk:'):
                continue

            wiki_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"

            elcontinue = None
            link_count = 0
            while link_count < 5:
                el_params = {
                    "action": "query",
                    "titles": title,
                    "prop": "extlinks",
                    "ellimit": "max",
                    "format": "json",
                }
                if elcontinue:
                    el_params["elcontinue"] = elcontinue

                try:
                    el_resp = requests.get(WIKI_API_URL, params=el_params, timeout=10)
                    el_resp.raise_for_status()
                    el_data = el_resp.json()
                except Exception as e:
                    print(f"Error fetching links for {title}: {e}")
                    break

                pages = el_data.get("query", {}).get("pages", {})
                for page_id, page_data in pages.items():
                    extlinks = page_data.get("extlinks", [])
                    for link_obj in extlinks:
                        ext_link = link_obj.get('*')
                        if ext_link and link_count < 5:
                            parsed_ext = urlparse(ext_link)
                            if parsed_ext.netloc not in ["web.archive.org", "archive.today", "archive.is", "wikipedia.org", "en.wikipedia.org", "commons.wikimedia.org"] and parsed_ext.scheme in ['http', 'https']:
                                ext_meta = {
                                    "source_article": wiki_url,
                                    "article_title": title,
                                    "original_source": "recent_changes_api",
                                }
                                row = [ext_link, wiki_url, timestamp, user, "wikipedia", json.dumps(ext_meta)]
                                writer.writerow(row)
                                wikipedia_count += 1
                                link_count += 1

                if "continue" in el_data and "elcontinue" in el_data["continue"]:
                    elcontinue = el_data["continue"]["elcontinue"]
                else:
                    break

        rccontinue = data.get('continue', {}).get('rccontinue')
        if not rccontinue:
            break
        time.sleep(SLEEP_INTERVAL)

    return wikipedia_count

def main():
    wiki_days, mediacloud_days = parse_args()
    profiler = get_active_profiler()

    if profiler:
        profiler.log_process_event("good_data_start", f"Starting good data collection: wiki={wiki_days}, mediacloud={mediacloud_days}")

    output_dir = "data/storage/good_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    today = date.today()
    yesterday = today - timedelta(days=1)

    print(f"Today: {today}")
    print(f"Yesterday: {yesterday}")

    output_filename = f"combined-links-{yesterday.strftime('%Y-%m-%d')}.tsv"
    output_path = os.path.join(output_dir, output_filename)

    total_processed = 0

    with open(output_path, 'w', newline='', encoding='utf-8') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')
        writer.writerow(['url', 'source_uri', 'timestamp', 'user_text', 'source_type', 'metadata'])

        if wiki_days > 0:
            with profile_spark_stage(profiler, "wikipedia_data_collection"):
                for day_offset in range(wiki_days):
                    target_date = yesterday - timedelta(days=day_offset)
                    print(f"Fetching Wikipedia data for day {day_offset + 1}/{wiki_days}: {target_date}")

                    if profiler:
                        profiler.log_process_event("wiki_api_fetch", f"Fetching Wikipedia data for {target_date.strftime('%Y-%m-%d')}")

                    wikipedia_count = get_wikipedia_data_for_date(target_date, writer)
                    total_processed += wikipedia_count
                    print(f"Wikipedia: {wikipedia_count} URLs for {target_date}")
        else:
            print("Skipping Wikipedia data collection")

        if mediacloud_days > 0:
            with profile_spark_stage(profiler, "mediacloud_data_collection"):
                for day_offset in range(mediacloud_days):
                    target_date = yesterday - timedelta(days=day_offset)
                    print(f"Fetching MediaCloud data for day {day_offset + 1}/{mediacloud_days}: {target_date}")

                    if profiler:
                        profiler.log_process_event("mediacloud_fetch", f"Fetching MediaCloud data for {target_date.strftime('%Y-%m-%d')}")

                    mediacloud_count = get_mediacloud_data_for_date(target_date, writer)
                    total_processed += mediacloud_count
                    print(f"MediaCloud: {mediacloud_count} URLs for {target_date}")
                    time.sleep(SLEEP_INTERVAL)
        else:
            print("Skipping MediaCloud data collection")

    print(f"Total URLs collected: {total_processed}")

    if profiler:
        try:
            file_size = os.path.getsize(output_path)
            log_file_operation(profiler, "write", output_path, file_size)
            profiler.log_process_event("good_data_end", f"Good data collection completed: {total_processed} URLs")
        except Exception as e:
            print(f"Error logging file operation: {e}")

    try:
        with open(output_path, 'r', encoding='utf-8') as f:
            lines = sum(1 for line in f) - 1
            print(f"Final TSV rows: {lines}")
    except Exception as e:
        print(f"Error counting rows: {e}")

if __name__ == "__main__":
    main()
