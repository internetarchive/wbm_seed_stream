import requests
import time
import gzip
import json
import os
import csv
import argparse
from datetime import datetime, timedelta
from urllib.parse import urlparse
import re
from dotenv import load_dotenv

from testing.profiler_integration import get_active_profiler, profile_spark_stage, log_file_operation

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

WIKI_API_URL = "https://en.wikipedia.org/w/api.php"
MEDIACLOUD_BASE_URL = os.getenv('MEDIACLOUD_URL')
SLEEP_INTERVAL = 2

def parse_args():
    parser = argparse.ArgumentParser(description="URL Collector Script")
    parser.add_argument(
        '--wiki',
        type=int,
        default=1,
        help="Number of days of Wikipedia data to process (0 to skip)"
    )
    parser.add_argument(
        '--mediacloud',
        type=int,
        default=1,
        help="Number of days of MediaCloud data to process (0 to skip)"
    )
    args = parser.parse_args()

    return args.wiki, args.mediacloud

def main():
    wiki_days, mediacloud_days = parse_args()
    profiler = get_active_profiler()
    if profiler:
        profiler.log_process_event(
            "good_data_start",
            f"Starting good data collection: wiki={wiki_days}, mediacloud={mediacloud_days}"
        )

    output_dir = "data/storage/good_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    yesterday = datetime.now() - timedelta(days=1)
    output_filename = f"combined-links-{yesterday.strftime('%Y-%m-%d')}.tsv"
    output_path = os.path.join(output_dir, output_filename)

    print(
        f"Starting collection. Wikipedia days: {wiki_days},"
        f" MediaCloud days: {mediacloud_days}"
    )

    total_processed = 0

    with open(output_path, 'w', newline='', encoding='utf-8') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')
        writer.writerow(
            ['url', 'source_uri', 'timestamp', 'user_text', 'source_type', 'metadata']
        )
        
        if wiki_days > 0:
            with profile_spark_stage(profiler, "wikipedia_data_collection"):
                print(f"Fetching Wikipedia data for {wiki_days} days.")
                for day_offset in range(wiki_days):
                    date = yesterday - timedelta(days=day_offset)
                    wiki_filename = (
                        f"wiki-eventstream-added-links-{date.strftime('%Y-%m-%d')}.tsv"
                    )
                    wiki_filepath = os.path.join(output_dir, wiki_filename)

                    wikipedia_count = 0

                    if os.path.exists(wiki_filepath):
                        try:
                            if profiler:
                                file_size = os.path.getsize(wiki_filepath)
                                log_file_operation(profiler, "read", wiki_filepath, file_size)

                            with open(wiki_filepath, 'r', encoding='utf-8') as wiki_file:
                                for line_num, line in enumerate(wiki_file, 1):
                                    line = line.strip()
                                    if not line:
                                        continue
                                    parts = line.split('\t')
                                    if len(parts) < 4:
                                        continue
                                    url = parts[0]
                                    if "wikipedia.org" in url:
                                        continue
                                    source_uri = parts[1]
                                    timestamp = parts[2]
                                    user_text = parts[3]
                                    metadata = {
                                        "source_article": source_uri,
                                        "original_source": "wiki_eventstream",
                                    }
                                    row = [
                                        url,
                                        source_uri,
                                        timestamp,
                                        user_text,
                                        "wikipedia",
                                        json.dumps(metadata),
                                    ]
                                    writer.writerow(row)
                                    wikipedia_count += 1
                        except Exception as e:
                            print(f"Error processing wiki file: {e}")
                        try:
                            os.remove(wiki_filepath)
                        except Exception as e:
                            print(f"Error deleting wiki file: {e}")

                        total_processed += wikipedia_count
                    else:
                        if profiler:
                            profiler.log_process_event(
                                "wiki_api_fetch",
                                f"Fetching Wikipedia data via API for {date.strftime('%Y-%m-%d')}",
                            )

                        rccontinue = None
                        for batch in range(5):
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
                                print(f"Error fetching changes: {e}")
                                break
                            changes = data.get('query', {}).get('recentchanges', [])
                            for change in changes:
                                title = change['title']
                                user = change.get('user', 'Unknown')
                                timestamp = change['timestamp']
                                wiki_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
                                parsed_url = urlparse(wiki_url)
                                if parsed_url.path.startswith("/wiki/Category:") or parsed_url.path.startswith("/wiki/User:"):
                                    continue
                                elcontinue = None
                                while True:
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
                                        el_resp = requests.get(
                                            WIKI_API_URL, params=el_params, timeout=10
                                        )
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
                                            if ext_link:
                                                parsed_ext = urlparse(ext_link)
                                                if parsed_ext.netloc not in [
                                                    "web.archive.org",
                                                    "archive.today",
                                                    "archive.is",
                                                    "wikipedia.org",
                                                ]:
                                                    ext_meta = {
                                                        "source_article": wiki_url,
                                                        "article_title": title,
                                                        "original_source": "recent_changes_api",
                                                    }
                                                    row = [
                                                        ext_link,
                                                        wiki_url,
                                                        timestamp,
                                                        user,
                                                        "wikipedia",
                                                        json.dumps(ext_meta),
                                                    ]
                                                    writer.writerow(row)
                                                    wikipedia_count += 1
                                    if (
                                        "continue" in el_data
                                        and "elcontinue" in el_data["continue"]
                                    ):
                                        elcontinue = el_data["continue"]["elcontinue"]
                                    else:
                                        break
                            rccontinue = data.get('continue', {}).get('rccontinue')
                            if not rccontinue:
                                break
                            time.sleep(SLEEP_INTERVAL)
                        total_processed += wikipedia_count
        else:
            print("Skipping Wikipedia data collection as --wiki is set to 0.")

        if mediacloud_days > 0:
            with profile_spark_stage(profiler, "mediacloud_data_collection"):
                print(f"Fetching MediaCloud data for {mediacloud_days} days.")
                dates = [
                    (yesterday - timedelta(days=i)).strftime("%Y-%m-%d")
                    for i in range(mediacloud_days)
                ]
                mediacloud_count = 0
                for date in dates:
                    if profiler:
                        profiler.log_process_event(
                            "mediacloud_fetch", f"Fetching MediaCloud data for {date}"
                        )

                    if not MEDIACLOUD_BASE_URL:
                        print("MEDIACLOUD_URL not set. Skipping MediaCloud fetch.")
                        break 
                    url = MEDIACLOUD_BASE_URL.format(date=date)
                    try:
                        resp = requests.get(url, timeout=30)
                        resp.raise_for_status()
                        decompressed_data = gzip.decompress(resp.content)
                        rss_content = decompressed_data.decode('utf-8')
                    except Exception as e:
                        print(f"Error fetching or decompressing MediaCloud for {date}: {e}")
                        continue
                try:
                    items = re.findall(r'<item>.*?</item>', rss_content, re.DOTALL)
                    for item_str in items:
                        link_match = re.search(r'<link>(.*?)</link>', item_str)
                        if not link_match:
                            continue
                        article_url = link_match.group(1).replace('&amp;', '&')
                        if "wikipedia.org" in article_url:
                            continue
                        title_match = re.search(r'<title>(.*?)</title>', item_str)
                        pubdate_match = re.search(r'<pubDate>(.*?)</pubDate>', item_str)
                        domain_match = re.search(r'<domain>(.*?)</domain>', item_str)
                        source_match = re.search(
                            r'<source url="([^"]*)"[^>]*mcFeedId="([^"]*)"[^>]*mcSourceId="([^"]*)"',
                            item_str,
                        )
                        parsed_mc = urlparse(article_url)
                        if parsed_mc.netloc not in [
                            "web.archive.org",
                            "archive.today",
                            "archive.is",
                        ]:
                            meta = {
                                "title": title_match.group(1) if title_match else '',
                                "domain": domain_match.group(1) if domain_match else '',
                                "mc_feed_id": source_match.group(2)
                                if source_match
                                else '',
                                "mc_source_id": source_match.group(3)
                                if source_match
                                else '',
                            }
                            timestamp = (
                                pubdate_match.group(1)
                                if pubdate_match
                                else datetime.now().isoformat()
                            )
                            source_uri = source_match.group(1) if source_match else "mediacloud"
                            row = [
                                article_url,
                                source_uri,
                                timestamp,
                                "mediacloud",
                                "mediacloud",
                                json.dumps(meta),
                            ]
                            writer.writerow(row)
                            mediacloud_count += 1
                except Exception as e:
                    print(f"Error parsing MediaCloud content: {e}")
                    time.sleep(SLEEP_INTERVAL)
                total_processed += mediacloud_count
        else:
            print("Skipping MediaCloud data collection as --mediacloud is set to 0.")

    print(f"Finished collection. Total URLs: {total_processed}")

    if profiler:
        try:
            file_size = os.path.getsize(output_path)
            log_file_operation(profiler, "write", output_path, file_size)
            profiler.log_process_event(
                "good_data_end", f"Good data collection completed: {total_processed} URLs"
            )
        except Exception as e:
            print(f"Error logging final file operation: {e}")

    try:
        with open(output_path, 'r', encoding='utf-8') as f:
            lines = sum(1 for line in f) - 1
            print(f"Final TSV rows: {lines}")
    except Exception as e:
        print(f"Error counting rows: {e}")

if __name__ == "__main__":
    main()