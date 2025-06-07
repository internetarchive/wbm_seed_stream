import requests
import time
import logging
from urllib.parse import urlparse
from email.utils import parsedate_to_datetime

INGEST_API_URL = "http://localhost:8000/api/ingest"
WIKI_API_URL = "https://en.wikipedia.org/w/api.php"
SLEEP_INTERVAL = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# TODO: parallelizability 

def fetch_recent_changes(rccontinue=None):
    params = {
        "action": "query",
        "list": "recentchanges",
        "rcprop": "title|ids|sizes|flags|timestamp",
        "rclimit": 50,
        "format": "json",
    }
    if rccontinue:
        params["rccontinue"] = rccontinue
    try:
        resp = requests.get(WIKI_API_URL, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error(f"Error fetching Wikipedia changes: {e}")
        return None

def fetch_external_links(title, elcontinue=None):
    links = []
    while True:
        params = {
            "action": "query",
            "titles": title,
            "prop": "extlinks",
            "ellimit": "max",
            "format": "json"
        }
        if elcontinue:
            params["elcontinue"] = elcontinue
            
        try:
            resp = requests.get(WIKI_API_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logging.error(f"Error fetching external links for {title}: {e}")
            break

        pages = data.get("query", {}).get("pages", {})
        for page_id, page_data in pages.items():
            extlinks = page_data.get("extlinks", [])
            for link_obj in extlinks:
                links.append(link_obj.get('*'))

        if "continue" in data and "elcontinue" in data["continue"]:
            elcontinue = data["continue"]["elcontinue"]
        else:
            break
            
    return links

def should_ingest_wiki_url(url_path):
    return not (
        url_path.startswith("/wiki/Category:") or
        url_path.startswith("/wiki/User:")
    )
    
def ingest_url(url, meta, priority=0, source="wikipedia"):
    parsed_url = urlparse(url)

    if parsed_url.netloc in ["web.archive.org", "archive.today"]:
        logging.info(f"Skipped (archived link): {url}")
        return

    url_path = parsed_url.path
    if not should_ingest_wiki_url(url_path):
        logging.info(f"Skipped (filtered): {url}")
        return

    last_modified = None
    try:
        head_resp = requests.head(url, timeout=5, allow_redirects=True)
        last_mod_header = head_resp.headers.get("Last-Modified")
        if last_mod_header:
            last_modified = parsedate_to_datetime(last_mod_header).isoformat()
    except Exception as e:
        logging.warning(f"Failed to fetch Last-Modified for {url}: {e}")
        last_modified = None  # explicitly set to None on error

    payload = {
        "url": url,
        "source": source,  # Added: include source in payload
        "meta": meta,
        "priority": priority,
        "last_modified": last_modified  # always include it, even if None
    }

    try:
        resp = requests.post(INGEST_API_URL, json=payload, timeout=5)
        if resp.status_code in (200, 201):
            logging.info(f"Ingested: {url}")
        else:
            logging.warning(f"Failed to ingest {url}: {resp.status_code} {resp.text}")
    except Exception as e:
        logging.error(f"Error posting to ingest API: {e}")

def run_collector():
    rccontinue = None
    while True:
        data = fetch_recent_changes(rccontinue)
        if not data:
            time.sleep(SLEEP_INTERVAL)
            continue

        changes = data.get('query', {}).get('recentchanges', [])
        for change in changes:
            title = change['title']
            wiki_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
            meta = {
                "change": change,
                "source": "wikipedia"
            }
            ingest_url(wiki_url, meta, source="wikipedia")  # Specify source

            ext_links = fetch_external_links(title)
            for ext_link in ext_links:
                ext_meta = {
                    "source_article": wiki_url,
                    "source": "wikipedia_reference"
                }
                ingest_url(ext_link, ext_meta, priority=1, source="wikipedia")  # Specify source

        rccontinue = data.get('continue', {}).get('rccontinue')
        time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    try:
        run_collector()
    except KeyboardInterrupt:
        logging.info("Shutting down Wikipedia collector.")
        
"""
Notes:

The event stream includes virtually all changes across all of wikipedia. Many articles are edited many times a day,
that does not mean that the Internet Archive should try and actually archive those articles every single time there
is a change. Therefore, while in the preliminary stages ingesting all of these URLs is helpful, with time we will need
to determine when there have been significant enough changes to a wikipedia article that a save is warranted. 

For URLs that are added as references to wikipedia however, if it has not been archived before, we should archive it.
If the last modified header is after when the internet archive last saved it, we should archive it. However, I do
believe that this is likely already happening. The reason that I am perhaps not seeing the pages that I am ingesting
to be archived is potentially because:
    - a. the wayback machine is seeing that URL around about the same time that I am seeing it.
    - b. they index wikipedia over perhaps the process of one hour during the day and not in a perhaps real time fashion.
"""