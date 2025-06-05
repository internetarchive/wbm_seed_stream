import requests
import zipfile
import io
import csv
import time
import logging

GDELT_BASE = "https://data.gdeltproject.org/gdeltv2/"
LAST_UPDATE_URL = GDELT_BASE + "lastupdate.txt"
INGEST_API_URL = "http://localhost:8000/api/ingest/gdelt"

def fetch_last_update_files():
    resp = requests.get(LAST_UPDATE_URL)
    resp.raise_for_status()
    return [line.strip() for line in resp.text.strip().splitlines()]

def download_and_extract_csv(zip_filename):
    url = GDELT_BASE + zip_filename
    resp = requests.get(url)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            return list(csv.reader(io.TextIOWrapper(f, 'utf-8', errors='ignore')))

def ingest_url(url, meta=None):
    payload = {
        "url": url,
        "meta": meta or {},
        "priority": 1
    }
    try:
        resp = requests.post(INGEST_API_URL, json=payload)
        if resp.ok:
            logging.info(f"Ingested {url}")
        else:
            logging.warning(f"Failed to ingest {url}: {resp.status_code}")
    except Exception as e:
        logging.error(f"Error ingesting {url}: {e}")

def extract_urls_from_gkg(rows):
    for row in rows:
        if len(row) > 4:
            url = row[4]  # Column 5 = DocumentIdentifier
            if url.startswith("http"):
                yield url

def run_ingestion():
    filenames = fetch_last_update_files()
    gkg_file = next(f for f in filenames if f.endswith(".gkg.csv.zip"))
    rows = download_and_extract_csv(gkg_file)
    for url in extract_urls_from_gkg(rows):
        ingest_url(url, meta={"source": "gdelt"})

if __name__ == "__main__":
    while True:
        try:
            run_ingestion()
        except Exception as e:
            logging.error(f"Pipeline error: {e}")
        time.sleep(15 * 60)  # wait 15 minutes
