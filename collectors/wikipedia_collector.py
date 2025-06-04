import requests
import time

INGEST_API_URL = "https://localhost:8000/api/ingest/wikipedia"

def fetch_wikipedia_recent_changes():
    response = requests.get("https://en.wikipedia.org/w/api.php?action=query&list=recentchanges&rcprop=title|ids|sizes|flags&format=json")
    data = response.json()
    changes = data.get('query', {}).get('recentchanges', [])
    for change in changes:
        url = f"https://en.wikipedia.org/wiki/{change['title'].replace(' ', '_')}"
        payload = {
            "url": url,
            "meta": "wikipedia recent change",
            "priority": 0
        }
        requests.post(INGEST_API_URL, json=payload)

if __name__ == "__main__":
    while True:
        fetch_wikipedia_recent_changes()
        time.sleep(60)