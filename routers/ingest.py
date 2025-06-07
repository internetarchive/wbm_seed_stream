from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from db import SessionLocal
from models import URL
from schemas import URLIngestRequest, URLResponse

router = APIRouter(prefix="/api", tags=["ingest"])
SOURCES = ["wikipedia", "gdelt", "rss", "web_sources"]

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/ingest", response_model=URLResponse)
def ingest_url(req: URLIngestRequest, db: Session = Depends(get_db)):
    
    if req.source not in SOURCES:
        raise HTTPException(status_code=400, detail=f"Invalid source. Must be one of: {', '.join(SOURCES)}")
    
    existing = db.query(URL).filter_by(url=str(req.url), source=req.source).first()
    if existing:
        return existing
    
    url_obj = URL()
    url_obj.url = str(req.url)
    url_obj.source = req.source
    url_obj.meta = req.meta
    url_obj.priority = req.priority or 0
    url_obj.last_modified = req.last_modified
    
    db.add(url_obj)
    db.commit()
    db.refresh(url_obj)
    return url_obj

"""
Notes:
- MediaCloud could serve as a replacement for a large range of rss_collector.py or web_sources_collector.py
  but it is not updated on something like a daily or a weekly basis, which means it may not be suitable for
  finding novel at-risk web content. Besides there is no way to query it for just all new links added, it
  seems like it can be queried solely on the basis of what genre or specific new story we are looking for
  additional information on. This is perhaps something to discuss about how the Internet Archive does it.

- Also the HEAD requests to check the last_modified date is absolutely killing the rate at which I could be
  ingesting URLs. I need to probably ignore this during the initial ingestion phase and focus on it later,
  or have some sort of side-job running to do this after URLs have already hit the database so I can still
  ingest a crap-ton of URLs at the same time.
"""