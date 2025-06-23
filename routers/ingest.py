from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import List, Optional, Generator
from db import SessionLocal
from models import URL
from schemas import URLIngestRequest, URLResponse


router = APIRouter(prefix="/api", tags=["ingest"])
SOURCES = ["wikipedia", "gdelt", "rss", "web_sources", "reddit", "mediacloud"]


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        
@router.post("/ingest", response_model=URLResponse)
def ingest_url(req: URLIngestRequest, response: Response, db: Session = Depends(get_db)):

    if req.source not in SOURCES:
        raise HTTPException(status_code=400, detail=f"Invalid source. Must be one of: {', '.join(SOURCES)}")

    existing = db.query(URL).filter(URL.url == str(req.url)).first() # type: ignore

    if existing:
        one_week_ago = datetime.utcnow() - timedelta(weeks=1)

        if existing.last_modified and existing.last_modified < one_week_ago:
            existing.source = req.source
            existing.meta = req.meta
            existing.priority = req.priority or existing.priority
            existing.last_modified = req.last_modified

            db.commit()
            db.refresh(existing)
            response.status_code = 204
            return existing
        else:
            raise HTTPException(
                status_code=409, 
                detail=f"URL already exists: {req.url}"
            )

    url_obj = URL()
    url_obj.url = str(req.url)
    url_obj.source = req.source
    url_obj.meta = req.meta
    url_obj.priority = req.priority or 0
    url_obj.last_modified = req.last_modified

    db.add(url_obj)
    db.commit()
    db.refresh(url_obj)

    response.status_code = 201
    return url_obj
@router.get("/ingest/{url_id}", response_model=URLResponse)
def get_ingested_url(url_id: int, db: Session = Depends(get_db)):
    url_obj = db.query(URL).filter(URL.id == url_id).first() # pyright: ignore[reportOptionalCall]
    if not url_obj:
        raise HTTPException(
            status_code=404, 
            detail=f"URL with ID {url_id} not found"
        )
    return url_obj
    
@router.get("/ingest", response_model=List[URLResponse])
def get_ingested_urls(
    skip: int = 0, 
    limit: int = 100, 
    source: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Retrieve multiple ingested URLs with optional filtering and pagination
    """
    query = db.query(URL)

    if source:
        if source not in SOURCES:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid source filter. Must be one of: {', '.join(SOURCES)}"
            )
        query = query.filter(URL.source == source) # pyright: ignore[reportOptionalCall]

    urls = query.offset(skip).limit(limit).all()  # pyright: ignore[reportOptionalCall]
    return urls
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