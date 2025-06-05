from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from db import SessionLocal
from models import URL
from schemas import URLIngestRequest, URLResponse

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def ingest_url(source: str, req: URLIngestRequest, db: Session):
    existing = db.query(URL).filter_by(url=str(req.url), source=source).first()
    if existing:
        return existing
    
    url_obj = URL()
    url_obj.url = str(req.url)
    url_obj.source = source
    url_obj.meta = req.meta
    url_obj.priority = req.priority or 0
    url_obj.last_modified = req.last_modified
    db.add(url_obj)
    db.commit()
    db.refresh(url_obj)
    return url_obj

@router.post("/mediacloud", response_model=URLResponse)
def ingest_mediacloud(req: URLIngestRequest, db: Session = Depends(get_db)):
    return ingest_url("mediacloud", req, db)
