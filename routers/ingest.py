from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from db import SessionLocal
from models import URL
from schemas import URLIngestRequest, URLResponse

router = APIRouter(prefix="/api/ingest", tags=["ingest"])
SOURCES = ["wikipedia", "spn", "mediacloud", "gdelt"]

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def ingest_url(source: str, req: URLIngestRequest, db: Session):
    if source not in SOURCES:
        raise HTTPException(status_code=400, detail="Invalid source")
    
    existing = db.query(URL).filter_by(url=req.url, source=source).first()
    if existing:
        return existing
    
    url_obj = URL()
    url_obj.url = str(req.url)
    url_obj.source = source
    url_obj.meta = req.meta
    url_obj.priority = req.priority or 0
    db.add(url_obj)
    db.commit()
    db.refresh(url_obj)
    return url_obj

@router.post("/wikipedia", response_model=URLResponse)
def ingest_wikipedia(req: URLIngestRequest, db: Session = Depends(get_db)):
    return ingest_url("wikipedia", req, db)

@router.post("/spn", response_model=URLResponse)
def ingest_spn(req: URLIngestRequest, db: Session = Depends(get_db)):
    return ingest_url("spn", req, db)

@router.post("/mediacloud", response_model=URLResponse)
def ingest_mediacloud(req: URLIngestRequest, db: Session = Depends(get_db)):
    return ingest_url("mediacloud", req, db)

@router.post("/gdelt", response_model=URLResponse)
def ingest_gdelt(req: URLIngestRequest, db: Session = Depends(get_db)):
    return ingest_url("gdelt", req, db)