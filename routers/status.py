from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from db import SessionLocal
from models import URL
from schemas import StatusResponse, StatsResponse
from sqlalchemy import func

router = APIRouter(prefix="/api", tags=["status"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/urls/{id}/status", response_model=StatusResponse)
def get_status(id: int, db: Session = Depends(get_db)):
    url = db.query(URL).filter_by(id=id).first()
    if not url:
        raise HTTPException(status_code=404, detail="URL not found")
    return StatusResponse(id=url.id, status=url.status)
    
@router.get("/stats", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    total = db.query(func.count(URL.id)).scalar() or 0
    print("func =", func)
    print("type(func) =", type(func))
    by_source_result = db.query(URL.source, func.count(URL.id)).group_by(URL.source).all() or [] # type: ignore
    by_status_result = db.query(URL.status, func.count(URL.id)).group_by(URL.status).all() or [] # type: ignore

    by_source = dict(by_source_result)
    by_status = dict(by_status_result)

    return StatsResponse(total=total, by_source=by_source, by_status=by_status)
