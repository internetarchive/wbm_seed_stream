from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime, timedelta
from typing import List, Optional, Generator
from db import SessionLocal
from models import URL, FilterRule, BlockedURL
from schemas import (
    URLIngestRequest, URLResponse,
    FilterRuleRequest, FilterRuleResponse,
    BlockedURLRequest, BlockedURLResponse
)

router = APIRouter(prefix="/api", tags=["ingest"])

SOURCES = ["wikipedia", "gdelt", "rss", "web_sources", "reddit", "mediacloud"]
RULE_TYPES = ["domain_block", "regex", "wildcard", "keyword", "exact_match"]
BLOCK_REASONS = ["badware", "spam", "malware", "phishing", "adult", "fraud", "suspicious"]

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
    
    existing = db.query(URL).filter(URL.url == str(req.url)).first() 
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
    url_obj = db.query(URL).filter(URL.id == url_id).first() 
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
    query = db.query(URL)
    if source:
        if source not in SOURCES:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid source filter. Must be one of: {', '.join(SOURCES)}"
            )
        query = query.filter(URL.source == source) 
    urls = query.offset(skip).limit(limit).all() 
    return urls

@router.post("/filter-rules", response_model=FilterRuleResponse)
def create_filter_rule(req: FilterRuleRequest, response: Response, db: Session = Depends(get_db)):
    if req.rule_type not in RULE_TYPES:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid rule_type. Must be one of: {', '.join(RULE_TYPES)}"
        )
    
    existing = db.query(FilterRule).filter(
        FilterRule.pattern == req.pattern,
        FilterRule.rule_type == req.rule_type
    ).first() 
    
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Filter rule with pattern '{req.pattern}' and type '{req.rule_type}' already exists"
        )
    
    rule_obj = FilterRule()
    rule_obj.pattern = req.pattern
    rule_obj.rule_type = req.rule_type
    rule_obj.source_file = req.source_file
    rule_obj.line_number = req.line_number
    rule_obj.modifiers = req.modifiers
    rule_obj.description = req.description
    rule_obj.is_active = req.is_active if req.is_active is not None else True
    rule_obj.confidence_score = req.confidence_score
    
    db.add(rule_obj)
    db.commit()
    db.refresh(rule_obj)
    response.status_code = 201
    return rule_obj

@router.get("/filter-rules/{rule_id}", response_model=FilterRuleResponse)
def get_filter_rule(rule_id: int, db: Session = Depends(get_db)):
    rule_obj = db.query(FilterRule).filter(FilterRule.id == rule_id).first() 
    if not rule_obj:
        raise HTTPException(
            status_code=404,
            detail=f"Filter rule with ID {rule_id} not found"
        )
    return rule_obj

@router.get("/filter-rules", response_model=List[FilterRuleResponse])
def get_filter_rules(
    skip: int = 0,
    limit: int = 100,
    rule_type: Optional[str] = None,
    is_active: Optional[bool] = None,
    source_file: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(FilterRule)
    
    if rule_type:
        if rule_type not in RULE_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid rule_type filter. Must be one of: {', '.join(RULE_TYPES)}"
            )
        query = query.filter(FilterRule.rule_type == rule_type) 
    
    if is_active is not None:
        query = query.filter(FilterRule.is_active == is_active) 
    
    if source_file:
        query = query.filter(FilterRule.source_file == source_file) 
    
    rules = query.offset(skip).limit(limit).all() 
    return rules

@router.put("/filter-rules/{rule_id}", response_model=FilterRuleResponse)
def update_filter_rule(rule_id: int, req: FilterRuleRequest, db: Session = Depends(get_db)):
    rule_obj = db.query(FilterRule).filter(FilterRule.id == rule_id).first() 
    if not rule_obj:
        raise HTTPException(
            status_code=404,
            detail=f"Filter rule with ID {rule_id} not found"
        )
    
    if req.rule_type not in RULE_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid rule_type. Must be one of: {', '.join(RULE_TYPES)}"
        )
    
    rule_obj.pattern = req.pattern
    rule_obj.rule_type = req.rule_type
    rule_obj.source_file = req.source_file
    rule_obj.line_number = req.line_number
    rule_obj.modifiers = req.modifiers
    rule_obj.description = req.description
    if req.is_active is not None:
        rule_obj.is_active = req.is_active
    rule_obj.confidence_score = req.confidence_score
    
    db.commit()
    db.refresh(rule_obj)
    return rule_obj

@router.delete("/filter-rules/{rule_id}")
def delete_filter_rule(rule_id: int, db: Session = Depends(get_db)):
    rule_obj = db.query(FilterRule).filter(FilterRule.id == rule_id).first() 
    if not rule_obj:
        raise HTTPException(
            status_code=404,
            detail=f"Filter rule with ID {rule_id} not found"
        )
    
    db.delete(rule_obj)
    db.commit()
    return {"message": f"Filter rule {rule_id} deleted successfully"}

@router.post("/blocked-urls", response_model=BlockedURLResponse)
def create_blocked_url(req: BlockedURLRequest, response: Response, db: Session = Depends(get_db)):
    if req.block_reason not in BLOCK_REASONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid block_reason. Must be one of: {', '.join(BLOCK_REASONS)}"
        )
    
    if req.source and req.source not in SOURCES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid source. Must be one of: {', '.join(SOURCES)}"
        )
    
    existing = db.query(BlockedURL).filter(BlockedURL.url == req.url).first() 
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"URL '{req.url}' is already blocked"
        )
    
    domain = req.domain
    if not domain:
        try:
            from urllib.parse import urlparse
            parsed = urlparse(req.url)
            domain = parsed.netloc.lower()
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="Could not extract domain from URL and no domain provided"
            )
    
    blocked_obj = BlockedURL()
    blocked_obj.url = req.url
    blocked_obj.domain = domain
    blocked_obj.matched_rule_id = req.matched_rule_id
    blocked_obj.block_reason = req.block_reason
    blocked_obj.source = req.source
    blocked_obj.meta = req.meta
    
    db.add(blocked_obj)
    db.commit()
    db.refresh(blocked_obj)
    response.status_code = 201
    return blocked_obj

@router.get("/blocked-urls/{blocked_id}", response_model=BlockedURLResponse)
def get_blocked_url(blocked_id: int, db: Session = Depends(get_db)):
    blocked_obj = db.query(BlockedURL).filter(BlockedURL.id == blocked_id).first() 
    if not blocked_obj:
        raise HTTPException(
            status_code=404,
            detail=f"Blocked URL with ID {blocked_id} not found"
        )
    return blocked_obj

@router.get("/blocked-urls", response_model=List[BlockedURLResponse])
def get_blocked_urls(
    skip: int = 0,
    limit: int = 100,
    domain: Optional[str] = None,
    block_reason: Optional[str] = None,
    source: Optional[str] = None,
    matched_rule_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    query = db.query(BlockedURL)
    
    if domain:
        query = query.filter(BlockedURL.domain == domain) 
    
    if block_reason:
        if block_reason not in BLOCK_REASONS:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid block_reason filter. Must be one of: {', '.join(BLOCK_REASONS)}"
            )
        query = query.filter(BlockedURL.block_reason == block_reason) 
    
    if source:
        if source not in SOURCES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid source filter. Must be one of: {', '.join(SOURCES)}"
            )
        query = query.filter(BlockedURL.source == source) 
    
    if matched_rule_id:
        query = query.filter(BlockedURL.matched_rule_id == matched_rule_id) 
    
    blocked_urls = query.offset(skip).limit(limit).all() 
    return blocked_urls

@router.delete("/blocked-urls/{blocked_id}")
def delete_blocked_url(blocked_id: int, db: Session = Depends(get_db)):
    blocked_obj = db.query(BlockedURL).filter(BlockedURL.id == blocked_id).first() 
    if not blocked_obj:
        raise HTTPException(
            status_code=404,
            detail=f"Blocked URL with ID {blocked_id} not found"
        )
    
    db.delete(blocked_obj)
    db.commit()
    return {"message": f"Blocked URL {blocked_id} deleted successfully"}

@router.get("/blocked-urls/check/{url:path}")
def check_url_blocked(url: str, db: Session = Depends(get_db)):
    blocked = db.query(BlockedURL).filter(BlockedURL.url == url).first() 
    return {
        "url": url,
        "is_blocked": blocked is not None,
        "block_info": blocked if blocked else None
    }

@router.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    total_urls = db.query(URL).count()
    total_filter_rules = db.query(FilterRule).count()
    active_filter_rules = db.query(FilterRule).filter(FilterRule.is_active == True).count() 
    total_blocked_urls = db.query(BlockedURL).count()
    
    url_sources = db.query(URL.source, func.count(URL.id)).group_by(URL.source).all() 
    blocked_reasons = db.query(BlockedURL.block_reason, func.count(BlockedURL.id)).group_by(BlockedURL.block_reason).all() 
    
    return {
        "total_urls": total_urls,
        "total_filter_rules": total_filter_rules,
        "active_filter_rules": active_filter_rules,
        "total_blocked_urls": total_blocked_urls,
        "url_sources": dict(url_sources),
        "blocked_reasons": dict(blocked_reasons)
    }

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