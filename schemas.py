from pydantic import BaseModel, HttpUrl
from typing import Optional, Any
from datetime import datetime

class URLIngestRequest(BaseModel):
    url: HttpUrl
    meta: Optional[Any] = None
    priority: Optional[int] = 0
    last_modified: Optional[datetime] = None
    source: str

class URLResponse(BaseModel):
    id: int
    url: HttpUrl
    source: str
    received_at: datetime
    status: str
    priority: int
    meta: Optional[Any] = None

class StatusResponse(BaseModel):
    id: int
    status: str

class StatsResponse(BaseModel):
    total: int
    by_source: dict
    by_status: dict

class FilterRuleRequest(BaseModel):
    pattern: str
    rule_type: str
    source_file: Optional[str] = None
    line_number: Optional[int] = None
    modifiers: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = True
    confidence_score: Optional[int] = None

class FilterRuleResponse(BaseModel):
    id: int
    pattern: str
    rule_type: str
    source_file: Optional[str] = None
    line_number: Optional[int] = None
    modifiers: Optional[str] = None
    description: Optional[str] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime
    confidence_score: Optional[int] = None
    false_positive_count: int
    match_count: int

class BlockedURLRequest(BaseModel):
    url: str
    domain: Optional[str] = None
    matched_rule_id: Optional[int] = None
    block_reason: str
    source: Optional[str] = None
    meta: Optional[Any] = None

class BlockedURLResponse(BaseModel):
    id: int
    url: str
    domain: str
    matched_rule_id: Optional[int] = None
    block_reason: str
    blocked_at: datetime
    source: Optional[str] = None
    meta: Optional[Any] = None

class URLCheckResponse(BaseModel):
    url: str
    is_blocked: bool
    block_info: Optional[BlockedURLResponse] = None