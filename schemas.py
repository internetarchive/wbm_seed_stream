from pydantic import BaseModel, HttpUrl
from typing import Optional, Any
from datetime import datetime

class URLIngestRequest(BaseModel):
    url: HttpUrl
    meta: Optional[Any] = None
    priority: Optional[int] = 0
    last_modified: Optional[datetime] = None

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