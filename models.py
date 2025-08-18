from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, UniqueConstraint, func, Float, Index
from db import Base
import uuid

def generate_batch_id():
    return str(uuid.uuid4())

class URL(Base):
    __tablename__ = 'urls'
    __table_args__ = (
        UniqueConstraint('url', 'source', name='uix_url_source'),
        Index('idx_urls_processed_at', 'processed_at'),
        Index('idx_urls_score', 'score'),
        Index('idx_urls_analysis_batch_id', 'analysis_batch_id'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(Text, nullable=False)
    source = Column(String(32), nullable=False)
    received_at = Column(DateTime, nullable=False, server_default=func.now())
    processed_at = Column(DateTime, nullable=True, index=True)
    status = Column(String(16), nullable=False, default='pending')
    priority = Column(Integer, nullable=False, default=0)
    score = Column(Float, nullable=True, index=True)
    analysis_batch_id = Column(String(36), nullable=True, index=True)
    meta = Column(JSON)
    last_modified = Column(DateTime, nullable=True)

class DomainReputation(Base):
    __tablename__ = 'domain_reputation'
    __table_args__ = (
        Index('idx_domain_reputation_score', 'reputation_score'),
        Index('idx_domain_reputation_updated_at', 'updated_at'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(String(255), nullable=False, unique=True, index=True)
    reputation_score = Column(Float, nullable=True, index=True)
    total_urls_seen = Column(Integer, nullable=False, default=0)
    malicious_urls_count = Column(Integer, nullable=False, default=0)
    benign_urls_count = Column(Integer, nullable=False, default=0)
    first_seen_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    meta = Column(JSON)
    content_volatility_score = Column(Float, nullable=True, default=0.0) # Score from 0.0 to 1.0
    predicted_revisit_interval_hours = Column(Integer, nullable=True) # Interval in hours
    content_diversity_score = Column(Float, nullable=True, default=0.0)
