from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, UniqueConstraint, func, Boolean, Index
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class URL(Base):
    __tablename__ = 'urls'
    __table_args__ = (
        UniqueConstraint('url', 'source', name='uix_url_source'),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(Text, nullable=False)
    source = Column(String(32), nullable=False)
    received_at = Column(DateTime, nullable=False, server_default=func.now())
    status = Column(String(16), nullable=False, default='pending')
    priority = Column(Integer, nullable=False, default=0)
    meta = Column(JSON)
    last_modified = Column(DateTime, nullable=True)
    
class FilterRule(Base):
    __tablename__ = 'filter_rules'
    __table_args__ = (
        Index('idx_filter_rule_type', 'rule_type'),
        Index('idx_filter_rule_pattern', 'pattern'),
        Index('idx_filter_rule_active', 'is_active'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    pattern = Column(Text, nullable=False)  
    rule_type = Column(String(32), nullable=False)  
    source_file = Column(String(64), nullable=True)  
    line_number = Column(Integer, nullable=True)  
    modifiers = Column(String(256), nullable=True)  
    description = Column(Text, nullable=True)  
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    confidence_score = Column(Integer, nullable=True)  
    false_positive_count = Column(Integer, nullable=False, default=0)
    match_count = Column(Integer, nullable=False, default=0)  

class BlockedURL(Base):
    __tablename__ = 'blocked_urls'
    __table_args__ = (
        Index('idx_blocked_url', 'url'),
        Index('idx_blocked_domain', 'domain'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(Text, nullable=False)
    domain = Column(String(256), nullable=False)
    matched_rule_id = Column(Integer, nullable=True)  
    block_reason = Column(String(64), nullable=False)  
    blocked_at = Column(DateTime, nullable=False, server_default=func.now())
    source = Column(String(32), nullable=True)  
    meta = Column(JSON)

class AnalyzedURL(Base):
    __tablename__ = 'analyzed_urls'
    __table_args__ = (
        Index('idx_analyzed_processed', 'processed'),
        Index('idx_analyzed_created_at', 'created_at'),
        Index('idx_analyzed_processed_created', 'processed', 'created_at'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(Text, nullable=False)
    timestamp = Column(DateTime, nullable=True)
    processed = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())