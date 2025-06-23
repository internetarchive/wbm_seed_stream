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
    
    # last_modified_http
    # last_changed_db

class FilterRule(Base):
    __tablename__ = 'filter_rules'
    __table_args__ = (
        Index('idx_filter_rule_type', 'rule_type'),
        Index('idx_filter_rule_pattern', 'pattern'),
        Index('idx_filter_rule_active', 'is_active'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    pattern = Column(Text, nullable=False)  # The actual filter pattern
    rule_type = Column(String(32), nullable=False)  # 'domain_block', 'regex', 'wildcard', etc.
    source_file = Column(String(64), nullable=True)  # e.g., 'badware.txt'
    line_number = Column(Integer, nullable=True)  # Line in source file
    modifiers = Column(String(256), nullable=True)  # uBlock modifiers like '$doc'
    description = Column(Text, nullable=True)  # Comments/description
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    
    # Additional metadata
    confidence_score = Column(Integer, nullable=True)  # 1-10 confidence in this rule
    false_positive_count = Column(Integer, nullable=False, default=0)
    match_count = Column(Integer, nullable=False, default=0)  # How many URLs matched this rule

class BlockedURL(Base):
    __tablename__ = 'blocked_urls'
    __table_args__ = (
        Index('idx_blocked_url', 'url'),
        Index('idx_blocked_domain', 'domain'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(Text, nullable=False)
    domain = Column(String(256), nullable=False)
    matched_rule_id = Column(Integer, nullable=True)  # FK to FilterRule
    block_reason = Column(String(64), nullable=False)  # 'badware', 'spam', 'malware', etc.
    blocked_at = Column(DateTime, nullable=False, server_default=func.now())
    source = Column(String(32), nullable=True)  # Where this URL was found
    meta = Column(JSON)  # Additional context