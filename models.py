from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, UniqueConstraint, func
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