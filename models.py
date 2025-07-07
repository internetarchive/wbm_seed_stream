from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, UniqueConstraint, func, Float, Index
from sqlalchemy.ext.declarative import declarative_base
import uuid

Base = declarative_base()

def generate_batch_id():
    return str(uuid.uuid4())

class URL(Base):
    __tablename__ = 'urls'
    __table_args__ = (
        UniqueConstraint('url', 'source', name='uix_url_source'),
        Index('idx_urls_processed_at', 'processed_at'),
        Index('idx_urls_score', 'score'),
        {'postgresql_using': 'btree'},
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
    
    @classmethod
    def create_hypertable(cls, connection):
        """Convert the table to a TimescaleDB hypertable"""
        connection.execute(
            f"""
            SELECT create_hypertable(
                '{cls.__tablename__}',
                'processed_at',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
            """
        )
        
        # Add compression policy
        connection.execute(
            f"""
            ALTER TABLE {cls.__tablename__} SET (
                timescaledb.compress,
                timescaledb.compress_orderby = 'processed_at DESC',
                timescaledb.compress_segmentby = 'source, status'
            );
            """
        )
        
        # Add compression policy (compress data older than 7 days)
        connection.execute(
            f"""
            SELECT add_compression_policy(
                '{cls.__tablename__}',
                INTERVAL '7 days'
            );
            """
        )