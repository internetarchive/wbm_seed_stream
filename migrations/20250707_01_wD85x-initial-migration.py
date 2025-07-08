"""
Initial migration
"""
from yoyo import step

__depends__ = {}

steps = [
    step("""
        CREATE TABLE urls (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            source VARCHAR(32) NOT NULL,
            received_at TIMESTAMP NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMP,
            status VARCHAR(16) NOT NULL DEFAULT 'pending',
            priority INTEGER NOT NULL DEFAULT 0,
            score FLOAT,
            analysis_batch_id VARCHAR(36),
            meta JSONB,
            last_modified TIMESTAMP,
            CONSTRAINT uix_url_source UNIQUE (url, source)
        );
        
        CREATE INDEX idx_urls_processed_at ON urls (processed_at);
        CREATE INDEX idx_urls_score ON urls (score);
        CREATE INDEX idx_urls_analysis_batch_id ON urls (analysis_batch_id);
    """, 
    """
        DROP TABLE IF EXISTS urls CASCADE;
    """),
]