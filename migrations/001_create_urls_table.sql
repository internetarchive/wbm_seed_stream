CREATE TABLE urls (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    source VARCHAR(32) NOT NULL,
    received_at TIMESTAMP NOT NULL DEFAULT NOW(),
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    meta JSONB,
    UNIQUE (url, source)
);

CREATE INDEX idx_urls_status ON urls(status);
CREATE INDEX idx_urls_priority ON urls(priority); 