"""
Add domain reputation table
"""
from yoyo import step

__depends__ = []

steps = [
    step(
        """
        CREATE TABLE domain_reputation (
            id SERIAL PRIMARY KEY,
            domain VARCHAR(255) NOT NULL UNIQUE,
            reputation_score FLOAT,
            total_urls_seen INTEGER NOT NULL DEFAULT 0,
            malicious_urls_count INTEGER NOT NULL DEFAULT 0,
            benign_urls_count INTEGER NOT NULL DEFAULT 0,
            first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            meta JSONB
        );
        """,
        """
        DROP TABLE domain_reputation;
        """
    ),
    step(
        """
        CREATE INDEX idx_domain_reputation_domain ON domain_reputation(domain);
        """,
        """
        DROP INDEX idx_domain_reputation_domain;
        """
    ),
    step(
        """
        CREATE INDEX idx_domain_reputation_score ON domain_reputation(reputation_score);
        """,
        """
        DROP INDEX idx_domain_reputation_score;
        """
    ),
    step(
        """
        CREATE INDEX idx_domain_reputation_updated_at ON domain_reputation(updated_at);
        """,
        """
        DROP INDEX idx_domain_reputation_updated_at;
        """
    )
]