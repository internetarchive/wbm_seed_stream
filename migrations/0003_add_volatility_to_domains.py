"""
Add content volatility and revisit interval to domain_reputation
"""
from yoyo import step

__depends__ = ['0002_add_domain_reputation_table']

steps = [
    step(
        """
        ALTER TABLE domain_reputation
        ADD COLUMN content_volatility_score FLOAT DEFAULT 0.0,
        ADD COLUMN predicted_revisit_interval_hours INTEGER;
        """,
        """
        ALTER TABLE domain_reputation
        DROP COLUMN content_volatility_score,
        DROP COLUMN predicted_revisit_interval_hours;
        """
    )
]
