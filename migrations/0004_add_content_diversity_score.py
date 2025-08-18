"""
Add content diversity score to domain_reputation table
"""
from yoyo import step

__depends__ = ['0003_add_volatility_to_domains']

steps = [
    step(
        """
        ALTER TABLE domain_reputation
        ADD COLUMN content_diversity_score FLOAT DEFAULT 0.0;
        """,
        """
        ALTER TABLE domain_reputation
        DROP COLUMN content_diversity_score;
        """
    )
]
