"""
Add unique constraint to url column in urls table
"""

from yoyo import step

__depends__ = {'20250707_01_wD85x-initial-migration'}

steps = [
    step(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_urls_url ON urls (url)",
        "DROP INDEX IF EXISTS idx_urls_url"
    )
]

