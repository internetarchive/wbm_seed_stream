ALTER TABLE urls ALTER COLUMN last_modified DROP NOT NULL;
-- Alembic was added after migration 003, so refer to /alembic/versions