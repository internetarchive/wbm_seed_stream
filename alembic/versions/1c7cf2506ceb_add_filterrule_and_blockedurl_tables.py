"""Add FilterRule and BlockedURL tables

Revision ID: 1c7cf2506ceb
Revises: 0c9844985a3d
Create Date: 2025-06-22 20:42:38.634815

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa



revision: str = '1c7cf2506ceb'
down_revision: Union[str, None] = '0c9844985a3d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    
    op.create_table('blocked_urls',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('url', sa.Text(), nullable=False),
    sa.Column('domain', sa.String(length=256), nullable=False),
    sa.Column('matched_rule_id', sa.Integer(), nullable=True),
    sa.Column('block_reason', sa.String(length=64), nullable=False),
    sa.Column('blocked_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.Column('source', sa.String(length=32), nullable=True),
    sa.Column('meta', sa.JSON(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_blocked_domain', 'blocked_urls', ['domain'], unique=False)
    op.create_index('idx_blocked_url', 'blocked_urls', ['url'], unique=False)
    op.create_table('filter_rules',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('pattern', sa.Text(), nullable=False),
    sa.Column('rule_type', sa.String(length=32), nullable=False),
    sa.Column('source_file', sa.String(length=64), nullable=True),
    sa.Column('line_number', sa.Integer(), nullable=True),
    sa.Column('modifiers', sa.String(length=256), nullable=True),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('is_active', sa.Boolean(), nullable=False),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.Column('confidence_score', sa.Integer(), nullable=True),
    sa.Column('false_positive_count', sa.Integer(), nullable=False),
    sa.Column('match_count', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_filter_rule_active', 'filter_rules', ['is_active'], unique=False)
    op.create_index('idx_filter_rule_pattern', 'filter_rules', ['pattern'], unique=False)
    op.create_index('idx_filter_rule_type', 'filter_rules', ['rule_type'], unique=False)
    


def downgrade() -> None:
    
    op.drop_index('idx_filter_rule_type', table_name='filter_rules')
    op.drop_index('idx_filter_rule_pattern', table_name='filter_rules')
    op.drop_index('idx_filter_rule_active', table_name='filter_rules')
    op.drop_table('filter_rules')
    op.drop_index('idx_blocked_url', table_name='blocked_urls')
    op.drop_index('idx_blocked_domain', table_name='blocked_urls')
    op.drop_table('blocked_urls')
    
