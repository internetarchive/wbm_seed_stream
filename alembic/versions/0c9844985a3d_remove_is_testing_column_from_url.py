"""Remove is_testing column from URL

Revision ID: 0c9844985a3d
Revises: f751ba1d4b7d
Create Date: 2025-06-04 20:30:40.719790

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa



revision: str = '0c9844985a3d'
down_revision: Union[str, None] = 'f751ba1d4b7d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    
    op.drop_column('urls', 'is_active')
    


def downgrade() -> None:
    
    op.add_column('urls', sa.Column('is_active', sa.BOOLEAN(), autoincrement=False, nullable=False))
    
