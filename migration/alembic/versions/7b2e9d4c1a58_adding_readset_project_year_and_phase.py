"""Adding readset project_year and project_phase

Revision ID: 7b2e9d4c1a58
Revises: 3c7f1a2d9e64
Create Date: 2026-11-18 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '7b2e9d4c1a58'
down_revision: Union[str, None] = '3c7f1a2d9e64'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('readset', sa.Column('project_year', sa.String(), nullable=True))
    op.add_column('readset', sa.Column('project_phase', sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column('readset', 'project_phase')
    op.drop_column('readset', 'project_year')
