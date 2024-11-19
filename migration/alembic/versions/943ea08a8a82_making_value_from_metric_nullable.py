"""Making value from Metric nullable

Revision ID: 943ea08a8a82
Revises: d1dc98f61aee
Create Date: 2024-11-19 14:53:47.781164

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from alembic_utils.pg_grant_table import PGGrantTable
from sqlalchemy import text as sql_text

# revision identifiers, used by Alembic.
revision: str = '943ea08a8a82'
down_revision: Union[str, None] = 'd1dc98f61aee'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('metric', 'value',
               existing_type=sa.VARCHAR(),
               nullable=True)


def downgrade() -> None:
    op.alter_column('metric', 'value',
               existing_type=sa.VARCHAR(),
               nullable=False)
