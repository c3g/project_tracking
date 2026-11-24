"""Adding readset preservation

Revision ID: 5d8a3f6c2b17
Revises: 7b2e9d4c1a58
Create Date: 2026-11-23 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '5d8a3f6c2b17'
down_revision: Union[str, None] = '7b2e9d4c1a58'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

preservation_enum = sa.Enum('FFPE', 'FRESH_FROZEN', name='preservationenum')


def upgrade() -> None:
    preservation_enum.create(op.get_bind())
    op.add_column('readset', sa.Column('preservation', preservation_enum, nullable=True))


def downgrade() -> None:
    op.drop_column('readset', 'preservation')
    preservation_enum.drop(op.get_bind())
