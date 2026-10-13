"""Adding oncotree_code to Sample

Revision ID: c3d8f1e27a54
Revises: b7e2c4a91f03
Create Date: 2026-10-06 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'c3d8f1e27a54'
down_revision: Union[str, None] = 'b7e2c4a91f03'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('sample', sa.Column('oncotree_code', sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column('sample', 'oncotree_code')
