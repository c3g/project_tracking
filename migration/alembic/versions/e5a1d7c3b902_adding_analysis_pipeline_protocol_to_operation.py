"""Adding analysis, pipeline and protocol to Operation

Revision ID: e5a1d7c3b902
Revises: c3d8f1e27a54
Create Date: 2026-10-13 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'e5a1d7c3b902'
down_revision: Union[str, None] = 'c3d8f1e27a54'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Temporary server default so existing rows get a value, then removed to match the model
    op.add_column('operation', sa.Column('analysis', sa.Boolean(), nullable=False, server_default=sa.false()))
    op.add_column('operation', sa.Column('pipeline', sa.String(), nullable=True))
    op.add_column('operation', sa.Column('protocol', sa.String(), nullable=True))
    # Until now ingest_genpipes was the only way to ingest an analysis, always named 'genpipes'
    op.execute(sa.text("UPDATE operation SET analysis = true WHERE name = 'genpipes'"))
    op.alter_column('operation', 'analysis', server_default=None)


def downgrade() -> None:
    op.drop_column('operation', 'protocol')
    op.drop_column('operation', 'pipeline')
    op.drop_column('operation', 'analysis')
