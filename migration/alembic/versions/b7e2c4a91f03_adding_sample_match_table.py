"""Adding sample_match table linking samples to be analysed together

Revision ID: b7e2c4a91f03
Revises: 64a560017524
Create Date: 2026-09-30 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'b7e2c4a91f03'
down_revision: Union[str, None] = '64a560017524'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'sample_match',
        sa.Column('sample_id', sa.Integer(), nullable=False),
        sa.Column('matched_sample_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['sample_id'], ['sample.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['matched_sample_id'], ['sample.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('sample_id', 'matched_sample_id'),
    )
    op.create_index('ix_sample_match_matched_sample_id', 'sample_match', ['matched_sample_id'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_sample_match_matched_sample_id', table_name='sample_match')
    op.drop_table('sample_match')
