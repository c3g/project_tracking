"""Replacing sample_match with sample_group and sample_group_member

Revision ID: a9c4e6f20b18
Revises: f2b6c8d41e17
Create Date: 2026-10-15 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'a9c4e6f20b18'
down_revision: Union[str, None] = 'f2b6c8d41e17'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # sample_match was never deployed: its links aren't carried over
    op.drop_index('ix_sample_match_matched_sample_id', table_name='sample_match')
    op.drop_table('sample_match')
    op.create_table(
        'sample_group',
        sa.Column('specimen_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('deprecated', sa.Boolean(), nullable=False),
        sa.Column('deleted', sa.Boolean(), nullable=False),
        sa.Column('creation', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('modification', sa.DateTime(timezone=True), nullable=True),
        sa.Column('extra_metadata', sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), 'postgresql'), nullable=True),
        sa.Column('ext_id', sa.Integer(), nullable=True),
        sa.Column('ext_src', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['specimen_id'], ['specimen.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
    )
    op.create_index('ix_sample_group_specimen_id', 'sample_group', ['specimen_id'], unique=False)
    op.create_table(
        'sample_group_member',
        sa.Column('sample_group_id', sa.Integer(), nullable=False),
        sa.Column('sample_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['sample_group_id'], ['sample_group.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['sample_id'], ['sample.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('sample_group_id', 'sample_id'),
    )
    op.create_index('ix_sample_group_member_sample_id', 'sample_group_member', ['sample_id'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_sample_group_member_sample_id', table_name='sample_group_member')
    op.drop_table('sample_group_member')
    op.drop_index('ix_sample_group_specimen_id', table_name='sample_group')
    op.drop_table('sample_group')
    op.create_table(
        'sample_match',
        sa.Column('sample_id', sa.Integer(), nullable=False),
        sa.Column('matched_sample_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['sample_id'], ['sample.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['matched_sample_id'], ['sample.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('sample_id', 'matched_sample_id'),
    )
    op.create_index('ix_sample_match_matched_sample_id', 'sample_match', ['matched_sample_id'], unique=False)
