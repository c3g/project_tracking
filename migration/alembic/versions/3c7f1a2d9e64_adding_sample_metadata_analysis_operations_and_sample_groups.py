"""Adding sample metadata, analysis operations and sample groups

Every schema change since release 3.1.6 (64a560017524), in one step:
- sample: oncotree_code and cancer_type
- operation: analysis flag (set on existing GenPipes analyses), pipeline and protocol
- sample_group and sample_group_member: samples analysed together

Revision ID: 3c7f1a2d9e64
Revises: 64a560017524
Create Date: 2026-10-15 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '3c7f1a2d9e64'
down_revision: Union[str, None] = '64a560017524'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('sample', sa.Column('oncotree_code', sa.String(), nullable=True))
    op.add_column('sample', sa.Column('cancer_type', sa.String(), nullable=True))

    # Temporary server default so existing rows get a value, then removed to match the model
    op.add_column('operation', sa.Column('analysis', sa.Boolean(), nullable=False, server_default=sa.false()))
    op.add_column('operation', sa.Column('pipeline', sa.String(), nullable=True))
    op.add_column('operation', sa.Column('protocol', sa.String(), nullable=True))
    # Until now ingest_genpipes was the only way to ingest an analysis, always named 'genpipes'
    op.execute(sa.text("UPDATE operation SET analysis = true WHERE name = 'genpipes'"))
    op.alter_column('operation', 'analysis', server_default=None)

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
    op.drop_column('operation', 'protocol')
    op.drop_column('operation', 'pipeline')
    op.drop_column('operation', 'analysis')
    op.drop_column('sample', 'cancer_type')
    op.drop_column('sample', 'oncotree_code')
