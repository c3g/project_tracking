"""Adding State for File + Add indexes

Revision ID: cacb85b3d114
Revises: 943ea08a8a82
Create Date: 2025-09-11 15:52:36.082049

"""
# pylint: disable=no-member
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from alembic_postgresql_enum import TableReference

# revision identifiers, used by Alembic.
revision = 'cacb85b3d114'
down_revision = '943ea08a8a82'
branch_labels = None
depends_on = None

def upgrade() -> None:
    # Add State File
    op.add_column('file', sa.Column('state', postgresql.ENUM('VALID', 'ON_HOLD', 'INVALID', name='stateenum', create_type=False), nullable=True))

    # Use raw SQL to cast alias to JSON
    op.execute("""
        ALTER TABLE reference
        ALTER COLUMN alias TYPE JSON
        USING alias::json;
    """)

    # Add indexes
    op.create_index('ix_file_md5sum', 'file', ['md5sum'], unique=False)
    op.create_index('ix_file_name', 'file', ['name'], unique=False)
    op.create_index('ix_job_name', 'job', ['name'], unique=False)
    op.create_index('ix_job_operation_id', 'job', ['operation_id'], unique=False)
    op.create_index('ix_job_status', 'job', ['status'], unique=False)
    op.drop_index('idx_job_file_file_id', table_name='job_file')
    op.create_index('ix_job_file_file_id', 'job_file', ['file_id'], unique=False)
    op.create_index('ix_job_file_job_id_file_id', 'job_file', ['job_id', 'file_id'], unique=False)
    op.drop_index('idx_location_file_id', table_name='location')
    op.create_index('ix_location_file_id', 'location', ['file_id'], unique=False)
    op.create_index('ix_metric_job_id', 'metric', ['job_id'], unique=False)
    op.create_index('ix_metric_name', 'metric', ['name'], unique=False)
    op.create_index('ix_operation_operation_config_id', 'operation', ['operation_config_id'], unique=False)
    op.create_index('ix_operation_project_id', 'operation', ['project_id'], unique=False)
    op.create_index('ix_operation_reference_id', 'operation', ['reference_id'], unique=False)
    op.create_index('ix_project_name', 'project', ['name'], unique=False)
    op.create_index('ix_readset_experiment_id', 'readset', ['experiment_id'], unique=False)
    op.create_index('ix_readset_name', 'readset', ['name'], unique=False)
    op.create_index('ix_readset_run_id', 'readset', ['run_id'], unique=False)
    op.create_index('ix_readset_sample_id', 'readset', ['sample_id'], unique=False)
    op.drop_index('idx_readset_file_file_id', table_name='readset_file')
    op.drop_index('idx_readset_file_readset_id', table_name='readset_file')
    op.create_index('ix_readset_file_file_id', 'readset_file', ['file_id'], unique=False)
    op.create_index('ix_readset_file_readset_id_file_id', 'readset_file', ['readset_id', 'file_id'], unique=False)
    op.create_index('ix_readset_job_job_id', 'readset_job', ['job_id'], unique=False)
    op.create_index('ix_readset_job_readset_id_job_id', 'readset_job', ['readset_id', 'job_id'], unique=False)
    op.create_index('ix_readset_metric_metric_id', 'readset_metric', ['metric_id'], unique=False)
    op.create_index('ix_readset_metric_readset_id_metric_id', 'readset_metric', ['readset_id', 'metric_id'], unique=False)
    op.create_index('ix_readset_operation_operation_id', 'readset_operation', ['operation_id'], unique=False)
    op.create_index('ix_readset_operation_readset_id_operation_id', 'readset_operation', ['readset_id', 'operation_id'], unique=False)
    op.create_index('ix_sample_name', 'sample', ['name'], unique=False)
    op.create_index('ix_sample_specimen_id', 'sample', ['specimen_id'], unique=False)
    op.create_index('ix_specimen_name', 'specimen', ['name'], unique=False)
    op.create_index('ix_specimen_project_id', 'specimen', ['project_id'], unique=False)
    op.sync_enum_values('public', 'stateenum', ['VALID', 'ON_HOLD', 'INVALID', 'DELIVERED'],
                        [TableReference(table_schema='public', table_name='file', column_name='state'), TableReference(table_schema='public', table_name='readset', column_name='state')],
                        enum_values_to_rename=[])

def downgrade() -> None:
    # Drop State File
    # Revert alias column back to VARCHAR
    op.execute("""
        ALTER TABLE reference
        ALTER COLUMN alias TYPE VARCHAR;
    """)

    op.drop_column('file', 'state')

    # Drop Indexes
    op.sync_enum_values('public', 'stateenum', ['VALID', 'ON_HOLD', 'INVALID'],
                        [TableReference(table_schema='public', table_name='file', column_name='state'), TableReference(table_schema='public', table_name='readset', column_name='state')],
                        enum_values_to_rename=[])
    op.drop_index('ix_specimen_project_id', table_name='specimen')
    op.drop_index('ix_specimen_name', table_name='specimen')
    op.drop_index('ix_sample_specimen_id', table_name='sample')
    op.drop_index('ix_sample_name', table_name='sample')
    op.drop_index('ix_readset_operation_readset_id_operation_id', table_name='readset_operation')
    op.drop_index('ix_readset_operation_operation_id', table_name='readset_operation')
    op.drop_index('ix_readset_metric_readset_id_metric_id', table_name='readset_metric')
    op.drop_index('ix_readset_metric_metric_id', table_name='readset_metric')
    op.drop_index('ix_readset_job_readset_id_job_id', table_name='readset_job')
    op.drop_index('ix_readset_job_job_id', table_name='readset_job')
    op.drop_index('ix_readset_file_readset_id_file_id', table_name='readset_file')
    op.drop_index('ix_readset_file_file_id', table_name='readset_file')
    op.create_index('idx_readset_file_readset_id', 'readset_file', ['readset_id'], unique=False)
    op.create_index('idx_readset_file_file_id', 'readset_file', ['file_id'], unique=False)
    op.drop_index('ix_readset_sample_id', table_name='readset')
    op.drop_index('ix_readset_run_id', table_name='readset')
    op.drop_index('ix_readset_name', table_name='readset')
    op.drop_index('ix_readset_experiment_id', table_name='readset')
    op.drop_index('ix_project_name', table_name='project')
    op.drop_index('ix_operation_reference_id', table_name='operation')
    op.drop_index('ix_operation_project_id', table_name='operation')
    op.drop_index('ix_operation_operation_config_id', table_name='operation')
    op.drop_index('ix_metric_name', table_name='metric')
    op.drop_index('ix_metric_job_id', table_name='metric')
    op.drop_index('ix_location_file_id', table_name='location')
    op.create_index('idx_location_file_id', 'location', ['file_id'], unique=False)
    op.drop_index('ix_job_file_job_id_file_id', table_name='job_file')
    op.drop_index('ix_job_file_file_id', table_name='job_file')
    op.create_index('idx_job_file_file_id', 'job_file', ['file_id'], unique=False)
    op.drop_index('ix_job_status', table_name='job')
    op.drop_index('ix_job_operation_id', table_name='job')
    op.drop_index('ix_job_name', table_name='job')
    op.drop_index('ix_file_name', table_name='file')
    op.drop_index('ix_file_md5sum', table_name='file')
