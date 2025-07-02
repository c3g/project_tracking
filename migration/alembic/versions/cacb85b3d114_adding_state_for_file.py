"""Adding State for File

Revision ID: cacb85b3d114
Revises: 943ea08a8a82
Create Date: 2025-06-13 16:17:28.743536

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'cacb85b3d114'
down_revision = '943ea08a8a82'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column('file', sa.Column('state', postgresql.ENUM('VALID', 'ON_HOLD', 'INVALID', name='stateenum', create_type=False), nullable=True))

    # Use raw SQL to cast alias to JSON
    op.execute("""
        ALTER TABLE reference
        ALTER COLUMN alias TYPE JSON
        USING alias::json;
    """)

def downgrade() -> None:
    # Revert alias column back to VARCHAR
    op.execute("""
        ALTER TABLE reference
        ALTER COLUMN alias TYPE VARCHAR;
    """)

    op.drop_column('file', 'state')
