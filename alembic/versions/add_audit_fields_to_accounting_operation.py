"""add_audit_fields_to_accounting_operation

Revision ID: a24f8c3b95d1
Revises: 
Create Date: 2025-10-01 20:39:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a24f8c3b95d1'
down_revision = None  # Change this to your previous migration revision ID
branch_labels = None
depends_on = None


def upgrade():
    # Add audit fields to accounting_operation table
    op.add_column('accountingoperation', sa.Column('sequence_number', sa.Integer(), nullable=True))
    op.add_column('accountingoperation', sa.Column('verified_amount', sa.Numeric(precision=15, scale=2), nullable=True))
    op.add_column('accountingoperation', sa.Column('deviation_amount', sa.Numeric(precision=15, scale=2), nullable=True))
    op.add_column('accountingoperation', sa.Column('control_action', sa.Text(), nullable=True))
    op.add_column('accountingoperation', sa.Column('deviation_note', sa.Text(), nullable=True))

    # Create an index on sequence_number for faster lookups
    op.create_index(op.f('ix_accountingoperation_sequence_number'), 'accountingoperation', ['sequence_number'], unique=False)


def downgrade():
    # Remove the columns in case of a rollback
    op.drop_index(op.f('ix_accountingoperation_sequence_number'), table_name='accountingoperation')
    op.drop_column('accountingoperation', 'deviation_note')
    op.drop_column('accountingoperation', 'control_action')
    op.drop_column('accountingoperation', 'deviation_amount')
    op.drop_column('accountingoperation', 'verified_amount')
    op.drop_column('accountingoperation', 'sequence_number')