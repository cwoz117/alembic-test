"""add column to TableB

Revision ID: a942a9caba78
Revises: 
Create Date: 2022-11-21 14:52:18.395146

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a942a9caba78'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('tableb', sa.Column('created', sa.DateTime), schema="myschema")



def downgrade() -> None:
    op.drop_column('tableb', sa.Column('created'), schema="myschema")
