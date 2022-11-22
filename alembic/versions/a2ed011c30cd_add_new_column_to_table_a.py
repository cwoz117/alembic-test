"""add new column to table A

Revision ID: a2ed011c30cd
Revises: a942a9caba78
Create Date: 2022-11-21 19:00:30.797607

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a2ed011c30cd'
down_revision = 'a942a9caba78'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(table_name='tableA', column=sa.Column('issuer', sa.String()), schema='myschema')


def downgrade() -> None:
    op.drop_column(table_name='tableA', column=sa.Column('issuer', sa.String()), schema='myschema')

