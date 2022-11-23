"""create view

Revision ID: ae6f716a3061
Revises: a2ed011c30cd
Create Date: 2022-11-23 14:28:39.366649

"""
from alembic import op
import sqlalchemy as sa
from alembic_utils.pg_view import PGView


# revision identifiers, used by Alembic.
revision = 'ae6f716a3061'
down_revision = 'a2ed011c30cd'
branch_labels = None
depends_on = None


def upgrade() -> None:
    from alembic_utils.pg_view import PGView
    first_view = PGView(
        schema="myschema",
        signature="select_me_some_data",
        definition="select distinct Contract_id, created from myschema.tableB order by created",
    )
    op.create_entity(first_view)

def downgrade() -> None:
    first_view = PGView(
        schema="myschema",
        signature="select_me_some_data",
        definition="# Not Used"
    )
    op.drop_entity(first_view)
