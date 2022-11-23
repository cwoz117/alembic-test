"""create view to a kill

Revision ID: b627c8d82eb5
Revises: ae6f716a3061
Create Date: 2022-11-23 16:27:36.857395

"""
from alembic import op
import sqlalchemy as sa
from alembic_utils.pg_view import PGView

# revision identifiers, used by Alembic.
revision = 'b627c8d82eb5'
down_revision = 'ae6f716a3061'
branch_labels = None
depends_on = None


def upgrade() -> None:
    from alembic_utils.pg_view import PGView
    view = PGView(
        schema="myschema",
        signature="view_to_a_kill",
        definition="select Contract_id, created from myschema.select_me_some_data order by created desc",
    )
    op.create_entity(view)

def downgrade() -> None:
    view = PGView(
        schema="myschema",
        signature="view_to_a_kill",
        definition="# Not Used"
    )
    op.drop_entity(view)
