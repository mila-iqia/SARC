"""empty message

Revision ID: fd67673fd295
Revises: 50c6641dfb7a
Create Date: 2026-11-24 18:59:40.455927+00:00

"""

import os
from typing import Sequence, Union

import sqlalchemy

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "fd67673fd295"
down_revision: Union[str, Sequence[str], None] = "50c6641dfb7a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    api_user = os.environ.get("SARC_API_DB_USER")
    assert api_user
    c = op.get_bind()
    user = c.dialect.identifier_preparer.quote(api_user)
    op.execute(
        sqlalchemy.text(
            f"GRANT SELECT, INSERT, UPDATE ON notification_messages, notification_feedback TO {user}"
        )
    )
    op.execute(
        sqlalchemy.text(
            f"GRANT USAGE ON SEQUENCE notification_messages_id_seq, notification_feedback_id_seq TO {user}"
        )
    )


def downgrade() -> None:
    """Downgrade schema."""
