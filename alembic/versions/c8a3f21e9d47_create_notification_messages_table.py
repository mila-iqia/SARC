"""create notification_messages table

Revision ID: c8a3f21e9d47
Revises: 2eaef96b85d5
Create Date: 2026-11-18 09:00:00.000000+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa

import sarc.db.sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c8a3f21e9d47"
down_revision: Union[str, Sequence[str], None] = "2eaef96b85d5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "notification_messages",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column(
            "window_end", sarc.db.sqlmodel.UTCDateTime(timezone=True), nullable=False
        ),
        sa.Column("channel", sa.String(), nullable=False),
        sa.Column("message_ts", sa.String(), nullable=False),
        sa.Column("slack_user_id", sa.String(), nullable=False),
        sa.Column(
            "sent_at", sarc.db.sqlmodel.UTCDateTime(timezone=True), nullable=False
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "kind", "window_end"),
    )
    op.create_index(
        op.f("ix_notification_messages_user_id"),
        "notification_messages",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_notification_messages_window_end"),
        "notification_messages",
        ["window_end"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        op.f("ix_notification_messages_window_end"), table_name="notification_messages"
    )
    op.drop_index(
        op.f("ix_notification_messages_user_id"), table_name="notification_messages"
    )
    op.drop_table("notification_messages")
