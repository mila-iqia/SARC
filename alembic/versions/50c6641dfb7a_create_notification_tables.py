"""create notification tracking tables (deliveries + button clicks)

Revision ID: 50c6641dfb7a
Revises: 2eaef96b85d5
Create Date: 2026-11-20 09:00:00.000000+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa

import sarc.db.sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "50c6641dfb7a"
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

    op.create_table(
        "notification_feedback",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("message_id", sa.Integer(), nullable=True),
        sa.Column("slack_user_id", sa.String(), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column(
            "window_end", sarc.db.sqlmodel.UTCDateTime(timezone=True), nullable=False
        ),
        sa.Column("action_id", sa.String(), nullable=False),
        sa.Column("channel", sa.String(), nullable=False),
        sa.Column("message_ts", sa.String(), nullable=False),
        sa.Column(
            "clicked_at", sarc.db.sqlmodel.UTCDateTime(timezone=True), nullable=False
        ),
        sa.ForeignKeyConstraint(["message_id"], ["notification_messages.id"]),
        sa.PrimaryKeyConstraint("id"),
        # Explicit name: the Postgres default (table + all columns + _key)
        # would exceed the 63-character identifier limit.
        sa.UniqueConstraint(
            "slack_user_id",
            "kind",
            "window_end",
            "action_id",
            name="uq_notification_feedback_user_kind_window_action",
        ),
    )
    op.create_index(
        op.f("ix_notification_feedback_message_id"),
        "notification_feedback",
        ["message_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_notification_feedback_slack_user_id"),
        "notification_feedback",
        ["slack_user_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_notification_feedback_window_end"),
        "notification_feedback",
        ["window_end"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    # feedback first: its message_id FK references notification_messages.
    op.drop_index(
        op.f("ix_notification_feedback_window_end"), table_name="notification_feedback"
    )
    op.drop_index(
        op.f("ix_notification_feedback_slack_user_id"),
        table_name="notification_feedback",
    )
    op.drop_index(
        op.f("ix_notification_feedback_message_id"), table_name="notification_feedback"
    )
    op.drop_table("notification_feedback")
    op.drop_index(
        op.f("ix_notification_messages_window_end"), table_name="notification_messages"
    )
    op.drop_index(
        op.f("ix_notification_messages_user_id"), table_name="notification_messages"
    )
    op.drop_table("notification_messages")
