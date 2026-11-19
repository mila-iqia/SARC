"""create notification_feedback table

Revision ID: f2b41c7a90d3
Revises: c8a3f21e9d47
Create Date: 2026-11-18 19:30:00.000000+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa

import sarc.db.sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f2b41c7a90d3"
down_revision: Union[str, Sequence[str], None] = "c8a3f21e9d47"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
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
        sa.UniqueConstraint("slack_user_id", "kind", "window_end"),
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
