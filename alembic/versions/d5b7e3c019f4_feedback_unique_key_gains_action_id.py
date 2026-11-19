"""feedback unique key gains action_id (read vs help are both kept)

Revision ID: d5b7e3c019f4
Revises: f2b41c7a90d3
Create Date: 2026-11-18 21:00:00.000000+00:00

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d5b7e3c019f4"
down_revision: Union[str, Sequence[str], None] = "f2b41c7a90d3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_OLD_NAME = "notification_feedback_slack_user_id_kind_window_end_key"
_NEW_NAME = "uq_notification_feedback_user_kind_window_action"


def upgrade() -> None:
    """Upgrade schema."""
    # Widening the key: the old constraint is a stricter subset, so existing
    # rows can never violate the new one.
    op.drop_constraint(_OLD_NAME, "notification_feedback", type_="unique")
    op.create_unique_constraint(
        _NEW_NAME,
        "notification_feedback",
        ["slack_user_id", "kind", "window_end", "action_id"],
    )


def downgrade() -> None:
    """Downgrade schema.

    May fail when read+help clicks on the same (user, kind, window_end)
    already exist — correct enough: the narrower key no longer describes the
    data.
    """
    op.drop_constraint(_NEW_NAME, "notification_feedback", type_="unique")
    op.create_unique_constraint(
        _OLD_NAME, "notification_feedback", ["slack_user_id", "kind", "window_end"]
    )
