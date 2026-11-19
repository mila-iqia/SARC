from sqlmodel import Field, UniqueConstraint

from sarc.db.sqlmodel import SQLModel, datetime_utc_field
from sarc.validators import datetime_utc


class NotificationMessage(SQLModel, table=True):
    """One row per delivered usage-notification DM (SARC-802 delivery tracking).

    Grain: one row per (user, kind, window_end) — a re-run of the same cycle's
    delivery upserts the row (see `sarc.notifications.delivery`) instead of
    creating a second denominator for the acknowledgement-rate computation.
    """

    __tablename__ = "notification_messages"
    __table_args__ = (UniqueConstraint("user_id", "kind", "window_end"),)

    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="users.id", index=True)
    kind: str
    """Which notification this delivery belongs to: "underusage" | "usage"."""
    window_end: datetime_utc = datetime_utc_field(index=True)
    """End of the usage window the message reports on (the run's window end)."""
    channel: str
    """Slack DM channel id the message was posted to."""
    message_ts: str
    """Slack message ts; (channel, ts) identifies the message for chat.update."""
    slack_user_id: str
    """Slack user id resolved from the recipient's email at send time —
    the identity an interactivity click will carry back."""
    sent_at: datetime_utc = datetime_utc_field()
    """When the message was posted."""


class NotificationFeedback(SQLModel, table=True):
    """One row per acknowledgement click on a notification's button (SARC-802).

    Grain: one row per (slack_user_id, kind, window_end) — re-clicks of the
    same cycle's button (e.g. after a failed chat.update) are dropped, so each
    message contributes at most one click to the acknowledgement-rate
    numerator. The clicker is identified by Slack id because that is what the
    interactivity payload carries; ``message_id`` links back to the tracked
    delivery row when one matches.
    """

    __tablename__ = "notification_feedback"
    __table_args__ = (UniqueConstraint("slack_user_id", "kind", "window_end"),)

    id: int | None = Field(default=None, primary_key=True)
    message_id: int | None = Field(
        default=None, foreign_key="notification_messages.id", index=True
    )
    """The notification_messages row this click acknowledges, when a tracked
    delivery matches (NULL for messages sent before tracking existed)."""
    slack_user_id: str = Field(index=True)
    kind: str
    """Mirrors the parent message's kind: "underusage" | "usage"."""
    window_end: datetime_utc = datetime_utc_field(index=True)
    """Mirrors the parent message's window end — with slack_user_id and kind,
    the unique key the button's ``value`` resolves against."""
    action_id: str
    """Slack action_id of the clicked element."""
    channel: str
    """Slack channel id of the message that carried the clicked button."""
    message_ts: str
    """Slack ts of the message that carried the clicked button."""
    clicked_at: datetime_utc = datetime_utc_field()
    """When the click arrived (server receive time)."""
