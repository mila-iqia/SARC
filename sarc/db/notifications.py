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
