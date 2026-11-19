"""Delivery tracking for usage notifications (SARC-802).

Writes one ``notification_messages`` row per delivered DM so the
acknowledgement rate has a denominator; the acknowledgement side (block-kit
buttons + the interactivity endpoint) matches clicks against these rows.
"""

from datetime import datetime
from typing import Literal

from sqlalchemy.dialects.postgresql import insert as pg_insert

from sarc.config import config
from sarc.db.notifications import NotificationMessage

NotificationKind = Literal["underusage", "usage"]


def record_notification_send(
    *,
    user_id: int,
    kind: NotificationKind,
    window_end: datetime,
    channel: str,
    message_ts: str,
    slack_user_id: str,
    sent_at: datetime,
) -> None:
    """Upsert the delivery row for (user_id, kind, window_end) — last send wins.

    A re-run of the same cycle (e.g. ``--send`` again after a partial failure)
    must not double-count the denominator of the acknowledgement rate, so the
    unique (user_id, kind, window_end) constraint drives an ON CONFLICT DO
    UPDATE, the same pattern ``usage refresh-store`` uses for user_periods.
    """
    with config.db.session() as session:
        insert_stmt = pg_insert(NotificationMessage).values(
            user_id=user_id,
            kind=kind,
            window_end=window_end,
            channel=channel,
            message_ts=message_ts,
            slack_user_id=slack_user_id,
            sent_at=sent_at,
        )
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["user_id", "kind", "window_end"],
            set_={
                "channel": insert_stmt.excluded.channel,
                "message_ts": insert_stmt.excluded.message_ts,
                "slack_user_id": insert_stmt.excluded.slack_user_id,
                "sent_at": insert_stmt.excluded.sent_at,
            },
        )
        session.exec(upsert_stmt)
        session.commit()
