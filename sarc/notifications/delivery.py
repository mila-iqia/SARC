"""Delivery tracking for usage notifications (SARC-802).

Writes one ``notification_messages`` row per delivered DM so the
acknowledgement rate has a denominator; the acknowledgement side (block-kit
buttons + the interactivity endpoint) matches clicks against these rows.
"""

from datetime import datetime
from typing import Literal

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import col, select

from sarc.config import config
from sarc.db.notifications import NotificationFeedback, NotificationMessage

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


def find_notification_message_id(
    *, slack_user_id: str, kind: NotificationKind, window_end: datetime
) -> int | None:
    """The tracked delivery row a click resolves to, or None.

    Matches on the (user, kind, window_end) unique key — see the module
    docstring of `sarc.notifications.ack` for why the click carries no id.
    """
    with config.db.session() as session:
        stmt = (
            select(NotificationMessage.id)
            .where(
                col(NotificationMessage.slack_user_id) == slack_user_id,
                col(NotificationMessage.kind) == kind,
                col(NotificationMessage.window_end) == window_end,
            )
            .limit(1)
        )
        return session.exec(stmt).first()


def record_notification_feedback(
    *,
    slack_user_id: str,
    kind: NotificationKind,
    window_end: datetime,
    action_id: str,
    channel: str,
    message_ts: str,
    clicked_at: datetime,
    message_id: int | None = None,
) -> bool:
    """Record an acknowledgement click; True when newly recorded.

    Re-clicks of the same cycle's button are dropped (ON CONFLICT DO NOTHING
    on the unique (slack_user_id, kind, window_end) key) so the numerator of
    the acknowledgement rate counts people, not clicks — the first click wins.
    """
    with config.db.session() as session:
        insert_stmt = pg_insert(NotificationFeedback).values(
            message_id=message_id,
            slack_user_id=slack_user_id,
            kind=kind,
            window_end=window_end,
            action_id=action_id,
            channel=channel,
            message_ts=message_ts,
            clicked_at=clicked_at,
        )
        result = session.exec(
            insert_stmt.on_conflict_do_nothing(
                index_elements=["slack_user_id", "kind", "window_end"]
            )
        )
        session.commit()
        return result.rowcount == 1
