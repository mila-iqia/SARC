"""Delivery tracking for usage notifications (SARC-802).

Writes one ``notification_messages`` row per delivered DM so the
acknowledgement rate has a denominator; the acknowledgement side (block-kit
buttons + the interactivity endpoint) matches clicks against these rows.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from sqlalchemy import and_, desc, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import col, select

from sarc.config import config
from sarc.db.notifications import NotificationFeedback, NotificationMessage

NotificationKind = Literal["underusage", "usage"]

ACK_ACTION_ID = "sarc_notification_ack"
HELP_ACTION_ID = "sarc_notification_help"
"""action_ids of the two underusage-DM buttons (see sarc.notifications.ack).
Defined here because the feedback tables and the acknowledgement-rate query
key on them; a help click is NOT an acknowledgement, so the rate query counts
only ACK_ACTION_ID feedback rows."""


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
    """Record a button click; True when newly recorded.

    Re-clicks of the same cycle's SAME button are dropped (ON CONFLICT DO
    NOTHING on the unique (slack_user_id, kind, window_end, action_id) key):
    the acknowledgement rate counts people, not clicks — first click wins —
    while a read click followed by a help click (or vice versa) keeps BOTH,
    so a request for help is never swallowed by an earlier ack.
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
                index_elements=["slack_user_id", "kind", "window_end", "action_id"]
            )
        )
        session.commit()
        return result.rowcount == 1


@dataclass(frozen=True)
class AckCycleStats:
    """Acknowledgement rate of one notification cycle."""

    kind: str
    window_end: datetime
    sent: int
    acknowledged: int

    @property
    def rate(self) -> float:
        return self.acknowledged / self.sent if self.sent else 0.0


def _ack_stats_stmt():
    """Per (kind, window_end) sent/acknowledged counts.

    Clicks join back on the (slack_user_id, kind, window_end) triple rather
    than on message_id — the triple stays exact even for clicks whose delivery
    row could not be resolved (a NULL message link still acknowledges its
    recipient's message). The join is narrowed to the ACK button's feedback:
    a help click is not an acknowledgement, and the narrowed join also keeps
    `sent` from counting a message twice for a user who clicked both buttons.
    Per-(button) uniqueness then means one click can never inflate a count.
    """
    return (
        select(
            col(NotificationMessage.kind).label("kind"),
            col(NotificationMessage.window_end).label("window_end"),
            func.count().label("sent"),
            func.count(col(NotificationFeedback.id)).label("acknowledged"),
        )
        .outerjoin(
            NotificationFeedback,
            and_(
                col(NotificationFeedback.slack_user_id)
                == col(NotificationMessage.slack_user_id),
                col(NotificationFeedback.kind) == col(NotificationMessage.kind),
                col(NotificationFeedback.window_end)
                == col(NotificationMessage.window_end),
                col(NotificationFeedback.action_id) == ACK_ACTION_ID,
            ),
        )
        .group_by(col(NotificationMessage.kind), col(NotificationMessage.window_end))
    )


def _stats(row) -> AckCycleStats:
    return AckCycleStats(
        kind=row.kind,
        window_end=row.window_end,
        sent=row.sent,
        acknowledged=row.acknowledged,
    )


def ack_stats_for_cycle(
    kind: NotificationKind, window_end: datetime
) -> AckCycleStats | None:
    """Sent/acknowledged counts of one cycle, or None when it has no tracked
    delivery (nothing sent through this tracker — e.g. a pre-SARC-802 cycle)."""
    stmt = _ack_stats_stmt().where(
        col(NotificationMessage.kind) == kind,
        col(NotificationMessage.window_end) == window_end,
    )
    with config.db.session() as session:
        row = session.exec(stmt).first()
    return _stats(row) if row is not None else None


def ack_rate_by_cycle(
    *, kind: NotificationKind | None = None, limit: int | None = None
) -> list[AckCycleStats]:
    """Per-cycle ack counts, most recent cycle first; all kinds unless
    filtered. ``limit`` caps the number of (kind, cycle) rows returned."""
    stmt = _ack_stats_stmt()
    if kind is not None:
        stmt = stmt.where(col(NotificationMessage.kind) == kind)
    stmt = stmt.order_by(
        desc(col(NotificationMessage.window_end)), col(NotificationMessage.kind)
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    with config.db.session() as session:
        return [_stats(row) for row in session.exec(stmt).all()]
