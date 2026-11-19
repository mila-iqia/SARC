# ruff: noqa: F811 -- fixtures are imported from test_cli.py on purpose
"""DB tests for feedback tracking helpers (SARC-802).

Direct tests of the (user, kind, window_end) resolution and the
first-click-wins upsert that the interactivity endpoint builds on.
"""

from datetime import UTC, datetime

from sqlmodel import select

from sarc.db.notifications import NotificationFeedback
from sarc.db.users import UserDB
from sarc.notifications.delivery import (
    find_notification_message_id,
    record_notification_feedback,
    record_notification_send,
)

_WINDOW_END = datetime(2024, 6, 16, tzinfo=UTC)
_OTHER_END = datetime(2024, 7, 14, tzinfo=UTC)
_CLICKED_AT = datetime(2024, 6, 17, 9, 30, tzinfo=UTC)


def _a_user_id(session):
    return session.exec(select(UserDB)).first().id


def _record_send(session, *, kind="underusage", window_end=_WINDOW_END):
    record_notification_send(
        user_id=_a_user_id(session),
        kind=kind,
        window_end=window_end,
        channel="D0XYZ",
        message_ts="111.222",
        slack_user_id="U0ABC",
        sent_at=window_end,
    )


# ── find_notification_message_id ───────────────────────────────────────────


def test_find_message_id_matches_the_unique_triple(jobless_read_write_db):
    session = jobless_read_write_db
    _record_send(session)

    assert (
        find_notification_message_id(
            slack_user_id="U0ABC", kind="underusage", window_end=_WINDOW_END
        )
        is not None
    )
    # Any leg of the key mismatching → no match.
    assert (
        find_notification_message_id(
            slack_user_id="UOTHER", kind="underusage", window_end=_WINDOW_END
        )
        is None
    )
    assert (
        find_notification_message_id(
            slack_user_id="U0ABC", kind="usage", window_end=_WINDOW_END
        )
        is None
    )
    assert (
        find_notification_message_id(
            slack_user_id="U0ABC", kind="underusage", window_end=_OTHER_END
        )
        is None
    )


# ── record_notification_feedback ────────────────────────────────────────────


def test_first_click_recorded_with_message_link(jobless_read_write_db):
    session = jobless_read_write_db
    _record_send(session)
    message_id = find_notification_message_id(
        slack_user_id="U0ABC", kind="underusage", window_end=_WINDOW_END
    )

    newly = record_notification_feedback(
        slack_user_id="U0ABC",
        kind="underusage",
        window_end=_WINDOW_END,
        action_id="sarc_notification_ack",
        channel="D0XYZ",
        message_ts="111.222",
        clicked_at=_CLICKED_AT,
        message_id=message_id,
    )
    assert newly is True

    session.expire_all()
    (row,) = session.exec(select(NotificationFeedback)).all()
    assert row.message_id == message_id
    assert row.slack_user_id == "U0ABC"
    assert row.kind == "underusage"
    assert row.window_end == _WINDOW_END
    assert row.clicked_at == _CLICKED_AT


def test_repeat_click_dropped_first_click_wins(jobless_read_write_db):
    assert (
        record_notification_feedback(
            slack_user_id="U0ABC",
            kind="underusage",
            window_end=_WINDOW_END,
            action_id="sarc_notification_ack",
            channel="D0XYZ",
            message_ts="111.222",
            clicked_at=_CLICKED_AT,
        )
        is True
    )
    # Second click on the same cycle's button (e.g. after a failed
    # chat.update): dropped — the numerator counts people, not clicks.
    assert (
        record_notification_feedback(
            slack_user_id="U0ABC",
            kind="underusage",
            window_end=_WINDOW_END,
            action_id="sarc_notification_ack",
            channel="D0XYZ",
            message_ts="111.222",
            clicked_at=_CLICKED_AT.replace(hour=10),
        )
        is False
    )
    rows = jobless_read_write_db.exec(select(NotificationFeedback)).all()
    assert len(rows) == 1
    assert rows[0].clicked_at == _CLICKED_AT


def test_click_without_tracked_delivery_is_still_recorded(jobless_read_write_db):
    """A button click whose delivery row is missing (message sent before
    tracking existed, or --send-to debug copy) keeps its feedback row with a
    NULL message link — the click is real, the join just can't resolve."""
    assert (
        record_notification_feedback(
            slack_user_id="U0ABC",
            kind="underusage",
            window_end=_OTHER_END,
            action_id="sarc_notification_ack",
            channel="D0XYZ",
            message_ts="333.444",
            clicked_at=_CLICKED_AT,
            message_id=find_notification_message_id(
                slack_user_id="U0ABC", kind="underusage", window_end=_OTHER_END
            ),
        )
        is True
    )
    rows = jobless_read_write_db.exec(select(NotificationFeedback)).all()
    assert len(rows) == 1
    assert rows[0].message_id is None
