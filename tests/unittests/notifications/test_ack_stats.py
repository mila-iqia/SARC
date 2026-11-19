# ruff: noqa: F811 -- fixtures are imported from other test modules on purpose
"""Tests for the acknowledgement-rate reporting of SARC-802: the per-cycle
statistics helpers, the `usage ack-stats` CLI, and the previous-cycle
acknowledgement line in `usage notify`'s admin footer."""

from datetime import UTC, datetime, timedelta

import gifnoc
import pytest
from sqlmodel import select

from sarc.db.users import UserDB
from sarc.notifications.delivery import (
    AckCycleStats,
    ack_rate_by_cycle,
    ack_stats_for_cycle,
    record_notification_feedback,
    record_notification_send,
)
from tests.unittests.notifications.test_cli import (  # noqa: F401
    _CYCLE_WEEK,
    _NOTIFY_CFG,
    _patch_senders,
    notify_db,  # pytest fixture, reused from test_cli.py
)
from tests.unittests.notifications.test_delivery_tracking import (
    _mock_slack_delivered,
    _underusage_only,
)

_W1 = datetime(2024, 6, 2, tzinfo=UTC)  # previous cycle (2-week cadence)
_W2 = datetime(2024, 6, 16, tzinfo=UTC)
_CLICKED_AT = _W2 + timedelta(days=1)


def _user_ids(session, n):
    """The ids of at least n users, creating extras when the seeded set is
    smaller (the tables under test only need a users FK target)."""
    users = session.exec(select(UserDB)).all()[:n]
    while len(users) < n:
        extra = UserDB(
            display_name=f"ack-stats {len(users)}",
            email=f"ackstats{len(users)}@example.com",
        )
        session.add(extra)
        session.commit()  # record_notification_send commits elsewhere: must be visible
        users.append(extra)
    return [u.id for u in users]


def _seed_cycle(session, *, kind, window_end, sent, acked):
    """Record `sent` deliveries (slack ids U0..U{sent-1}) for one cycle, then
    clicks from the first `acked` of them."""
    ids = _user_ids(session, sent)
    for i in range(sent):
        record_notification_send(
            user_id=ids[i],
            kind=kind,
            window_end=window_end,
            channel="D0XYZ",
            message_ts=f"111.{i}",
            slack_user_id=f"U{i}",
            sent_at=window_end,
        )
    for i in range(acked):
        record_notification_feedback(
            slack_user_id=f"U{i}",
            kind=kind,
            window_end=window_end,
            action_id="sarc_notification_ack",
            channel="D0XYZ",
            message_ts=f"111.{i}",
            clicked_at=_CLICKED_AT,
        )


# ── stats helpers ───────────────────────────────────────────────────────────


def test_ack_stats_for_cycle(jobless_read_write_db):
    _seed_cycle(
        jobless_read_write_db, kind="underusage", window_end=_W2, sent=3, acked=2
    )

    stats = ack_stats_for_cycle("underusage", _W2)
    assert stats == AckCycleStats(
        kind="underusage", window_end=_W2, sent=3, acknowledged=2
    )
    assert stats.rate == pytest.approx(2 / 3)
    # Other kinds and unknown cycles have nothing tracked.
    assert ack_stats_for_cycle("usage", _W2) is None
    assert ack_stats_for_cycle("underusage", _W1) is None


def test_repeat_click_does_not_inflate_ack_count(jobless_read_write_db):
    _seed_cycle(
        jobless_read_write_db, kind="underusage", window_end=_W2, sent=2, acked=1
    )
    # Same user clicks again (deduped at insert): the count stays 1.
    record_notification_feedback(
        slack_user_id="U0",
        kind="underusage",
        window_end=_W2,
        action_id="sarc_notification_ack",
        channel="D0XYZ",
        message_ts="111.0",
        clicked_at=_CLICKED_AT + timedelta(hours=1),
    )
    stats = ack_stats_for_cycle("underusage", _W2)
    assert (stats.sent, stats.acknowledged) == (2, 1)


def test_ack_rate_by_cycle_ordering_and_filters(jobless_read_write_db):
    _seed_cycle(
        jobless_read_write_db, kind="underusage", window_end=_W1, sent=2, acked=2
    )
    _seed_cycle(
        jobless_read_write_db, kind="underusage", window_end=_W2, sent=4, acked=1
    )
    _seed_cycle(jobless_read_write_db, kind="usage", window_end=_W2, sent=5, acked=0)

    stats = ack_rate_by_cycle()
    # Most recent cycle first; ties broken by kind (alphabetical: underusage
    # before usage).
    assert [(s.kind, s.window_end, s.sent, s.acknowledged) for s in stats] == [
        ("underusage", _W2, 4, 1),
        ("usage", _W2, 5, 0),
        ("underusage", _W1, 2, 2),
    ]

    only_underusage = ack_rate_by_cycle(kind="underusage")
    assert [s.kind for s in only_underusage] == ["underusage", "underusage"]
    assert [s.window_end for s in only_underusage] == [_W2, _W1]

    assert len(ack_rate_by_cycle(limit=1)) == 1


# ── usage ack-stats CLI ─────────────────────────────────────────────────────


def test_ack_stats_cli_prints_table(jobless_read_write_db, cli_main, capsys):
    _seed_cycle(
        jobless_read_write_db, kind="underusage", window_end=_W2, sent=3, acked=2
    )
    _seed_cycle(jobless_read_write_db, kind="usage", window_end=_W2, sent=1, acked=0)

    assert cli_main(["usage", "ack-stats"]) == 0
    out = capsys.readouterr().out
    lines = out.splitlines()
    assert "window_end" in lines[0] and "rate" in lines[0]
    assert "2024-06-16  underusage" in lines[1]
    assert "67%" in lines[1]  # 2/3, rounded
    assert "2024-06-16  usage" in lines[2]

    capsys.readouterr()
    assert cli_main(["usage", "ack-stats", "--kind", "usage"]) == 0
    out = capsys.readouterr().out
    assert "usage" in out and "underusage" not in out


def test_ack_stats_cli_bad_kind(jobless_read_write_db, cli_main, capsys):
    assert cli_main(["usage", "ack-stats", "--kind", "digest"]) == -1
    assert "Unknown kind" in capsys.readouterr().out


def test_ack_stats_cli_no_data_yet(jobless_read_write_db, cli_main, capsys):
    assert cli_main(["usage", "ack-stats"]) == 0
    assert "No tracked notification deliveries yet." in capsys.readouterr().out


# ── previous-cycle line in the notify footer ────────────────────────────────


def test_notify_footer_shows_previous_cycle_ack_rate(
    notify_db, cli_main, monkeypatch, capsys
):
    # Last cycle (window end 2024-06-02): 2 of 3 flagged users acked.
    _seed_cycle(notify_db, kind="underusage", window_end=_W1, sent=3, acked=2)
    slack_cls, _ = _mock_slack_delivered()
    _patch_senders(monkeypatch, slack_cls)
    with gifnoc.overlay({"sarc.notifications": _underusage_only()}):
        assert cli_main(["usage", "notify", "--as-of", _CYCLE_WEEK, "--send"]) == 0

    out = capsys.readouterr().out
    assert "=== Delivery Summary ===" in out
    assert "acknowledged for 2024-06-02: 2/3 (67%)" in out


def test_notify_footer_omits_ack_line_without_history(
    notify_db, cli_main, monkeypatch, capsys
):
    """No tracked previous cycle (pre-SARC-802 history, or first run): the
    footer simply stays as before."""
    slack_cls, _ = _mock_slack_delivered()
    _patch_senders(monkeypatch, slack_cls)
    with gifnoc.overlay({"sarc.notifications": _underusage_only()}):
        assert cli_main(["usage", "notify", "--as-of", _CYCLE_WEEK, "--send"]) == 0

    out = capsys.readouterr().out
    assert "=== Delivery Summary ===" in out
    assert "acknowledged for" not in out
