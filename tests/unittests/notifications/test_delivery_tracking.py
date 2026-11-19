# ruff: noqa: F811 -- fixtures are imported from test_cli.py on purpose
"""Tests for notification delivery tracking (SARC-802).

Runs `sarc usage notify --send` against the existing notify fixtures with a
mocked Slack client and asserts each delivered DM leaves exactly one
notification_messages row — and that nothing else does (failed sends, debug
--send-to redirects, degraded OK results, re-runs).
"""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

import gifnoc
from sqlmodel import select

from sarc.db.notifications import NotificationMessage
from sarc.db.users import UserDB
from sarc.notifications.slack import SendResult, SendStatus
from tests.unittests.notifications.test_cli import (  # noqa: F401
    _CYCLE_WEEK,
    _NOTIFY_CFG,
    _USAGE_REPORT_WEEK,
    _patch_senders,
    notify_db,  # pytest fixture, reused from test_cli.py
    usage_report_db,  # pytest fixture, reused from test_cli.py
)

_WINDOW_END_CYCLE_WEEK = datetime(2024, 6, 16, tzinfo=UTC)
_WINDOW_END_USAGE_WEEK = datetime(2024, 7, 14, tzinfo=UTC)


def _mock_slack_delivered(dm_status=SendStatus.OK, ts="111.222"):
    """SlackClient class mock whose dm_user returns a fully identified OK."""
    inst = MagicMock()
    inst.dm_user.return_value = SendResult(
        dm_status, ts=ts, slack_user_id="U0ABC", channel="D0XYZ"
    )
    inst.post_channel.return_value = SendResult(SendStatus.OK, ts="999.000")
    inst.upload_files.return_value = SendResult(SendStatus.OK)
    return MagicMock(return_value=inst), inst


def _recorded(session):
    """All notification_messages rows, freshest commit first.

    expire_all() first: record_notification_send commits in its own session,
    so without this a query on the test session can return stale objects.
    """
    session.expire_all()
    return session.exec(
        select(NotificationMessage).order_by(
            NotificationMessage.user_id, NotificationMessage.kind
        )
    ).all()


def _ids_by_email(session):
    return {u.email: u.id for u in session.exec(select(UserDB)).all() if u.email}


def _underusage_only(**extra):
    return {
        **_NOTIFY_CFG,
        "send_underusage_report": True,
        "send_usage_report": False,
        **extra,
    }


# ── underusage DMs ─────────────────────────────────────────────────────────


def test_underusage_dms_record_one_row_each(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack_delivered()
    _patch_senders(monkeypatch, slack_cls)
    with gifnoc.overlay({"sarc.notifications": _underusage_only()}):
        rc = cli_main(["usage", "notify", "--as-of", _CYCLE_WEEK, "--send"])
    assert rc == 0

    ids = _ids_by_email(notify_db)
    dm_emails = {c.args[0] for c in slack_inst.dm_user.call_args_list}
    assert dm_emails, "fixture must flag at least one underuser"

    rows = _recorded(notify_db)
    assert len(rows) == len(dm_emails)
    assert {r.user_id for r in rows} == {ids[e] for e in dm_emails}
    for row in rows:
        assert row.kind == "underusage"
        assert row.window_end == _WINDOW_END_CYCLE_WEEK
        assert row.channel == "D0XYZ"
        assert row.message_ts == "111.222"
        assert row.slack_user_id == "U0ABC"
        assert row.sent_at is not None and row.sent_at.tzinfo is not None


# ── usage-report DMs ───────────────────────────────────────────────────────


def test_usage_and_underusage_kinds_are_tracked_apart(
    usage_report_db, cli_main, monkeypatch
):
    slack_cls, _ = _mock_slack_delivered()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["usage", "notify", "--as-of", _USAGE_REPORT_WEEK, "--send"])
    assert rc == 0

    ids = _ids_by_email(usage_report_db)
    rows = _recorded(usage_report_db)
    by_user = {r.user_id: r.kind for r in rows}
    # petitbonhomme: underuser → underusage DM only;
    # beaubonhomme: active user → usage DM only (see usage_report_db fixture).
    assert by_user == {
        ids["petitbonhomme@mila.quebec"]: "underusage",
        ids["beaubonhomme@mila.quebec"]: "usage",
    }
    assert all(r.window_end == _WINDOW_END_USAGE_WEEK for r in rows)


# ── nothing recorded that should not be ────────────────────────────────────


def test_failed_dms_are_not_recorded(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack_delivered(dm_status=SendStatus.FAILED)
    _patch_senders(monkeypatch, slack_cls)
    with gifnoc.overlay({"sarc.notifications": _underusage_only()}):
        rc = cli_main(["usage", "notify", "--as-of", _CYCLE_WEEK, "--send"])
    assert rc == 0
    assert slack_inst.dm_user.call_count > 0
    assert _recorded(notify_db) == []


def test_send_to_redirect_is_not_recorded(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack_delivered()
    _patch_senders(monkeypatch, slack_cls)
    with gifnoc.overlay({"sarc.notifications": _underusage_only()}):
        rc = cli_main(
            [
                "usage",
                "notify",
                "--as-of",
                _CYCLE_WEEK,
                "--send",
                "--send-to",
                "developer@example.com",
            ]
        )
    assert rc == 0
    # DMs did go out (to the developer) but nothing lands in the tracking table.
    assert slack_inst.dm_user.call_count > 0
    assert all(
        c.args[0] == "developer@example.com" for c in slack_inst.dm_user.call_args_list
    )
    assert _recorded(notify_db) == []


def test_ok_result_without_message_id_is_not_recorded(
    notify_db, cli_main, monkeypatch, caplog
):
    """A SendResult OK lacking ts/identity (degraded or bare-mock client) is a
    delivery success but unrecordable — logged at ERROR, no row, no crash."""
    inst = MagicMock()
    inst.dm_user.return_value = SendResult(SendStatus.OK)
    inst.post_channel.return_value = SendResult(SendStatus.OK, ts="999.000")
    inst.upload_files.return_value = SendResult(SendStatus.OK)
    _patch_senders(monkeypatch, MagicMock(return_value=inst))
    with caplog.at_level(logging.ERROR, logger="sarc.cli.usage.notify"):
        with gifnoc.overlay({"sarc.notifications": _underusage_only()}):
            rc = cli_main(["usage", "notify", "--as-of", _CYCLE_WEEK, "--send"])
    assert rc == 0
    assert inst.dm_user.call_count > 0
    assert _recorded(notify_db) == []
    assert any("Not recording" in r.message for r in caplog.records)


# ── re-run: last send wins, denominator unchanged ──────────────────────────


def test_rerun_same_cycle_upserts_single_row(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack_delivered(ts="111.111")
    _patch_senders(monkeypatch, slack_cls)
    with gifnoc.overlay({"sarc.notifications": _underusage_only()}):
        assert cli_main(["usage", "notify", "--as-of", _CYCLE_WEEK, "--send"]) == 0
    first_rows = _recorded(notify_db)
    assert first_rows and all(r.message_ts == "111.111" for r in first_rows)

    # Same window re-sent (e.g. after a partial failure): no duplicate rows,
    # and the stored message id is the one from the latest delivery.
    slack_inst.dm_user.return_value = SendResult(
        SendStatus.OK, ts="222.222", slack_user_id="U0ABC", channel="D0XYZ"
    )
    with gifnoc.overlay({"sarc.notifications": _underusage_only()}):
        assert cli_main(["usage", "notify", "--as-of", _CYCLE_WEEK, "--send"]) == 0

    second_rows = _recorded(notify_db)
    assert len(second_rows) == len(first_rows)
    assert all(r.message_ts == "222.222" for r in second_rows)
