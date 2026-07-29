"""Tests for `sarc notify usage` CLI command."""

import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import gifnoc
import pytest
from sqlmodel import select

from sarc.db.cluster import SlurmClusterDB
from sarc.db.users import UserDB
from sarc.notifications.slack import SendResult, SendStatus
from sarc.notifications.usage import (
    get_recurring_underusers as _real_get_recurring_underusers,
)
from sarc.notifications.usage import get_underusers_usage as _real_get_underusers_usage
from sarc.notifications.usage import get_users_usage as _real_get_users_usage
from tests.unittests.notifications._factory import (
    UNDERUSAGE_REPORT_TEMPLATE,
    USAGE_REPORT_TEMPLATE,
    add_gpu_job,
)

_MILA_GPU_TYPE = "A100-SXM4-80GB"

_CLI_TEST_END = datetime(
    2024, 6, 24, tzinfo=UTC
)  # ISO week 26 — multiple of usage_cycle_length_weeks

_NOTIFY_JOB_END = datetime(2024, 6, 10, tzinfo=UTC)

_NOTIFY_CFG = {
    "slack_underusage": {
        "description": "test channel",
        "token": "xoxb-test-token",
        "channel": "#test-channel",
    },
    "slack_usage": {
        "description": "test channel",
        "token": "xoxb-test-token",
        "channel": "#test-channel",
    },
    "underusage_report_template": UNDERUSAGE_REPORT_TEMPLATE,
    "usage_report_template": USAGE_REPORT_TEMPLATE,
    "dashboard_url": "https://dash.example.com",
    "enabled": True,
    "send_underusage_report": True,
    "min_waste_ratio": 0.50,
    "min_waste_rgu_hours": 672.0,
    "digest_top_n": 16,
    "send_usage_report": True,
    "usage_report_min_usage_rgu_hours": 0,
}


def _add_gpu_job(session, *, end_time: datetime, elapsed_h: float, **kwargs):
    """Seed a job whose end_time is the given anchor."""
    return add_gpu_job(
        session,
        submit_time=end_time - timedelta(hours=elapsed_h),
        elapsed_h=elapsed_h,
        **kwargs,
    )


@pytest.fixture
def notify_db(read_write_db):
    """Seed one high-wasting user (petitbonhomme) into the test DB."""
    session = read_write_db
    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}
    mila_id = clusters["mila"].id
    petitbonhomme_id = users["petitbonhomme"].id
    beaubonhomme_id = users["beaubonhomme"].id
    bramin_id = users["bramin"].id

    # 700 GPU-hours @ 10 % util → rgu_h = 4.8 * 700 = 3360, waste_ratio = 0.90 ≥ 0.50
    # rgu_h ≥ 672 (floor) ✓
    _add_gpu_job(
        session,
        user_id=petitbonhomme_id,
        cluster_id=mila_id,
        elapsed_h=700,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.10,
        job_id=70001,
        end_time=_NOTIFY_JOB_END,
    )

    _add_gpu_job(
        session,
        user_id=beaubonhomme_id,
        cluster_id=mila_id,
        elapsed_h=700,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.10,
        job_id=70002,
        end_time=_NOTIFY_JOB_END - timedelta(hours=1),
    )

    _add_gpu_job(
        session,
        user_id=bramin_id,
        cluster_id=mila_id,
        elapsed_h=700,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.10,
        job_id=70003,
        end_time=_NOTIFY_JOB_END + timedelta(hours=1),
    )
    session.commit()
    yield session


# ── basic exit code + dry-run marker ─────────────────────────────────────────


def test_dry_run(notify_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.usage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "usage"])
    captured = capsys.readouterr()
    out = captured.out

    # Exit code
    assert rc == 0
    # DRY RUN marker
    assert "DRY RUN" in captured.err

    # User in recipients
    assert "petitbonhomme@mila.quebec" in out

    # Admin digest
    assert "Admin Digest" in out
    assert "Weekly GPU Underusage Digest" in out

    # _CLI_TEST_END is ISO week 26 — multiple of usage_cycle_length_weeks → shows DMs
    # DM preview
    assert "=== Under Usage Report Previews" in out
    # display_name = "M/Ms Petitbonhomme"; _first_name() returns the first token
    assert "M/Ms" in out


# ── usage-cycle cadence gating ────────────────────────────────────────────

_CYCLE_WEEK = "2024-06-16"  # ISO week 24 — multiple of usage_cycle_length_weeks
_OFF_CYCLE_WEEK = (
    "2024-06-23"  # ISO week 25 — not a multiple of usage_cycle_length_weeks
)


def test_cycle_week_shows_dm_previews(notify_db, cli_main, capsys):
    # 2024-06-16 is ISO week 24 — multiple of usage_cycle_length_weeks;
    # job at 2024-06-10 is inside [2024-06-02, 2024-06-16]
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}) as config:
        usage_cycle_length_weeks = config.sarc.notifications.usage_cycle_length_weeks
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK])
    assert rc == 0
    captured = capsys.readouterr()
    assert "=== Under Usage Report Previews" in captured.out
    assert f"multiple of {usage_cycle_length_weeks}" in captured.err


def test_off_cycle_week_suppresses_dm_previews(notify_db, cli_main, capsys):
    # 2024-06-23 is ISO week 25 — not a multiple of usage_cycle_length_weeks;
    # job at 2024-06-10 is inside [2024-06-09, 2024-06-23]
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "usage", "--as-of", _OFF_CYCLE_WEEK])
    assert rc == 0
    err = capsys.readouterr().err
    assert "=== Under Usage Report Previews" not in err
    assert "Underusage report eligible this run" not in err


# ── year-boundary regression ──────────────────────────────────────────────────


@pytest.fixture
def year_boundary_db(read_write_db):
    """Seed one high-wasting user with a job in late-December 2024."""
    session = read_write_db
    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}
    _add_gpu_job(
        session,
        user_id=users["petitbonhomme"].id,
        cluster_id=clusters["mila"].id,
        elapsed_h=700,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.10,
        job_id=70004,
        end_time=datetime(2024, 12, 27, tzinfo=UTC),
    )
    session.commit()
    yield session


def test_year_boundary_window_is_correct(year_boundary_db, cli_main, capsys):
    # 2025-01-05 is ISO week 1 — not a multiple of usage_cycle_length_weeks;
    # window [2024-12-22, 2025-01-05] spans the year boundary and covers the
    # Dec-27 job.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "usage", "--as-of", "2025-01-05"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "2024-12-22" in captured.out  # window start in digest header
    assert "2025-01-05" in captured.out  # window end in digest header
    assert "petitbonhomme@mila.quebec" in captured.out  # job is inside the window
    assert (
        "Underusage report eligible this run" not in captured.err
    )  # week 1 is not a multiple of usage_cycle_length_weeks → no Under Usage Report


# ── future anchor guard ───────────────────────────────────────────────────────


def test_future_anchor_prints_note_and_does_not_crash(notify_db, cli_main, capsys):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "usage", "--as-of", "2099-01-01"])
    assert rc == 0
    assert "future" in capsys.readouterr().err


# ── invalid --as-of ───────────────────────────────────────────────────────────


def test_invalid_as_of_returns_error(notify_db, cli_main, caplog):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "usage", "--as-of", "not-a-date"])
    assert rc == -1
    assert any("not-a-date" in r.message for r in caplog.records)


# ── window-end clipped to midnight ───────────────────────────────────────


def test_now_clipped_to_midnight(notify_db, cli_main, monkeypatch, capsys):
    # _now_utc returns 15:30 UTC; the window end in the digest header must still
    # show the date only (derived from end.date()), and the period start must be
    # exactly usage_report_cycles × usage_cycle_length_weeks weeks before
    # midnight on that date.
    monkeypatch.setattr(
        "sarc.cli.notify.usage._now_utc",
        lambda: _NOTIFY_JOB_END + timedelta(hours=15, minutes=30),
    )
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "usage"])
    assert rc == 0
    out = capsys.readouterr().out
    # window: [2024-05-27, 2024-06-10] (midnight-to-midnight,
    # usage_report_cycles × usage_cycle_length_weeks weeks)
    assert "2024-06-10" in out
    assert "2024-05-27" in out
    assert "petitbonhomme@mila.quebec" not in out  # job is not inside the window
    assert "beaubonhomme@mila.quebec" in out  # job is inside the window (-1h)
    assert "bramin@mila.quebec" not in out  # job not inside the window (+1h)


# ── enabled kill-switch ───────────────────────────────────────────────────


def test_enabled_false_returns_zero_without_sending(cli_main, monkeypatch):
    slack_cls = MagicMock()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {
        **_NOTIFY_CFG,
        "enabled": False,
        "send_underusage_report": True,
        "send_usage_report": True,
    }
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK, "--send"])
    assert rc == 0
    slack_cls.assert_not_called()


# ── missing config ────────────────────────────────────────────────────────────


def test_missing_notifications_config_returns_error(cli_main, caplog):
    # base_config (autouse) does not set sarc.notifications → must return -1
    rc = cli_main(["notify", "usage"])
    assert rc == -1


# ── --send flag wires actual sending ──────────────────────────────────────


def _mock_slack(dm_status=SendStatus.OK, channel_status=SendStatus.OK):
    inst = MagicMock()
    inst.dm_user.return_value = SendResult(dm_status)
    ts = "111.222" if channel_status == SendStatus.OK else None
    inst.post_channel.return_value = SendResult(channel_status, ts=ts)
    cls = MagicMock(return_value=inst)
    return cls, inst


def _channel_posts(inst):
    """Split post_channel calls into (digest posts, thread replies).

    The digest post never passes thread_ts; the footer replies always do
    (possibly as None when the digest post failed).
    """
    calls = inst.post_channel.call_args_list
    digests = [c for c in calls if "thread_ts" not in c.kwargs]
    replies = [c for c in calls if "thread_ts" in c.kwargs]
    return digests, replies


def _patch_senders(monkeypatch, slack_cls):
    monkeypatch.setattr("sarc.cli.notify.usage.SlackClient", slack_cls)


def test_dry_run_does_not_instantiate_slack(notify_db, cli_main, monkeypatch):
    slack_cls = MagicMock()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": False}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK])
    assert rc == 0
    slack_cls.assert_not_called()


def test_send_cycle_week_posts_digest_and_dms(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": False}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK, "--send"])
    assert rc == 0
    # week 24 is also a usage-report week (24 % 4 == 0) → digest + 2 replies
    digests, replies = _channel_posts(slack_inst)
    assert len(digests) == 1
    assert len(replies) == 2
    assert slack_inst.dm_user.call_count == 3


def test_send_footers_target_the_underusage_channel(notify_db, cli_main, monkeypatch):
    """Regression: both delivery-summary footers must land in the digest's own
    thread (slack_underusage's channel), never slack_usage's channel — a
    thread_ts only resolves within the channel its parent message was posted
    to, and slack_usage's bot token isn't even scoped for channel posts."""
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {
        **_NOTIFY_CFG,
        "slack_underusage": {
            **_NOTIFY_CFG["slack_underusage"],
            "channel": "#underusage-channel",
        },
        "slack_usage": {**_NOTIFY_CFG["slack_usage"], "channel": "#usage-channel"},
        "send_underusage_report": True,
        "send_usage_report": True,
    }
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK, "--send"])
    assert rc == 0
    calls = slack_inst.post_channel.call_args_list
    assert len(calls) == 3  # digest + underusage footer + usage footer
    assert all(c.args[0] == "#underusage-channel" for c in calls)


def test_send_off_cycle_week_posts_digest_only(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _OFF_CYCLE_WEEK, "--send"])
    assert rc == 0
    # off-cycle week → digest only
    digests, replies = _channel_posts(slack_inst)
    assert len(digests) == 1
    assert len(replies) == 0
    slack_inst.dm_user.assert_not_called()


def test_send_no_dms_flag_skips_dms(notify_db, cli_main, monkeypatch, capsys):
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK, "--send", "--no-dms"])
    assert rc == 0
    digests, replies = _channel_posts(slack_inst)
    assert len(digests) == 1
    assert len(replies) == 2  # week 24 is also a usage-report week
    slack_inst.dm_user.assert_not_called()
    out = capsys.readouterr().out
    summary = out[out.index("=== Delivery Summary ===") :]
    assert "flagged=3" in summary
    assert "skipped=3" in summary
    assert "dm_sent=0" in summary


def test_send_underusage_report_false_suppresses_underusage_report(
    notify_db, cli_main, monkeypatch, capsys
):
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": False, "send_usage_report": False}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK, "--send"])
    assert rc == 0
    digests, replies = _channel_posts(slack_inst)
    assert len(digests) == 1
    assert len(replies) == 2  # week 24 is also a usage-report week
    slack_inst.dm_user.assert_not_called()
    out = capsys.readouterr().out
    summary = out[out.index("=== Delivery Summary ===") :]
    assert "flagged=3" in summary
    assert "skipped=3" in summary
    assert "dm_sent=0" in summary


def test_send_dm_failure_surfaced_in_footer(notify_db, cli_main, monkeypatch, capsys):
    slack_cls, slack_inst = _mock_slack()
    slack_inst.dm_user.return_value = SendResult(SendStatus.FAILED, "channel_not_found")
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": False}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK, "--send"])
    assert rc == 0  # failures reported, run does not crash
    out = capsys.readouterr().out
    assert "failed=3" in out
    # The digest stays pure content; the delivery summaries (with the failed
    # user emails) are posted as replies in the digest's thread.
    digests, replies = _channel_posts(slack_inst)
    posted_digest = digests[0].args[1]
    assert "Delivery Summary" not in posted_digest
    assert "Usage Report Summary" not in posted_digest
    footer_reply = replies[0]
    assert footer_reply.kwargs["thread_ts"] == "111.222"
    assert "Delivery Summary" in footer_reply.args[1]
    assert "channel_not_found" in footer_reply.args[1]


def test_send_digest_failure_posts_footers_unthreaded(notify_db, cli_main, monkeypatch):
    """No digest ts to thread on → summaries land as regular channel posts."""
    slack_cls, slack_inst = _mock_slack(channel_status=SendStatus.FAILED)
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": False}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK, "--send"])
    assert rc == 0
    digests, replies = _channel_posts(slack_inst)
    assert len(digests) == 1
    assert len(replies) == 2  # week 24 is also a usage-report week
    for reply in replies:
        assert reply.kwargs["thread_ts"] is None


# ── usage report ──────────────────────────────────────────────────────────────
# Usage report period = usage_report_cycles × usage_cycle_length_weeks (= 4 weeks).
# ISO week 28 (2024-07-14) → multiple of the usage-report period → usage report eligible.
# ISO week 26 (2024-06-30) → multiple of usage_cycle_length_weeks only → underusage DMs, no usage report.
_USAGE_REPORT_WEEK = "2024-07-14"  # wk 28 — multiple of the usage-report period
_CYCLE_NON_REPORT_WEEK = "2024-06-30"  # wk 26 — cycle week but not a usage-report week


@pytest.fixture
def usage_report_db(read_write_db):
    """Two users with GPU jobs inside [2024-06-16, 2024-07-14]:
    - petitbonhomme: high waste → underuser (gets the under usage report, not the usage report)
    - beaubonhomme:  low waste  → active user (gets the usage report)

    beaubonhomme also has a job in May, inside the wider [2024-05-05, 2024-06-16]
    window used by the N=3 usage-report cadence test (usage_report_cycles(2) x 3
    = 6-week period, aligned to ISO week 24).
    """
    session = read_write_db
    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}
    mila_id = clusters["mila"].id

    # petitbonhomme: 700 h @ 10 % → waste_ratio=0.90, wasted=3024 >> 672 floor → underuser
    _add_gpu_job(
        session,
        user_id=users["petitbonhomme"].id,
        cluster_id=mila_id,
        elapsed_h=700,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.10,
        job_id=80001,
        end_time=datetime(2024, 6, 29, tzinfo=UTC),
    )
    _add_gpu_job(
        session,
        user_id=users["petitbonhomme"].id,
        cluster_id=mila_id,
        elapsed_h=700,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.10,
        job_id=80002,
        end_time=datetime(2024, 7, 7, tzinfo=UTC),
    )
    # beaubonhomme: 100 h @ 90 % → waste_ratio=0.10, wasted=48 < 672 floor → NOT underuser
    _add_gpu_job(
        session,
        user_id=users["beaubonhomme"].id,
        cluster_id=mila_id,
        elapsed_h=100,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.90,
        job_id=80003,
        end_time=datetime(2024, 6, 29, tzinfo=UTC),
    )
    # beaubonhomme: same shape, but in May — only inside the N=3 6-week report window.
    _add_gpu_job(
        session,
        user_id=users["beaubonhomme"].id,
        cluster_id=mila_id,
        elapsed_h=100,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.90,
        job_id=80004,
        end_time=datetime(2024, 5, 20, tzinfo=UTC),
    )
    session.commit()
    yield session


def test_usage_report_week_underuser_not_in_report_previews(
    usage_report_db, cli_main, monkeypatch, capsys
):
    """petitbonhomme is an underuser — they get the under usage report, not the usage report."""
    slack_cls = MagicMock()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _USAGE_REPORT_WEEK])
    assert rc == 0
    out = capsys.readouterr().out
    assert "=== Under Usage Report Previews" in out
    assert "=== Usage Report Previews" in out
    # petitbonhomme should appear in Under Usage Report previews, not usage report previews
    dm_section = out[
        out.find("=== Under Usage Report Previews ===") : out.find(
            "=== Usage Report Previews"
        )
    ]
    assert "petitbonhomme@mila.quebec" in dm_section
    usage_section = out[out.find("=== Usage Report Previews") :]
    assert "petitbonhomme@mila.quebec" not in usage_section
    assert "beaubonhomme@mila.quebec" in usage_section
    # Regression: the usage-report query excludes underusers at the DB level, so
    # there is no longer a way to know how many were excluded without a second
    # query — the header must not claim a (possibly wrong) count.
    assert "=== Usage Report Previews (1 recipient(s)) ===" in out
    assert "already getting the underusage alert" not in out

    # Dry-run never instantiates senders
    slack_cls.assert_not_called()


def test_non_usage_report_week_no_report_section(
    usage_report_db, cli_main, monkeypatch, capsys
):
    """Multiple of usage_cycle_length_weeks but not of usage_report_cycles ×
    usage_cycle_length_weeks → no usage report section."""
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        cli_main(["notify", "usage", "--as-of", _CYCLE_NON_REPORT_WEEK, "--send"])
    out = capsys.readouterr().out
    assert "=== Under Usage Report Previews" in out
    assert "=== Usage Report Previews" not in out

    # Digest + Delivery Summary reply only — no Usage Report Summary on a
    # non-report week; no usage report DMs
    digests, replies = _channel_posts(slack_inst)
    assert len(digests) == 1
    assert len(replies) == 1
    assert "Usage Report Summary" not in replies[0].args[1]
    # beaubonhomme is not an underuser, and week 26 is not a usage-report week
    # → no DM of either kind
    dm_calls = [call.args[0] for call in slack_inst.dm_user.call_args_list]
    assert "beaubonhomme@mila.quebec" not in dm_calls


def test_send_usage_report_disabled_no_report_sends(
    usage_report_db, cli_main, monkeypatch, capsys
):
    """send_usage_report=False → no usage report DMs even on a report week."""
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": False, "send_usage_report": False}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _USAGE_REPORT_WEEK, "--send"])
    assert rc == 0
    # Digest + both summary replies (report week); no DMs for underusers or
    # report recipients
    digests, replies = _channel_posts(slack_inst)
    assert len(digests) == 1
    assert len(replies) == 2
    # dm_user may be called 0 times (send_underusage_report=False, send_usage_report=False)
    slack_inst.dm_user.assert_not_called()
    out = capsys.readouterr().out
    # The usage-report recipient (beaubonhomme) is recorded as skipped in the
    # Usage Report Summary footer — scope the assertion there so it is unambiguous.
    report_section = out[out.index("=== Usage Report Summary ===") :]
    assert "eligible=1" in report_section
    assert "skipped=1" in report_section
    assert "dm_sent=0" in report_section
    # The same summary is threaded under the digest
    report_reply = replies[1]
    assert report_reply.kwargs["thread_ts"] == "111.222"
    assert "Usage Report Summary" in report_reply.args[1]


def test_send_usage_report_enabled_sends_report_to_non_underusers(
    usage_report_db, cli_main, monkeypatch, capsys
):
    """send_usage_report=True + --send + wk%4==0 → beaubonhomme gets the report."""
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": False, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _USAGE_REPORT_WEEK, "--send"])
    assert rc == 0
    # beaubonhomme gets the usage report via dm_user
    dm_calls = [call.args[0] for call in slack_inst.dm_user.call_args_list]
    assert "beaubonhomme@mila.quebec" in dm_calls
    # petitbonhomme is an underuser (send_underusage_report=False) → no dm for them
    assert "petitbonhomme@mila.quebec" not in dm_calls
    out = capsys.readouterr().out
    assert "dm_sent=1" in out


# ── Config-knob wiring ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "cfg_entries",
    [
        # notify_db only has mila jobs. With clusters=["raisin"] → no flagged
        # recipients.
        {"clusters": ["raisin"]},
        # petitbonhomme has util=0.10. At threshold=0.10 credited_used=rgu_h →
        # wasted=0 → waste_ratio=0 < min_waste_ratio=0.50 → not flagged.
        {"utilization_ceiling": 0.10},
    ],
)
def test_clusters_config_filters_cli_results(
    cfg_entries, notify_db, cli_main, monkeypatch, capsys
):
    monkeypatch.setattr("sarc.cli.notify.usage._now_utc", lambda: _CLI_TEST_END)

    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "usage"])
    out = capsys.readouterr().out
    assert "2 user(s) flagged" in out

    cfg = {**_NOTIFY_CFG, **cfg_entries}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        cli_main(["notify", "usage"])
    out = capsys.readouterr().out
    assert "0 user(s) flagged" in out


def test_no_dms_flag_suppresses_usage_report_sends(
    usage_report_db, cli_main, monkeypatch, capsys
):
    """--no-dms suppresses usage-report DMs and records no_dms_flag as the skip reason."""
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            ["notify", "usage", "--as-of", _USAGE_REPORT_WEEK, "--send", "--no-dms"]
        )
    assert rc == 0
    # No per-user DMs of any kind — neither under usage report nor usage reports
    slack_inst.dm_user.assert_not_called()
    out = capsys.readouterr().out
    assert "skipped=1" in out  # usage-report recipients recorded as skipped


# ── Debug overrides (--min-waste-ratio, --user-email, etc.) ─────────────────


def test_min_waste_ratio_flag_overrides_config(
    notify_db, cli_main, monkeypatch, capsys
):
    spy = MagicMock(wraps=_real_get_underusers_usage)
    monkeypatch.setattr("sarc.cli.notify.usage.get_underusers_usage", spy)

    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK])
    assert rc == 0
    assert spy.call_args.kwargs["min_waste_ratio"] == 0.50  # config default
    assert "3 user(s) flagged" in capsys.readouterr().out

    # All 3 seeded users have waste_ratio == 0.90 — a 0.95 override excludes them all.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "usage", "--as-of", _CYCLE_WEEK, "--min-waste-ratio", "0.95"]
        )
    assert rc == 0
    assert spy.call_args.kwargs["min_waste_ratio"] == 0.95
    assert "0 user(s) flagged" in capsys.readouterr().out


def test_min_waste_ratio_flag_explicit_zero_is_honored(
    notify_db, cli_main, monkeypatch
):
    """An explicit --min-waste-ratio 0 must reach get_underusers_usage as 0.0, not be
    silently dropped in favor of the config default (0.0 is falsy in Python, so
    a naive `if self.min_waste_ratio:` check would incorrectly ignore it)."""
    spy = MagicMock(wraps=_real_get_underusers_usage)
    monkeypatch.setattr("sarc.cli.notify.usage.get_underusers_usage", spy)

    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "usage", "--as-of", _CYCLE_WEEK, "--min-waste-ratio", "0"]
        )
    assert rc == 0
    assert spy.call_args.kwargs["min_waste_ratio"] == 0.0


def test_min_waste_ratio_override_logs_warning(notify_db, cli_main, caplog):
    """Threshold overrides are debug-only; warn so one left active isn't missed."""
    with (
        gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}),
        caplog.at_level(logging.WARNING, logger="sarc.cli.notify.usage"),
    ):
        rc = cli_main(
            ["notify", "usage", "--as-of", _CYCLE_WEEK, "--min-waste-ratio", "0.95"]
        )
    assert rc == 0
    assert any("min_waste_ratio" in r.message for r in caplog.records)


def test_user_email_flag_logs_warning(notify_db, cli_main, caplog):
    """--user-email restricts a run to one test user; warn so it's not left on by accident."""
    with (
        gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}),
        caplog.at_level(logging.WARNING, logger="sarc.cli.notify.usage"),
    ):
        rc = cli_main(
            [
                "notify",
                "usage",
                "--as-of",
                _CYCLE_WEEK,
                "--user-email",
                "beaubonhomme@mila.quebec",
            ]
        )
    assert rc == 0
    assert any("beaubonhomme@mila.quebec" in r.message for r in caplog.records)


def test_min_waste_rgu_hours_flag_overrides_config(
    notify_db, cli_main, monkeypatch, capsys
):
    spy = MagicMock(wraps=_real_get_underusers_usage)
    monkeypatch.setattr("sarc.cli.notify.usage.get_underusers_usage", spy)

    # Each seeded user wastes 4.8 * 700 * 0.9 = 3024 RGU-h — a 5000 override excludes them all.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "usage", "--as-of", _CYCLE_WEEK, "--min-waste-rgu-hours", "5000"]
        )
    assert rc == 0
    assert spy.call_args.kwargs["min_waste_rgu_hours"] == 5000.0
    assert "0 user(s) flagged" in capsys.readouterr().out


def test_usage_report_min_usage_rgu_hours_flag_overrides_config(
    usage_report_db, cli_main, monkeypatch, capsys
):
    spy = MagicMock(wraps=_real_get_users_usage)
    monkeypatch.setattr("sarc.cli.notify.usage.get_users_usage", spy)

    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "usage", "--as-of", _USAGE_REPORT_WEEK])
    assert rc == 0
    assert spy.call_args.kwargs["min_usage_rgu_hours"] == 0.0
    out = capsys.readouterr().out
    assert "beaubonhomme@mila.quebec" in out[out.find("=== Usage Report Previews") :]

    # beaubonhomme's in-window usage is 4.8 * 100 = 480 RGU-h — a 1000 override
    # drops them below the floor, so the section disappears entirely.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            [
                "notify",
                "usage",
                "--as-of",
                _USAGE_REPORT_WEEK,
                "--usage-report-min-usage-rgu-hours",
                "1000",
            ]
        )
    assert rc == 0
    assert spy.call_args.kwargs["min_usage_rgu_hours"] == 1000.0
    assert "=== Usage Report Previews" not in capsys.readouterr().out


def test_usage_report_not_email_filtered_when_no_underusers(
    usage_report_db, cli_main, monkeypatch, capsys
):
    """Regression: with no --user-email and no underusers this cycle, the
    usage-report query must receive user_emails=None (no filter). Passing an
    empty list instead makes _select_jobs_usage filter `email IN ()`, matching
    nobody — silently emptying the fleet-wide usage report."""
    spy = MagicMock(wraps=_real_get_users_usage)
    monkeypatch.setattr("sarc.cli.notify.usage.get_users_usage", spy)

    # An impossible waste-ratio floor means nobody qualifies as an underuser
    # this cycle, while the usage report is still eligible.
    cfg = {**_NOTIFY_CFG, "min_waste_ratio": 2.0}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _USAGE_REPORT_WEEK])
    assert rc == 0
    assert spy.call_args.kwargs["user_emails"] is None
    out = capsys.readouterr().out
    assert "beaubonhomme@mila.quebec" in out[out.find("=== Usage Report Previews") :]


def test_personalized_action_min_waste_rgu_hours_flag_overrides_config(
    notify_db, cli_main, monkeypatch
):
    spy = MagicMock(wraps=_real_get_recurring_underusers)
    monkeypatch.setattr("sarc.cli.notify.usage.get_recurring_underusers", spy)

    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            [
                "notify",
                "usage",
                "--as-of",
                _CYCLE_WEEK,
                "--personalized-action-min-waste-rgu-hours",
                "42.0",
            ]
        )
    assert rc == 0
    assert spy.call_args.kwargs["personalized_action_min_waste_rgu_hours"] == 42.0


def test_user_email_filters_dm_to_single_user(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": False}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            [
                "notify",
                "usage",
                "--as-of",
                _CYCLE_WEEK,
                "--send",
                "--user-email",
                "beaubonhomme@mila.quebec",
            ]
        )
    assert rc == 0
    # All 3 seeded users would normally be DMed at this cycle week (see
    # test_send_cycle_week_posts_digest_and_dms) — --user-email restricts to one.
    assert slack_inst.dm_user.call_count == 1
    assert slack_inst.dm_user.call_args.args[0] == "beaubonhomme@mila.quebec"


def test_user_email_skips_admin_channel_posts(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            [
                "notify",
                "usage",
                "--as-of",
                _CYCLE_WEEK,
                "--send",
                "--user-email",
                "beaubonhomme@mila.quebec",
            ]
        )
    assert rc == 0
    digests, replies = _channel_posts(slack_inst)
    assert digests == []
    assert replies == []


def test_user_email_auto_zeroes_thresholds(usage_report_db, cli_main, capsys):
    """beaubonhomme (waste_ratio=0.10, wasted=48 < 672 floor) is NOT an underuser
    by default at week 26 — see test_non_usage_report_week_no_report_section.
    Forcing --user-email auto-zeroes the floors, so they qualify instead."""
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            [
                "notify",
                "usage",
                "--as-of",
                _CYCLE_NON_REPORT_WEEK,
                "--user-email",
                "beaubonhomme@mila.quebec",
            ]
        )
    assert rc == 0
    out = capsys.readouterr().out
    assert "=== Under Usage Report Previews" in out
    dm_section = out[out.find("=== Under Usage Report Previews") :]
    assert "beaubonhomme@mila.quebec" in dm_section
    # petitbonhomme is a real underuser at week 26 too, but the query-level
    # user_emails filter restricts the DM section to just the forced target.
    assert "petitbonhomme@mila.quebec" not in dm_section


def test_user_email_explicit_override_wins_over_auto_zero(
    usage_report_db, cli_main, capsys
):
    # 2024-07-08 is ISO week 28 — a multiple of both usage_cycle_length_weeks(2)
    # and the usage-report period(4), and its 2-week underusage window
    # [2024-06-24, 2024-07-08] contains beaubonhomme's 2024-06-29 job (unlike
    # _USAGE_REPORT_WEEK's window, which starts after that job).
    as_of = "2024-07-08"
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            [
                "notify",
                "usage",
                "--as-of",
                as_of,
                "--user-email",
                "beaubonhomme@mila.quebec",
                "--min-waste-ratio",
                "0.99",
            ]
        )
    assert rc == 0
    out = capsys.readouterr().out
    # --user-email skips admin-digest computation entirely, so there's no
    # "N user(s) flagged" text to check here — the override is instead
    # verified by beaubonhomme landing in the Usage Report below, not the
    # Under Usage Report (which would happen if the auto-zero had won).
    assert "=== Under Usage Report Previews" not in out
    # usage_report_min_usage_rgu_hours is still auto-zeroed (no explicit flag for
    # it), and beaubonhomme is no longer excluded as an underuser, so they now
    # land in the Usage Report instead.
    assert "=== Usage Report Previews" in out
    assert "beaubonhomme@mila.quebec" in out[out.find("=== Usage Report Previews") :]


def test_user_email_skips_recurring_computation(notify_db, cli_main, monkeypatch):
    """--user-email skips the fleet-wide admin-digest/recurring-underusers
    computation entirely (not just the later channel post) — so
    get_recurring_underusers, and by extension the
    personalized_action_min_waste_rgu_hours auto-zero-exemption it would
    otherwise receive, is never reached in this mode. The exemption itself
    is still covered by
    test_personalized_action_min_waste_rgu_hours_flag_overrides_config in
    the normal (non-single-user) path, where get_recurring_underusers does run.
    """
    spy = MagicMock(wraps=_real_get_recurring_underusers)
    monkeypatch.setattr("sarc.cli.notify.usage.get_recurring_underusers", spy)

    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            [
                "notify",
                "usage",
                "--as-of",
                _CYCLE_WEEK,
                "--user-email",
                "beaubonhomme@mila.quebec",
            ]
        )
    assert rc == 0
    spy.assert_not_called()


# ── non-default usage_cycle_length_weeks (N) ──────────────────────────────────
# Smoke tests proving the CLI pipeline reads the config value end-to-end,
# rather than a hardcoded 2. Each test asserts something that would differ
# under a hardcoded-2 regression.


def test_n1_underusage_window_and_eligibility(notify_db, cli_main, capsys):
    # N=1: window is 1 week back from _CYCLE_WEEK (2024-06-16) → 2024-06-09,
    # not the default-2 start of 2024-06-02. Job at 2024-06-10 is inside
    # [2024-06-09, 2024-06-16].
    cfg = {**_NOTIFY_CFG, "usage_cycle_length_weeks": 1}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK])
    assert rc == 0
    captured = capsys.readouterr()
    assert "multiple of 1" in captured.err
    out = captured.out
    assert "2024-06-09" in out
    assert "2024-06-02" not in out
    assert "petitbonhomme@mila.quebec" in out
    assert "=== Under Usage Report Previews" in out


def test_n1_usage_report_cadence(usage_report_db, cli_main, capsys):
    # N=1: usage-report period = usage_report_cycles(2) * 1 = 2 weeks.
    # ISO week 26 (2024-06-30) is a multiple of 2 → report-eligible; under a
    # hardcoded-2 regression the period would be 4 weeks and week 26 (26 % 4
    # != 0) would NOT be eligible, so the report section would be absent.
    cfg = {**_NOTIFY_CFG, "usage_cycle_length_weeks": 1, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", "2024-06-30"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "ISO week 26 (multiple of 2) — Usage report eligible" in captured.err
    out = captured.out
    assert "=== Usage Report Previews" in out
    assert "beaubonhomme@mila.quebec" in out


def test_n3_off_cycle_week_suppresses_dms(notify_db, cli_main, capsys):
    # Week 26 (2024-06-30) is aligned at the default N=2 but off-cycle at N=3
    # (26 % 3 == 2). Under a hardcoded-2 regression this would incorrectly
    # show DM previews.
    cfg = {**_NOTIFY_CFG, "usage_cycle_length_weeks": 3}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", "2024-06-30"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "Underusage report eligible this run" not in captured.err
    assert "=== Under Usage Report Previews" not in captured.out


def test_n3_aligned_week_shows_dms_with_window(notify_db, cli_main, capsys):
    # Week 24 (_CYCLE_WEEK) is aligned for N=3 (24 % 3 == 0). Window is 3
    # weeks back → 2024-05-26, not the default-2 start of 2024-06-02. Job at
    # 2024-06-10 is inside [2024-05-26, 2024-06-16].
    cfg = {**_NOTIFY_CFG, "usage_cycle_length_weeks": 3}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK])
    assert rc == 0
    captured = capsys.readouterr()
    assert "multiple of 3" in captured.err
    out = captured.out
    assert "2024-05-26" in out
    assert "2024-06-02" not in out
    assert "petitbonhomme@mila.quebec" in out
    assert "=== Under Usage Report Previews" in out


def test_n3_usage_report_cadence_flip(usage_report_db, cli_main, capsys):
    # N=3: usage-report period = usage_report_cycles(2) * 3 = 6 weeks. Week 28
    # (_USAGE_REPORT_WEEK) is a report week at the default N=2 (28 % 4 == 0)
    # but NOT at N=3 (28 % 6 == 4) — the strongest plumbing discriminator.
    cfg = {**_NOTIFY_CFG, "usage_cycle_length_weeks": 3, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _USAGE_REPORT_WEEK])
    assert rc == 0
    captured = capsys.readouterr()
    assert "Usage report eligible this run" not in captured.err
    assert "=== Usage Report Previews" not in captured.out


def test_n3_usage_report_positive_case(usage_report_db, cli_main, capsys):
    # N=3: report period = 6 weeks. Week 24 (_CYCLE_WEEK) IS report-aligned
    # (24 % 6 == 0); window = [2024-05-05, 2024-06-16], which contains
    # beaubonhomme's May job (see usage_report_db) but none of petitbonhomme's
    # (underuser, excluded from the usage report regardless).
    cfg = {**_NOTIFY_CFG, "usage_cycle_length_weeks": 3, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "usage", "--as-of", _CYCLE_WEEK])
    assert rc == 0
    captured = capsys.readouterr()
    assert "ISO week 24 (multiple of 6) — Usage report eligible" in captured.err
    out = captured.out
    assert "=== Usage Report Previews" in out
    assert "beaubonhomme@mila.quebec" in out
