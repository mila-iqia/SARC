"""Tests for `sarc notify underusage` CLI command (T4 + T8)."""

import copy
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import gifnoc
import pytest
from sqlmodel import select

from sarc.notifications.slack import SendResult, SendStatus

from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.users import UserDB
from tests.db.factory import base_job

_MILA_GPU_TYPE = "A100-SXM4-80GB"

# "Today" for all CLI tests.  Window of 30 days covers jobs seeded on 2024-06-10.
_CLI_TEST_END = datetime(2024, 6, 30, tzinfo=UTC)

_NOTIFY_CFG = {
    "slack": {
        "description": "test channel",
        "token": "xoxb-test-token",
        "channel": "#test-channel",
    },
    "min_ratio": 0.50,
    "min_rgu_hours": 672.0,
    "window_weeks": 4,
    "digest_top_n": 16,
}


def _add_gpu_job(
    session,
    *,
    user_id: int,
    cluster_id: int,
    elapsed_h: float,
    gpu_type: str,
    utilization: float | None,
    job_id: int,
    submit_time: datetime,
) -> SlurmJobDB:
    job_data = copy.deepcopy(base_job)
    job_data.pop("cluster_name")
    job_data.update(
        {
            "sarc_user_id": user_id,
            "cluster_id": cluster_id,
            "elapsed_time": int(elapsed_h * 3600),
            "submit_time": submit_time,
            "start_time": submit_time + timedelta(seconds=60),
            "end_time": submit_time + timedelta(hours=elapsed_h),
            "job_id": job_id,
            "requested_gres_gpu": 1,
            "allocated_gres_gpu": 1,
            "allocated_gpu_type": gpu_type,
            "job_state": "COMPLETED",
        }
    )
    job = SlurmJobDB(**job_data)
    session.add(job)
    session.flush()
    if utilization is not None:
        session.add(
            JobStatisticDB(
                job_id=job.id,
                name="gpu_utilization",
                mean=utilization,
                std=None,
                q05=None,
                q25=None,
                median=None,
                q75=None,
                max=None,
                unused=None,
            )
        )
    return job


@pytest.fixture
def notify_db(read_write_db):
    """Seed one high-wasting user (petitbonhomme) into the test DB."""
    session = read_write_db
    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}
    mila_id = clusters["mila"].id
    petitbonhomme_id = users["petitbonhomme"].id

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
        submit_time=datetime(2024, 6, 10, tzinfo=UTC),
    )
    session.commit()
    yield session


# ── basic exit code + dry-run marker ─────────────────────────────────────────


def test_dry_run_exits_zero(notify_db, cli_main, monkeypatch):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "underusage", "--window-weeks", "4"])
    assert rc == 0


def test_dry_run_prints_dry_run_header(notify_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-weeks", "4"])
    assert "DRY RUN" in capsys.readouterr().out


# ── recipients ────────────────────────────────────────────────────────────────


def test_dry_run_prints_flagged_recipient(notify_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-weeks", "4"])
    assert "petitbonhomme@mila.quebec" in capsys.readouterr().out


# ── digest ────────────────────────────────────────────────────────────────────


def test_dry_run_prints_admin_digest(notify_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-weeks", "4"])
    out = capsys.readouterr().out
    assert "Admin Digest" in out
    assert "Weekly GPU Underusage Digest" in out


# ── DM preview ────────────────────────────────────────────────────────────────


def test_dry_run_prints_dm_previews_section(notify_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-weeks", "4"])
    assert "DM Previews" in capsys.readouterr().out


def test_dry_run_dm_preview_contains_greeting(notify_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-weeks", "4"])
    # display_name = "M/Ms Petitbonhomme"; _first_name() returns the first token
    assert "Hi M/Ms," in capsys.readouterr().out


# ── T5: bi-weekly cadence gating ──────────────────────────────────────────────


def test_even_week_shows_dm_previews(notify_db, cli_main, capsys):
    # 2024-06-30 is ISO week 26 (even); job at 2024-06-10 is inside [2024-05-31, 2024-06-30]
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "4", "--as-of", "2024-06-30"]
        )
    assert rc == 0
    out = capsys.readouterr().out
    assert "DM Previews" in out
    assert "even" in out


def test_odd_week_suppresses_dm_previews(notify_db, cli_main, capsys):
    # 2024-06-23 is ISO week 25 (odd); job at 2024-06-10 is inside [2024-05-24, 2024-06-23]
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "4", "--as-of", "2024-06-23"]
        )
    assert rc == 0
    out = capsys.readouterr().out
    assert "DM Previews" not in out
    assert "digest-only" in out


def test_week_parity_derived_from_run_date(notify_db, cli_main, monkeypatch, capsys):
    # _CLI_TEST_END is ISO week 26 (even) — should show DMs without --as-of
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-weeks", "4"])
    assert "DM Previews" in capsys.readouterr().out


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
        job_id=70002,
        submit_time=datetime(2024, 12, 20, tzinfo=UTC),
    )
    session.commit()
    yield session


def test_year_boundary_window_is_correct(year_boundary_db, cli_main, capsys):
    # 2025-01-05 is ISO week 1 (odd); window [2024-12-08, 2025-01-05] spans the year
    # boundary and covers the Dec-20 job.  The old --week-number 1 arithmetic would
    # have shifted end to the wrong quarter (week 1 from ~week 24 = 161 days back).
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "4", "--as-of", "2025-01-05"]
        )
    assert rc == 0
    out = capsys.readouterr().out
    assert "2024-12-08" in out              # window start in digest header
    assert "2025-01-05" in out              # window end in digest header
    assert "petitbonhomme@mila.quebec" in out  # job is inside the window
    assert "digest-only" in out             # week 1 is odd → no DMs


# ── future anchor guard ───────────────────────────────────────────────────────


def test_future_anchor_prints_note_and_does_not_crash(notify_db, cli_main, capsys):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "4", "--as-of", "2099-01-01"]
        )
    assert rc == 0
    assert "future" in capsys.readouterr().out


# ── invalid --as-of ───────────────────────────────────────────────────────────


def test_invalid_as_of_returns_error(notify_db, cli_main, caplog):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "4", "--as-of", "not-a-date"]
        )
    assert rc == -1
    assert any("not-a-date" in r.message for r in caplog.records)


def test_bare_date_as_of_interpreted_as_utc_midnight(notify_db, cli_main, capsys):
    # A bare YYYY-MM-DD must be treated as 00:00 UTC, not local midnight.
    # The period string in the digest header is derived from start.date() and end.date(),
    # so if end is midnight UTC on 2024-06-23 the window is [2024-05-26, 2024-06-23].
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(
            ["notify", "underusage", "--window-weeks", "4", "--as-of", "2024-06-23"]
        )
    out = capsys.readouterr().out
    assert "2024-06-23" in out
    assert "2024-05-26" in out


# ── T3: window-end clipped to midnight ───────────────────────────────────────


def test_now_clipped_to_midnight(notify_db, cli_main, monkeypatch, capsys):
    # _now_utc returns 15:30 UTC; the window end in the digest header must still
    # show the date only (derived from end.date()), and the period start must be
    # exactly 4 weeks before midnight on that date.
    non_midnight = datetime(2024, 6, 30, 15, 30, 0, tzinfo=UTC)
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: non_midnight)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "underusage", "--window-weeks", "4"])
    assert rc == 0
    out = capsys.readouterr().out
    # window: [2024-06-02, 2024-06-30]  (midnight-to-midnight, 4 weeks)
    assert "2024-06-30" in out
    assert "2024-06-02" in out


# ── T1: enabled kill-switch ───────────────────────────────────────────────────


def test_enabled_false_returns_zero_without_sending(cli_main, monkeypatch):
    slack_cls = MagicMock()
    email_cls = MagicMock()
    _patch_senders(monkeypatch, slack_cls, email_cls)
    cfg = {**_NOTIFY_CFG, "enabled": False, "send_dms": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "underusage", "--window-weeks", "4", "--as-of", _EVEN_WEEK, "--send"])
    assert rc == 0
    slack_cls.assert_not_called()
    email_cls.assert_not_called()


# ── missing config ────────────────────────────────────────────────────────────


def test_missing_notifications_config_returns_error(cli_main, caplog):
    # base_config (autouse) does not set sarc.notifications → must return -1
    rc = cli_main(["notify", "underusage"])
    assert rc == -1


# ── CLI flags override config ─────────────────────────────────────────────────


def test_cli_flags_override_config_thresholds(notify_db, cli_main, monkeypatch, capsys):
    """Config has impossibly strict thresholds; CLI flags relax them back."""
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    strict_cfg = {**_NOTIFY_CFG, "min_ratio": 0.99, "min_rgu_hours": 999_999.0}
    with gifnoc.overlay({"sarc.notifications": strict_cfg}):
        rc = cli_main(
            [
                "notify",
                "underusage",
                "--window-weeks",
                "4",
                "--min-ratio",
                "0.50",
                "--min-rgu-hours",
                "672.0",
            ]
        )
    assert rc == 0
    assert "petitbonhomme@mila.quebec" in capsys.readouterr().out


# ── T8: --send flag wires actual sending ──────────────────────────────────────

_EVEN_WEEK = "2024-06-30"  # ISO week 26
_ODD_WEEK = "2024-06-23"   # ISO week 25


def _mock_slack(dm_status=SendStatus.OK, channel_status=SendStatus.OK):
    inst = MagicMock()
    inst.dm_user.return_value = SendResult(dm_status)
    inst.post_channel_file.return_value = SendResult(channel_status)
    cls = MagicMock(return_value=inst)
    return cls, inst


def _mock_email(status=SendStatus.OK):
    inst = MagicMock()
    inst.send_plaintext.return_value = SendResult(status)
    cls = MagicMock(return_value=inst)
    return cls, inst


def _patch_senders(monkeypatch, slack_cls, email_cls):
    monkeypatch.setattr("sarc.cli.notify.underusage.SlackClient", slack_cls)
    monkeypatch.setattr("sarc.cli.notify.underusage.EmailClient", email_cls)


def test_dry_run_does_not_instantiate_slack_or_email(notify_db, cli_main, monkeypatch):
    slack_cls = MagicMock()
    email_cls = MagicMock()
    _patch_senders(monkeypatch, slack_cls, email_cls)
    cfg = {**_NOTIFY_CFG, "send_dms": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "4", "--as-of", _EVEN_WEEK]
        )
    assert rc == 0
    slack_cls.assert_not_called()
    email_cls.assert_not_called()


def test_send_even_week_posts_digest_and_dms(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack()
    email_cls, email_inst = _mock_email()
    _patch_senders(monkeypatch, slack_cls, email_cls)
    cfg = {**_NOTIFY_CFG, "send_dms": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "4", "--as-of", _EVEN_WEEK, "--send"]
        )
    assert rc == 0
    slack_inst.post_channel_file.assert_called_once()
    slack_inst.dm_user.assert_called_once()
    email_inst.send_plaintext.assert_not_called()


def test_send_odd_week_posts_digest_only(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack()
    email_cls, email_inst = _mock_email()
    _patch_senders(monkeypatch, slack_cls, email_cls)
    cfg = {**_NOTIFY_CFG, "send_dms": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "4", "--as-of", _ODD_WEEK, "--send"]
        )
    assert rc == 0
    slack_inst.post_channel_file.assert_called_once()
    slack_inst.dm_user.assert_not_called()


def test_send_no_dms_flag_skips_dms(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack()
    email_cls, email_inst = _mock_email()
    _patch_senders(monkeypatch, slack_cls, email_cls)
    cfg = {**_NOTIFY_CFG, "send_dms": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            [
                "notify", "underusage", "--window-weeks", "4",
                "--as-of", _EVEN_WEEK, "--send", "--no-dms",
            ]
        )
    assert rc == 0
    slack_inst.post_channel_file.assert_called_once()
    slack_inst.dm_user.assert_not_called()


def test_send_dms_false_suppresses_dms(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack()
    email_cls, email_inst = _mock_email()
    _patch_senders(monkeypatch, slack_cls, email_cls)
    # send_dms defaults to False in _NOTIFY_CFG
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "4", "--as-of", _EVEN_WEEK, "--send"]
        )
    assert rc == 0
    slack_inst.post_channel_file.assert_called_once()
    slack_inst.dm_user.assert_not_called()


def test_send_slack_not_found_uses_email_fallback(notify_db, cli_main, monkeypatch, capsys):
    slack_cls, slack_inst = _mock_slack(dm_status=SendStatus.USER_NOT_FOUND)
    email_cls, email_inst = _mock_email(status=SendStatus.OK)
    _patch_senders(monkeypatch, slack_cls, email_cls)
    cfg = {**_NOTIFY_CFG, "send_dms": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "4", "--as-of", _EVEN_WEEK, "--send"]
        )
    assert rc == 0
    slack_inst.dm_user.assert_called_once()
    email_inst.send_plaintext.assert_called_once()
    assert "email_sent=1" in capsys.readouterr().out


def test_send_dm_failure_surfaced_in_footer(notify_db, cli_main, monkeypatch, capsys):
    slack_cls, slack_inst = _mock_slack()
    slack_inst.dm_user.return_value = SendResult(SendStatus.FAILED, "channel_not_found")
    email_cls, email_inst = _mock_email()
    _patch_senders(monkeypatch, slack_cls, email_cls)
    cfg = {**_NOTIFY_CFG, "send_dms": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "4", "--as-of", _EVEN_WEEK, "--send"]
        )
    assert rc == 0  # failures reported, run does not crash
    out = capsys.readouterr().out
    assert "failed=1" in out


# ── T12: usage report (Phase 3) ───────────────────────────────────────────────
# ISO week 28 (2024-07-14) → week_num=28, 28 % 4 == 0 → usage report eligible.
# ISO week 26 (2024-06-30) → week_num=26, 26 % 4 == 2 → not eligible.
_USAGE_REPORT_WEEK = "2024-07-14"   # wk 28, multiple of 4
_EVEN_NON_REPORT_WEEK = "2024-06-30"  # wk 26, even but not multiple of 4


@pytest.fixture
def usage_report_db(read_write_db):
    """Two users with GPU jobs inside [2024-06-16, 2024-07-14]:
    - petitbonhomme: high waste → underuser (gets the alert, not the report)
    - beaubonhomme:  low waste  → active user (gets the usage report)
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
        submit_time=datetime(2024, 7, 1, tzinfo=UTC),
    )
    # beaubonhomme: 100 h @ 90 % → waste_ratio=0.10, wasted=48 < 672 floor → NOT underuser
    _add_gpu_job(
        session,
        user_id=users["beaubonhomme"].id,
        cluster_id=mila_id,
        elapsed_h=100,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.90,
        job_id=80002,
        submit_time=datetime(2024, 7, 1, tzinfo=UTC),
    )
    session.commit()
    yield session


def test_usage_report_week_dry_run_prints_previews(usage_report_db, cli_main, capsys):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "2", "--as-of", _USAGE_REPORT_WEEK]
        )
    assert rc == 0
    out = capsys.readouterr().out
    assert "Usage Report Previews" in out
    assert "beaubonhomme@mila.quebec" in out


def test_usage_report_week_underuser_not_in_report_previews(usage_report_db, cli_main, capsys):
    """petitbonhomme is an underuser — they get the DM alert, not the usage report."""
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(
            ["notify", "underusage", "--window-weeks", "2", "--as-of", _USAGE_REPORT_WEEK]
        )
    out = capsys.readouterr().out
    # petitbonhomme should appear in DM previews (alert), not usage report previews
    dm_section = out[out.find("=== DM Previews ==="):out.find("=== Usage Report Previews")]
    assert "petitbonhomme" in dm_section


def test_non_usage_report_week_no_report_section(usage_report_db, cli_main, capsys):
    """Even week but not a multiple of 4 → no usage report section."""
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(
            ["notify", "underusage", "--window-weeks", "2", "--as-of", _EVEN_NON_REPORT_WEEK]
        )
    assert "Usage Report Previews" not in capsys.readouterr().out


def test_dry_run_usage_report_week_no_sends(usage_report_db, cli_main, monkeypatch):
    """Dry-run never instantiates senders, even on a usage-report week."""
    slack_cls = MagicMock()
    email_cls = MagicMock()
    _patch_senders(monkeypatch, slack_cls, email_cls)
    cfg = {**_NOTIFY_CFG, "send_dms": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "2", "--as-of", _USAGE_REPORT_WEEK]
        )
    assert rc == 0
    slack_cls.assert_not_called()
    email_cls.assert_not_called()


def test_send_usage_report_disabled_no_report_sends(usage_report_db, cli_main, monkeypatch, capsys):
    """send_usage_report=False (default) → no usage report DMs even on a report week."""
    slack_cls, slack_inst = _mock_slack()
    email_cls, email_inst = _mock_email()
    _patch_senders(monkeypatch, slack_cls, email_cls)
    # _NOTIFY_CFG has send_dms=False and no send_usage_report key → defaults to False
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "2", "--as-of", _USAGE_REPORT_WEEK, "--send"]
        )
    assert rc == 0
    # Only the admin digest channel post; no DMs for underusers or report recipients
    slack_inst.post_channel_file.assert_called_once()
    # dm_user may be called 0 times (send_dms=False, send_usage_report=False)
    assert slack_inst.dm_user.call_count == 0
    out = capsys.readouterr().out
    assert "send_usage_report_disabled" in out or "skipped=1" in out


def test_send_usage_report_enabled_sends_report_to_non_underusers(
    usage_report_db, cli_main, monkeypatch, capsys
):
    """send_usage_report=True + --send + wk%4==0 → beaubonhomme gets the report."""
    slack_cls, slack_inst = _mock_slack()
    email_cls, email_inst = _mock_email()
    _patch_senders(monkeypatch, slack_cls, email_cls)
    cfg = {**_NOTIFY_CFG, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "2", "--as-of", _USAGE_REPORT_WEEK, "--send"]
        )
    assert rc == 0
    # beaubonhomme gets the usage report via dm_user
    dm_calls = [call.args[0] for call in slack_inst.dm_user.call_args_list]
    assert "beaubonhomme@mila.quebec" in dm_calls
    # petitbonhomme is an underuser (send_dms=False) → no dm for them
    assert "petitbonhomme@mila.quebec" not in dm_calls
    out = capsys.readouterr().out
    assert "dm_sent=1" in out


def test_send_usage_report_non_report_week_no_reports(
    usage_report_db, cli_main, monkeypatch
):
    """Even week but not wk%4==0 → no usage report sends regardless of config."""
    slack_cls, slack_inst = _mock_slack()
    email_cls, _email_inst = _mock_email()
    _patch_senders(monkeypatch, slack_cls, email_cls)
    cfg = {**_NOTIFY_CFG, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            ["notify", "underusage", "--window-weeks", "2", "--as-of", _EVEN_NON_REPORT_WEEK, "--send"]
        )
    assert rc == 0
    # Only the digest channel post; no usage report DMs
    slack_inst.post_channel_file.assert_called_once()
    # beaubonhomme's job on 2024-07-01 is outside [2024-06-16, 2024-06-30] — no reports
    dm_calls = [call.args[0] for call in slack_inst.dm_user.call_args_list]
    assert "beaubonhomme@mila.quebec" not in dm_calls


def test_no_dms_flag_suppresses_usage_report_sends(
    usage_report_db, cli_main, monkeypatch, capsys
):
    """--no-dms suppresses usage-report DMs and records no_dms_flag as the skip reason."""
    slack_cls, slack_inst = _mock_slack()
    email_cls, email_inst = _mock_email()
    _patch_senders(monkeypatch, slack_cls, email_cls)
    cfg = {**_NOTIFY_CFG, "send_dms": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            [
                "notify", "underusage", "--window-weeks", "2",
                "--as-of", _USAGE_REPORT_WEEK, "--send", "--no-dms",
            ]
        )
    assert rc == 0
    # No per-user DMs of any kind — neither underusage alerts nor usage reports
    assert slack_inst.dm_user.call_count == 0
    out = capsys.readouterr().out
    assert "skipped=1" in out  # usage-report recipients recorded as skipped
