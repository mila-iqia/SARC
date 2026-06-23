"""Tests for `sarc notify underusage` CLI command."""

import copy
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import gifnoc
import pytest
from sqlmodel import select

from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.users import UserDB
from sarc.notifications.slack import SendResult, SendStatus
from tests.db.factory import base_job

_MILA_GPU_TYPE = "A100-SXM4-80GB"

_CLI_TEST_END = datetime(2024, 6, 24, tzinfo=UTC)  # week 26 (even)

_NOTIFY_JOB_END = datetime(2024, 6, 10, tzinfo=UTC)

_NOTIFY_CFG = {
    "slack": {
        "description": "test channel",
        "token": "xoxb-test-token",
        "channel": "#test-channel",
    },
    "enabled": True,
    "send_underusage_report": True,
    "min_ratio": 0.50,
    "min_rgu_hours": 672.0,
    "digest_top_n": 16,
    "send_usage_report": True,
    "usage_report_min_rgu_hours": 0,
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
    end_time: datetime,
) -> SlurmJobDB:
    submit_time = end_time - timedelta(hours=elapsed_h)
    job_data = copy.deepcopy(base_job)
    job_data.pop("cluster_name")
    job_data.update(
        {
            "sarc_user_id": user_id,
            "cluster_id": cluster_id,
            "elapsed_time": int(elapsed_h * 3600),
            "submit_time": submit_time,
            "start_time": submit_time + timedelta(seconds=60),
            "end_time": end_time,
            "job_id": job_id,
            "requested_gres_gpu": 1,
            "allocated_gres_gpu": 1,
            "allocated_gpu_type": gpu_type,
            "harmonized_gpu_type": gpu_type,
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
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "underusage"])
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

    # _CLI_TEST_END is ISO week 26 (even) — should show DMs
    # DM preview
    assert "DM Previews" in out
    # display_name = "M/Ms Petitbonhomme"; _first_name() returns the first token
    assert "Hi M/Ms," in out


# ── bi-weekly cadence gating ──────────────────────────────────────────────

_EVEN_WEEK = "2024-06-16"  # ISO week 24
_ODD_WEEK = "2024-06-23"  # ISO week 25


def test_even_week_shows_dm_previews(notify_db, cli_main, capsys):
    # 2024-06-16 is ISO week 24 (even); job at 2024-06-10 is inside [2024-05-31, 2024-06-16]
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "underusage", "--as-of", _EVEN_WEEK])
    assert rc == 0
    captured = capsys.readouterr()
    assert "DM Previews" in captured.out
    assert "even" in captured.err


def test_odd_week_suppresses_dm_previews(notify_db, cli_main, capsys):
    # 2024-06-23 is ISO week 25 (odd); job at 2024-06-10 is inside [2024-06-09, 2024-06-23]
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "underusage", "--as-of", _ODD_WEEK])
    assert rc == 0
    err = capsys.readouterr().err
    assert "DM Previews" not in err
    assert "digest-only" in err


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
    # 2025-01-05 is ISO week 1 (odd); window [2024-12-22, 2025-01-05] spans the
    # year boundary and covers the Dec-27 job.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "underusage", "--as-of", "2025-01-05"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "2024-12-22" in captured.out  # window start in digest header
    assert "2025-01-05" in captured.out  # window end in digest header
    assert "petitbonhomme@mila.quebec" in captured.out  # job is inside the window
    assert "digest-only" in captured.err  # week 1 is odd → no DMs


# ── future anchor guard ───────────────────────────────────────────────────────


def test_future_anchor_prints_note_and_does_not_crash(notify_db, cli_main, capsys):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "underusage", "--as-of", "2099-01-01"])
    assert rc == 0
    assert "future" in capsys.readouterr().err


# ── invalid --as-of ───────────────────────────────────────────────────────────


def test_invalid_as_of_returns_error(notify_db, cli_main, caplog):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "underusage", "--as-of", "not-a-date"])
    assert rc == -1
    assert any("not-a-date" in r.message for r in caplog.records)


# ── window-end clipped to midnight ───────────────────────────────────────


def test_now_clipped_to_midnight(notify_db, cli_main, monkeypatch, capsys):
    # _now_utc returns 15:30 UTC; the window end in the digest header must still
    # show the date only (derived from end.date()), and the period start must be
    # exactly 4 weeks before midnight on that date.
    monkeypatch.setattr(
        "sarc.cli.notify.underusage._now_utc",
        lambda: _NOTIFY_JOB_END + timedelta(hours=15, minutes=30),
    )
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["notify", "underusage"])
    assert rc == 0
    out = capsys.readouterr().out
    # window: [2024-05-27, 2024-06-10]  (midnight-to-midnight, 4 weeks)
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
        rc = cli_main(["notify", "underusage", "--as-of", _EVEN_WEEK, "--send"])
    assert rc == 0
    slack_cls.assert_not_called()


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
            ["notify", "underusage", "--min-ratio", "0.50", "--min-rgu-hours", "672.0"]
        )
    assert rc == 0
    assert "petitbonhomme@mila.quebec" in capsys.readouterr().out


# ── --send flag wires actual sending ──────────────────────────────────────


def _mock_slack(dm_status=SendStatus.OK, channel_status=SendStatus.OK):
    inst = MagicMock()
    inst.dm_user.return_value = SendResult(dm_status)
    inst.post_channel_file.return_value = SendResult(channel_status)
    cls = MagicMock(return_value=inst)
    return cls, inst


def _patch_senders(monkeypatch, slack_cls):
    monkeypatch.setattr("sarc.cli.notify.underusage.SlackClient", slack_cls)


def test_dry_run_does_not_instantiate_slack_or_email(notify_db, cli_main, monkeypatch):
    slack_cls = MagicMock()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": False}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "underusage", "--as-of", _EVEN_WEEK])
    assert rc == 0
    slack_cls.assert_not_called()


def test_send_even_week_posts_digest_and_dms(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": False}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "underusage", "--as-of", _EVEN_WEEK, "--send"])
    assert rc == 0
    slack_inst.post_channel_file.assert_called_once()
    assert slack_inst.dm_user.call_count == 3


def test_send_odd_week_posts_digest_only(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "underusage", "--as-of", _ODD_WEEK, "--send"])
    assert rc == 0
    slack_inst.post_channel_file.assert_called_once()
    slack_inst.dm_user.assert_not_called()


def test_send_no_dms_flag_skips_dms(notify_db, cli_main, monkeypatch):
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(
            ["notify", "underusage", "--as-of", _EVEN_WEEK, "--send", "--no-dms"]
        )
    assert rc == 0
    slack_inst.post_channel_file.assert_called_once()
    slack_inst.dm_user.assert_not_called()


def test_send_underusage_report_false_suppresses_underusage_report(
    notify_db, cli_main, monkeypatch
):
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": False, "send_usage_report": False}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "underusage", "--as-of", _EVEN_WEEK, "--send"])
    assert rc == 0
    slack_inst.post_channel_file.assert_called_once()
    slack_inst.dm_user.assert_not_called()


def test_send_dm_failure_surfaced_in_footer(notify_db, cli_main, monkeypatch, capsys):
    slack_cls, slack_inst = _mock_slack()
    slack_inst.dm_user.return_value = SendResult(SendStatus.FAILED, "channel_not_found")
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": False}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "underusage", "--as-of", _EVEN_WEEK, "--send"])
    assert rc == 0  # failures reported, run does not crash
    out = capsys.readouterr().out
    assert "failed=3" in out


# ── usage report ──────────────────────────────────────────────────────────────
# ISO week 28 (2024-07-14) → week_num=28, 28 % 4 == 0 → usage report eligible.
# ISO week 26 (2024-06-30) → week_num=26, 26 % 4 == 2 → not eligible.
_USAGE_REPORT_WEEK = "2024-07-14"  # wk 28, multiple of 4
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
    session.commit()
    yield session


def test_usage_report_week_underuser_not_in_report_previews(
    usage_report_db, cli_main, monkeypatch, capsys
):
    """petitbonhomme is an underuser — they get the DM alert, not the usage report."""
    slack_cls = MagicMock()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "underusage", "--as-of", _USAGE_REPORT_WEEK])
    assert rc == 0
    out = capsys.readouterr().out
    assert "DM Previews" in out
    assert "Usage Report Previews" in out
    # petitbonhomme should appear in DM previews (alert), not usage report previews
    dm_section = out[
        out.find("=== DM Previews ===") : out.find("=== Usage Report Previews")
    ]
    assert "petitbonhomme@mila.quebec" in dm_section
    usage_section = out[out.find("=== Usage Report Previews") :]
    assert "petitbonhomme@mila.quebec" not in usage_section
    assert "beaubonhomme@mila.quebec" in usage_section

    # Dry-run never instantiates senders
    slack_cls.assert_not_called()


def test_non_usage_report_week_no_report_section(
    usage_report_db, cli_main, monkeypatch, capsys
):
    """Even week but not a multiple of 4 → no usage report section."""
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": True, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        cli_main(["notify", "underusage", "--as-of", _EVEN_NON_REPORT_WEEK, "--send"])
    out = capsys.readouterr().out
    assert "DM Previews" in out
    assert "Usage Report Previews" not in out

    # Only the digest channel post; no usage report DMs
    slack_inst.post_channel_file.assert_called_once()
    # beaubonhomme's job on 2024-07-01 is outside [2024-06-16, 2024-06-30] — no reports
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
        rc = cli_main(["notify", "underusage", "--as-of", _USAGE_REPORT_WEEK, "--send"])
    assert rc == 0
    # Only the admin digest channel post; no DMs for underusers or report recipients
    slack_inst.post_channel_file.assert_called_once()
    # dm_user may be called 0 times (send_underusage_report=False, send_usage_report=False)
    slack_inst.dm_user.assert_not_called()
    out = capsys.readouterr().out
    assert "skipped=1" in out


def test_send_usage_report_enabled_sends_report_to_non_underusers(
    usage_report_db, cli_main, monkeypatch, capsys
):
    """send_usage_report=True + --send + wk%4==0 → beaubonhomme gets the report."""
    slack_cls, slack_inst = _mock_slack()
    _patch_senders(monkeypatch, slack_cls)
    cfg = {**_NOTIFY_CFG, "send_underusage_report": False, "send_usage_report": True}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        rc = cli_main(["notify", "underusage", "--as-of", _USAGE_REPORT_WEEK, "--send"])
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
        # wasted=0 → waste_ratio=0 < min_ratio=0.50 → not flagged.
        {"utilization_ceiling": 0.10},
    ],
)
def test_clusters_config_filters_cli_results(
    cfg_entries, notify_db, cli_main, monkeypatch, capsys
):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)

    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage"])
    out = capsys.readouterr().out
    assert "2 user(s) flagged" in out

    cfg = {**_NOTIFY_CFG, **cfg_entries}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        cli_main(["notify", "underusage"])
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
            [
                "notify",
                "underusage",
                "--as-of",
                _USAGE_REPORT_WEEK,
                "--send",
                "--no-dms",
            ]
        )
    assert rc == 0
    # No per-user DMs of any kind — neither underusage alerts nor usage reports
    slack_inst.dm_user.assert_not_called()
    out = capsys.readouterr().out
    assert "skipped=1" in out  # usage-report recipients recorded as skipped
