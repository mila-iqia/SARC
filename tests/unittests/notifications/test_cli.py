"""Tests for `sarc notify underusage` CLI command (T4)."""

import copy
from datetime import UTC, datetime, timedelta

import gifnoc
import pytest
from sqlmodel import select

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
    "window_days": 30,
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
        rc = cli_main(["notify", "underusage", "--window-days", "30"])
    assert rc == 0


def test_dry_run_prints_dry_run_header(notify_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-days", "30"])
    assert "DRY RUN" in capsys.readouterr().out


# ── recipients ────────────────────────────────────────────────────────────────


def test_dry_run_prints_flagged_recipient(notify_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-days", "30"])
    assert "petitbonhomme@mila.quebec" in capsys.readouterr().out


# ── digest ────────────────────────────────────────────────────────────────────


def test_dry_run_prints_admin_digest(notify_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-days", "30"])
    out = capsys.readouterr().out
    assert "Admin Digest" in out
    assert "Weekly GPU Underusage Digest" in out


# ── DM preview ────────────────────────────────────────────────────────────────


def test_dry_run_prints_dm_previews_section(notify_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-days", "30"])
    assert "DM Previews" in capsys.readouterr().out


def test_dry_run_dm_preview_contains_greeting(notify_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-days", "30"])
    # display_name = "M/Ms Petitbonhomme"; _first_name() returns the first token
    assert "Hi M/Ms," in capsys.readouterr().out


# ── T5: bi-weekly cadence gating ──────────────────────────────────────────────


def test_even_week_shows_dm_previews(notify_db, cli_main, capsys):
    # 2024-06-30 is ISO week 26 (even); job at 2024-06-10 is inside [2024-05-31, 2024-06-30]
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-days", "30", "--as-of", "2024-06-30"]
        )
    assert rc == 0
    out = capsys.readouterr().out
    assert "DM Previews" in out
    assert "even" in out


def test_odd_week_suppresses_dm_previews(notify_db, cli_main, capsys):
    # 2024-06-23 is ISO week 25 (odd); job at 2024-06-10 is inside [2024-05-24, 2024-06-23]
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-days", "30", "--as-of", "2024-06-23"]
        )
    assert rc == 0
    out = capsys.readouterr().out
    assert "DM Previews" not in out
    assert "digest-only" in out


def test_week_parity_derived_from_run_date(notify_db, cli_main, monkeypatch, capsys):
    # _CLI_TEST_END is ISO week 26 (even) — should show DMs without --as-of
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _CLI_TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-days", "30"])
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
    # 2025-01-05 is ISO week 1 (odd); window [2024-12-06, 2025-01-05] spans the year
    # boundary and covers the Dec-20 job.  The old --week-number 1 arithmetic would
    # have shifted end to the wrong quarter (week 1 from ~week 24 = 161 days back).
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-days", "30", "--as-of", "2025-01-05"]
        )
    assert rc == 0
    out = capsys.readouterr().out
    assert "2024-12-06" in out              # window start in digest header
    assert "2025-01-05" in out              # window end in digest header
    assert "petitbonhomme@mila.quebec" in out  # job is inside the window
    assert "digest-only" in out             # week 1 is odd → no DMs


# ── future anchor guard ───────────────────────────────────────────────────────


def test_future_anchor_prints_note_and_does_not_crash(notify_db, cli_main, capsys):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-days", "30", "--as-of", "2099-01-01"]
        )
    assert rc == 0
    assert "future" in capsys.readouterr().out


# ── invalid --as-of ───────────────────────────────────────────────────────────


def test_invalid_as_of_returns_error(notify_db, cli_main, caplog):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(
            ["notify", "underusage", "--window-days", "30", "--as-of", "not-a-date"]
        )
    assert rc == -1
    assert any("not-a-date" in r.message for r in caplog.records)


def test_bare_date_as_of_interpreted_as_utc_midnight(notify_db, cli_main, capsys):
    # A bare YYYY-MM-DD must be treated as 00:00 UTC, not local midnight.
    # The period string in the digest header is derived from start.date() and end.date(),
    # so if end is midnight UTC on 2024-06-23 the window is [2024-05-24, 2024-06-23].
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(
            ["notify", "underusage", "--window-days", "30", "--as-of", "2024-06-23"]
        )
    out = capsys.readouterr().out
    assert "2024-06-23" in out
    assert "2024-05-24" in out


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
                "--window-days",
                "30",
                "--min-ratio",
                "0.50",
                "--min-rgu-hours",
                "672.0",
            ]
        )
    assert rc == 0
    assert "petitbonhomme@mila.quebec" in capsys.readouterr().out
