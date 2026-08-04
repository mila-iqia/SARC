"""Tests for `sarc usage refresh-store` CLI command."""

from datetime import UTC, datetime, timedelta

import gifnoc
from sqlmodel import select

from sarc.config import UsageNotifyConfig
from sarc.db.cluster import SlurmClusterDB
from sarc.db.user_periods import UserPeriods
from sarc.db.users import UserDB
from sarc.notifications.usage import _restrictive_action_flags, get_recurring_underusers
from tests.unittests.notifications.test_recurring import (
    _MIN_WASTE_RATIO,
    _MIN_WASTE_RGU_HOURS,
    _NOTIFY_CFG,
    _TEST_END,
    recurring_db,  # noqa: F401 -- pytest fixture, reused from test_recurring.py
)

_14D = timedelta(days=14)
_HISTORY_CYCLES = UsageNotifyConfig.history_cycles


def _run(cli_main, monkeypatch, *extra_args, notify_cfg=None):
    monkeypatch.setattr("sarc.cli.usage.notify._now_utc", lambda: _TEST_END)
    with gifnoc.overlay({"sarc.notifications": notify_cfg or _NOTIFY_CFG}):
        return cli_main(["usage", "refresh-store", *extra_args])


def _mila_id(session):
    return next(
        c.id for c in session.exec(select(SlurmClusterDB)).all() if c.name == "mila"
    )


def _user_id(session, name):
    return next(
        u.id
        for u in session.exec(select(UserDB)).all()
        if u.email.split("@")[0] == name
    )


def _stored_rows(session, *, user_id, cluster_id):
    """Return this (user, cluster)'s stored rows, newest end_date first.

    Expires the session's identity map first: refresh_store's own
    `config.db.session()` commits in a separate session, so without this a
    repeat query here can return stale, already-loaded Python objects instead
    of the freshly committed values.
    """
    session.expire_all()
    rows = session.exec(
        select(UserPeriods).where(
            UserPeriods.user_id == user_id, UserPeriods.cluster_id == cluster_id
        )
    ).all()
    return sorted(rows, key=lambda r: r.end_date, reverse=True)


# ── Populates last _HISTORY_CYCLES cycles, nothing beyond ───────────────────


def test_populates_last_12_cycles(recurring_db, cli_main, monkeypatch):  # noqa: F811
    rc = _run(cli_main, monkeypatch)
    assert rc == 0

    firstuser_id = _user_id(recurring_db, "firstuser")
    mila_id = _mila_id(recurring_db)
    rows = _stored_rows(recurring_db, user_id=firstuser_id, cluster_id=mila_id)

    # firstuser has jobs at W0, W-2, W-4, W-6, W-8 only (recurring_db fixture) —
    # every stored end_date must fall within the last _HISTORY_CYCLES cycles,
    # and must match exactly those 5 known cycle boundaries.
    expected_end_dates = {_TEST_END - i * _14D for i in range(5)}
    assert {r.end_date for r in rows} == expected_end_dates
    assert len(rows) <= _HISTORY_CYCLES


# ── Pruning ──────────────────────────────────────────────────────────────────


def test_prunes_rows_older_than_history_window(recurring_db, cli_main, monkeypatch):  # noqa: F811
    firstuser_id = _user_id(recurring_db, "firstuser")
    mila_id = _mila_id(recurring_db)

    # A stale row from a hypothetical earlier run, well outside the
    # _HISTORY_CYCLES=12-cycle window kept by this refresh.
    stale_end_date = _TEST_END - (_HISTORY_CYCLES + 5) * _14D
    recurring_db.add(
        UserPeriods(
            user_id=firstuser_id,
            cluster_id=mila_id,
            start_date=stale_end_date - _14D,
            end_date=stale_end_date,
            sm_occ_mean=0.05,
            rgu_hours=100.0,
            unused_rguh=95.0,
            isunderuser=True,
            flagged=False,
            elevated=False,
        )
    )
    recurring_db.commit()

    rc = _run(cli_main, monkeypatch)
    assert rc == 0

    rows = _stored_rows(recurring_db, user_id=firstuser_id, cluster_id=mila_id)
    assert stale_end_date not in {r.end_date for r in rows}


# ── Idempotent re-run ────────────────────────────────────────────────────────


def test_idempotent_rerun_no_duplicate_rows(recurring_db, cli_main, monkeypatch):  # noqa: F811
    rc1 = _run(cli_main, monkeypatch)
    assert rc1 == 0
    count1 = len(recurring_db.exec(select(UserPeriods)).all())

    rc2 = _run(cli_main, monkeypatch)
    assert rc2 == 0
    count2 = len(recurring_db.exec(select(UserPeriods)).all())

    assert count1 == count2


# ── Upsert updates values in place ──────────────────────────────────────────


def test_upsert_updates_changed_values_in_place(recurring_db, cli_main, monkeypatch):  # noqa: F811
    from tests.unittests.notifications._factory import add_gpu_job
    from tests.unittests.notifications.test_recurring import _W0_START

    firstuser_id = _user_id(recurring_db, "firstuser")
    mila_id = _mila_id(recurring_db)

    rc1 = _run(cli_main, monkeypatch)
    assert rc1 == 0
    before = _stored_rows(recurring_db, user_id=firstuser_id, cluster_id=mila_id)
    before_w0 = next(r for r in before if r.end_date == _TEST_END)
    before_id = before_w0.id
    before_unused = before_w0.unused_rguh

    # Add another W0 job for firstuser, changing the W0 cycle's totals.
    add_gpu_job(
        recurring_db,
        user_id=firstuser_id,
        cluster_id=mila_id,
        elapsed_h=20,
        submit_time=_W0_START,
        job_id=99999,
        utilization=0.05,
    )
    recurring_db.commit()

    rc2 = _run(cli_main, monkeypatch)
    assert rc2 == 0
    after = _stored_rows(recurring_db, user_id=firstuser_id, cluster_id=mila_id)
    after_w0 = next(r for r in after if r.end_date == _TEST_END)

    # Same row (same primary key — updated in place, not duplicated), new value.
    assert after_w0.id == before_id
    assert after_w0.unused_rguh != before_unused
    assert len(after) == len(before)


# ── elevated matches _restrictive_action_flags ──────────────────────────────


def test_elevated_matches_restrictive_action_flags(recurring_db, cli_main, monkeypatch):  # noqa: F811
    rc = _run(cli_main, monkeypatch)
    assert rc == 0

    firstuser_id = _user_id(recurring_db, "firstuser")
    mila_id = _mila_id(recurring_db)
    rows = _stored_rows(recurring_db, user_id=firstuser_id, cluster_id=mila_id)

    rows_by_end_date = {r.end_date: r for r in rows}
    flagged_by_position = [
        rows_by_end_date[_TEST_END - i * _14D].flagged
        if (_TEST_END - i * _14D) in rows_by_end_date
        else False
        for i in range(_HISTORY_CYCLES)
    ]
    # _restrictive_action_flags reads restrictive_action_run_cycles from config.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        expected_elevated = _restrictive_action_flags(flagged_by_position)

    for i in range(_HISTORY_CYCLES):
        end_date = _TEST_END - i * _14D
        if end_date in rows_by_end_date:
            assert rows_by_end_date[end_date].elevated == expected_elevated[i]


# ── Error paths (mirror UsageNotifyCommand's) ───────────────────────────────


def test_missing_notifications_config_returns_error(cli_main):
    # base_config (autouse) does not set sarc.notifications → must return -1.
    rc = cli_main(["usage", "refresh-store"])
    assert rc == -1


def test_invalid_as_of_returns_error(recurring_db, cli_main, caplog):  # noqa: F811
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc = cli_main(["usage", "refresh-store", "--as-of", "not-a-date"])
    assert rc == -1
    assert any("not-a-date" in r.message for r in caplog.records)


# ── Parity: store-backed vs. --ignore-store live recompute ─────────────────


def test_notify_usage_parity_between_store_and_ignore_store(
    recurring_db,  # noqa: F811
    cli_main,
    monkeypatch,
    capsys,
):
    """Once the store is freshly refreshed, `usage notify` (store-backed,
    default) and `usage notify --ignore-store` (live recompute) must render
    an identical recurring-underusers table for the same fixture — proving
    PowerBI (which only ever reads the store) and Slack (either path) can
    never disagree."""
    rc0 = _run(cli_main, monkeypatch)
    assert rc0 == 0

    monkeypatch.setattr("sarc.cli.usage.notify._now_utc", lambda: _TEST_END)

    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc1 = cli_main(["usage", "notify"])
    assert rc1 == 0
    store_out = capsys.readouterr().out
    assert "Recurring underusers" in store_out

    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        rc2 = cli_main(["usage", "notify", "--ignore-store"])
    assert rc2 == 0
    live_out = capsys.readouterr().out

    assert store_out == live_out


# ── Off-cycle end on the store path ─────────────────────────────────────────


def test_off_cycle_week_end_store_path_w0_excluded_but_aggregated(
    recurring_db,  # noqa: F811
    cli_main,
    monkeypatch,
):
    """Store-path counterpart to test_off_cycle_week_end_w0_is_none: with an
    off-cycle `end`, anchor lands on an existing stored end_date but position 0
    is excluded from active_positions (cycles[0] stays None) while its stored
    waste still feeds the aggregate — and `clusters` is omitted here to also
    exercise the no-allowlist branch on the store-read path."""
    rc = _run(
        cli_main, monkeypatch
    )  # populates store anchored at _TEST_END (wk 26, even)
    assert rc == 0

    off_cycle_end = datetime(
        2024, 6, 23, tzinfo=UTC
    )  # wk 25 (odd) -> anchor == _TEST_END
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            off_cycle_end,
            min_waste_ratio=_MIN_WASTE_RATIO,
            min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
            ignore_store=False,
        )  # clusters omitted -> None -> exercises the `if clusters:` false arm

    assert result, "expected selected users for the off-cycle-week window"
    for rows in result.values():
        for row in rows:
            assert row.cycles[0] is None, (
                f"expected cycles[0]=None for off-cycle-week end, got {row.cycles[0]}"
            )
