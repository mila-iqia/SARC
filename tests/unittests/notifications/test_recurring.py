"""Tests for recurring-underusers table (get_recurring_underusers + builder)."""

from datetime import UTC, date, datetime, timedelta

import gifnoc
import pytest
from sqlmodel import select

from sarc.db.cluster import SlurmClusterDB
from sarc.db.users import UserDB
from sarc.notifications.messages import build_recurring_table
from sarc.notifications.underusage import (
    RecurringUserRow,
    _even_week_anchor,
    get_cycle_dates,
    get_recurring_underusers,
)
from tests.unittests.notifications._factory import DEFAULT_GPU_TYPE, add_gpu_job

# "Today" for all recurring tests.
_TEST_END = datetime(2024, 6, 30, tzinfo=UTC)
_14D = timedelta(days=14)

# Default keyword args for build_recurring_table and get_recurring_underusers callers
# that don't need to exercise specific window/share values.
_BRT_KW = {"cluster_share_threshold": 0.30, "cycle_length_weeks": 2, "active_cycles": 3}
_GRU_KW = {"cluster_share_threshold": 0.30}

# Cycle windows (rolling from _TEST_END)
# W0:  [2024-06-16, 2024-06-30]
# W-2: [2024-06-02, 2024-06-16]
# W-4: [2024-05-19, 2024-06-02]
# W-6: [2024-05-05, 2024-05-19]
# W-8: [2024-04-21, 2024-05-05]
# 6-week aggregate: [2024-05-19, 2024-06-30] = W-4 + W-2 + W0

_W0_START = _TEST_END - 1 * _14D  # 2024-06-16
_W2_START = _TEST_END - 2 * _14D  # 2024-06-02
_W4_START = _TEST_END - 3 * _14D  # 2024-05-19
_W6_START = _TEST_END - 4 * _14D  # 2024-05-05
_W8_START = _TEST_END - 5 * _14D  # 2024-04-21

_MILA_RGU = 4.8

_MIN_RATIO = 0.50
_MIN_RGU_HOURS = 24

_NOTIFY_CFG = {
    "slack": {
        "description": "test channel",
        "token": "xoxb-test-token",
        "channel": "#test-channel",
    },
    "min_ratio": _MIN_RATIO,
    "min_rgu_hours": _MIN_RGU_HOURS,
    "digest_top_n": 16,
    "recurrence_cluster_share": 0.70,
    "personalized_action_min_rgu_hours": 115.2,  # 24h * A100 80GB RGU
}


def _add_gpu_job(session, *, utilization: float | None = 0.0, **kwargs):
    """Seed a job at the literal submit_time (recurring tests pass cycle starts)."""
    return add_gpu_job(session, utilization=utilization, **kwargs)


@pytest.fixture
def recurring_db(read_write_db):
    """Seed multi-cluster, multi-cycle data for recurring-underusers tests.

    Layout (mila cluster, util=0.05 so rgu_used > 0 for exclude_zero_usage=True):
      rgu_h = 4.8 * elapsed_h; wasted = rgu_h * 0.95; waste_ratio = 0.95 ≥ 0.50 ✓

      - firstuser:     W0+W-2+W-4 @ 20h → wasted 3*91.2=273.6 RGU-h over 6w (highest)
                       ALSO W-6 @ 20h and W-8 @ 20h
                       personalized_action=True (W0+W-2+W-4 >115.2 RGU)
      - seconduser:    W0+W-2+W-4 @ 15h → wasted 3*68.4=205.2 RGU-h (2nd)
                       NO W-6, NO W-8
                       personalized_action=True (W0+W-2+W-4 >115.2 RGU)
      - thirduser:     W0+W-2+W-4 @ 10h → wasted 3*45.6=136.8 RGU-h (not selected at 70%)
                       ALSO W-6 @ 0h and W-8 @ 10h
                       personalized_action=True (W0+W-2+W-4 >115.2 RGU)
      - fourthuser:    W0+W-2+W-4 @  5h → wasted 3*22.8=68.4 RGU-h (not selected)
                       ALSO W-6 @ 15h and W-8 @ 15h
                       personalized_action=False (W0+W-2+W-4 <115.2 RGU)

    Cluster wasted total (6w) = 684 RGU-h.
    Selection at 70%:
      firstuser (40%) < 70% → continue
      +seconduser (70%) ≥ 70% → stop  (thirduser + fourthuser excluded)

    Per-cycle floor (min_rgu_hours=24): firstuser/seconduser/thirduser pass (≥24);
    fourthuser (22.8) does not
    """
    session = read_write_db
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}
    mila_id = clusters["mila"].id

    # Add 4 dedicated test users so the fixture isn't polluted by the shared DB
    # fixture's users.
    firstuser = UserDB(display_name="M/Ms Firstuser", email="firstuser@mila.quebec")
    session.add(firstuser)
    seconduser = UserDB(display_name="M/Ms Seconduser", email="seconduser@mila.quebec")
    session.add(seconduser)
    thirduser = UserDB(display_name="M/Ms Thirduser", email="thirduser@mila.quebec")
    session.add(thirduser)
    fourthuser = UserDB(display_name="M/Ms Fourthuser", email="fourthuser@mila.quebec")
    session.add(fourthuser)
    session.flush()

    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    firstuser_id = users["firstuser"].id
    seconduser_id = users["seconduser"].id
    thirduser_id = users["thirduser"].id
    fourthuser_id = users["fourthuser"].id

    job_id = 90000

    def _seed(user_id: int, submit: datetime, elapsed: float) -> None:
        nonlocal job_id
        _add_gpu_job(
            session,
            user_id=user_id,
            cluster_id=mila_id,
            elapsed_h=elapsed,
            submit_time=submit,
            job_id=job_id,
            gpu_type=DEFAULT_GPU_TYPE,
            utilization=0.05,
        )
        job_id += 1

    # firstuser: W0, W-2, W-4 (6w aggregate) + W-6 + W-8 (display-only cycles)
    for start in (_W0_START, _W2_START, _W4_START, _W6_START, _W8_START):
        _seed(firstuser_id, start, 20)

    # seconduser: W0, W-2, W-4 only
    for start in (_W0_START, _W2_START, _W4_START):
        _seed(seconduser_id, start, 15)

    # thirduser: W0, W-2, W-4 (not selected at 70%)
    for start in (_W0_START, _W2_START, _W4_START):
        _seed(thirduser_id, start, 10)

    for start in (_W8_START,):
        _seed(thirduser_id, start, 10)

    # fourthuser: W0, W-2, W-4 (not selected; per-cycle wasted < floor)
    for start in (_W0_START, _W2_START, _W4_START):
        _seed(fourthuser_id, start, 5)

    for start in (_W6_START, _W8_START):
        _seed(fourthuser_id, start, 15)

    session.commit()
    yield session


# ── Selection / 30 % cutoff ───────────────────────────────────────────────────


def test_selection_threshold(recurring_db):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            cluster_share_threshold=0.70,
        )
    mila_rows = result.get("mila", [])
    selected_emails = {r.email for r in mila_rows}
    # firstuser (highest) + seconduser together cross 70%; thirduser/fourthuser not included.
    assert "firstuser@mila.quebec" in selected_emails
    assert "seconduser@mila.quebec" in selected_emails
    assert "thirduser@mila.quebec" not in selected_emails
    assert "fourthuser@mila.quebec" not in selected_emails

    cumulative = sum(r.cluster_share for r in mila_rows)
    assert cumulative >= 0.70


# ── Cluster share values ──────────────────────────────────────────────────────


def test_all_users_included_when_threshold_is_one(recurring_db):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}) as config:
        ncfg = config.sarc.notifications
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            cluster_share_threshold=1.0,
            personalized_action_min_rgu_hours=ncfg.personalized_action_min_rgu_hours,
        )
    mila_emails = {r.email for r in result.get("mila", [])}
    assert "firstuser@mila.quebec" in mila_emails
    assert "seconduser@mila.quebec" in mila_emails
    assert "thirduser@mila.quebec" in mila_emails
    assert "fourthuser@mila.quebec" in mila_emails

    wastes = [r.wasted_current_active_window for r in result["mila"]]
    assert wastes == sorted(wastes, reverse=True)

    total_share = sum(r.cluster_share for r in result["mila"])
    assert abs(total_share - 1.0) < 1e-6

    # Test firstuser flagged all five cycles
    row = next(r for r in result["mila"] if r.email == "firstuser@mila.quebec")
    assert row.cycles == [True] * 5
    # Test firstuser flagged for personalized action all 3 cycles
    assert row.pa_flags == [True] * 3
    assert row.personalized_action is True

    # Test seconduser only flagged for the first 3 cycles
    row = next(r for r in result["mila"] if r.email == "seconduser@mila.quebec")
    assert row.cycles == [True] * 3 + [False] * 2
    # Test seconduser flagged for personalized action the first 2 cycles
    assert row.pa_flags == [True] * 2 + [False] * 1
    assert row.personalized_action is True

    # Test thirduser flagged all cycles except for 4th
    row = next(r for r in result["mila"] if r.email == "thirduser@mila.quebec")
    assert row.cycles == [True] * 3 + [False] * 1 + [True] * 1
    # Test thirduser flagged for personalized action the first cycle
    assert row.pa_flags == [True] * 1 + [False] * 2
    assert row.personalized_action is True

    # Test fourthuser only flagged for the last 2 cycles
    row = next(r for r in result["mila"] if r.email == "fourthuser@mila.quebec")
    assert row.cycles == [False] * 3 + [True] * 2
    # Test fourthuser only flagged for personalized action on 3rd cycles
    assert row.pa_flags == [False] * 2 + [True] * 1
    assert row.personalized_action is False


# ── Non-default cycle counts (acceptance criteria) ────────────────────────────


def test_display_cycles_columns(recurring_db):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            cluster_share_threshold=1.0,
            recurrence_display_cycles=4,
        )
    # Test cycles 4 produces 4 columns
    for rows in result.values():
        for row in rows:
            assert len(row.cycles) == 4

    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            cluster_share_threshold=1.0,
            recurrence_display_cycles=6,
        )
    # Test cycles 6 produces 6 columns
    for rows in result.values():
        for row in rows:
            assert len(row.cycles) == 6


def test_active_cycles_personalized_action(recurring_db):
    """recurrence_active_cycles widens the PA window; floor=300 splits at the 3→4 cycle boundary."""
    # Using a loop instead of pytest.mark.parametrize to avoid repopulating the
    # database for each tests
    for active_cycles, expected in [
        (2, False),  # PA window=4w: firstuser waste=182.4 < 300 floor
        (3, False),  # PA window=6w: firstuser waste=273.6 < 300 floor
        (
            4,
            True,
        ),  # PA window=8w: firstuser waste=364.8 >= 300 floor (W-6 enters window)
        (
            5,
            True,
        ),  # PA window=10w: firstuser waste=456 >= 300 floor (W-8 enters window)
    ]:
        with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
            result = get_recurring_underusers(
                _TEST_END,
                min_ratio=_MIN_RATIO,
                min_rgu_hours=_MIN_RGU_HOURS,
                cluster_share_threshold=1.0,
                recurrence_active_cycles=active_cycles,
                personalized_action_min_rgu_hours=300.0,
            )
            row = next(r for r in result["mila"] if r.email == "firstuser@mila.quebec")
            assert row.personalized_action is expected, (
                f"{expected=} for {active_cycles=} but got {row.personalized_action=}"
            )


# ── Empty / edge cases ────────────────────────────────────────────────────────


def test_empty_db_returns_empty_dict(read_write_db):
    before = datetime(2020, 1, 1, tzinfo=UTC)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            before, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS, **_GRU_KW
        )
    assert result == {}


def test_unsupported_resource_raises(recurring_db):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        with pytest.raises(ValueError, match="Unsupported resource"):
            get_recurring_underusers(
                _TEST_END,
                min_ratio=_MIN_RATIO,
                min_rgu_hours=_MIN_RGU_HOURS,
                resource="cpu",
                **_GRU_KW,
            )


# ── build_recurring_table ─────────────────────────────────────────────────────

_ROW_ALICE = RecurringUserRow(
    email="alice@mila.quebec",
    display_name="Alice Liddell",
    cluster="narval",
    wasted_current_active_window=4200.0,
    cluster_share=0.18,
    cycles=[True, True, True, True, True],
    personalized_action=True,
)

_ROW_BOB = RecurringUserRow(
    email="bob@mila.quebec",
    display_name="Bob Marley",
    cluster="narval",
    wasted_current_active_window=2600.0,
    cluster_share=0.11,
    cycles=[True, False, True, True, True],
    personalized_action=False,
)

_ROW_CAROL = RecurringUserRow(
    email="carol@mila.quebec",
    display_name="Carol Danvers",
    cluster="narval",
    wasted_current_active_window=1100.0,
    cluster_share=0.05,
    cycles=[True, True, False, False, False],
    personalized_action=False,
)


def test_table_header_contains_cluster():
    text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "Cluster narval" in text


def test_table_header_contains_window_weeks():
    text = build_recurring_table(
        {"narval": [_ROW_ALICE]},
        cluster_share_threshold=0.30,
        cycle_length_weeks=2,
        active_cycles=3,
    )
    assert "last 6 weeks" in text


def test_table_threshold_in_sub_header():
    text = build_recurring_table(
        {"narval": [_ROW_ALICE]},
        cluster_share_threshold=0.30,
        cycle_length_weeks=2,
        active_cycles=3,
    )
    assert "30 %" in text


def test_table_contains_email():
    text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "alice@mila.quebec" in text


def test_table_wasted_formatted_with_space_thousands():
    text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "4 200" in text


def test_table_share_percentage():
    text = build_recurring_table({"narval": [_ROW_BOB]}, **_BRT_KW)
    assert "11 %" in text


def test_table_personalized_action_flag():
    text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "⚑ personalized" in text


def test_table_no_action_flag_when_not_all_cycles():
    text = build_recurring_table({"narval": [_ROW_BOB]}, **_BRT_KW)
    assert "⚑ personalized" not in text


def test_table_check_and_cross_marks():
    text = build_recurring_table({"narval": [_ROW_BOB]}, **_BRT_KW)
    assert "✓" in text
    assert "✗" in text


def test_table_tree_chars_multiple_rows():
    text = build_recurring_table(
        {"narval": [_ROW_ALICE, _ROW_BOB, _ROW_CAROL]}, **_BRT_KW
    )
    assert "┌─" in text
    assert "├─" in text
    assert "└─" in text


def test_table_single_row_uses_end_cap():
    text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "└─" in text
    assert "┌─" not in text


def test_table_multiple_clusters_rendered():
    carol_fir = RecurringUserRow(
        email="carol@mila.quebec",
        display_name="Carol Danvers",
        cluster="fir",
        wasted_current_active_window=500.0,
        cluster_share=0.50,
        cycles=[True, False, False, False, False],
        personalized_action=False,
    )
    text = build_recurring_table(
        {"narval": [_ROW_ALICE], "fir": [carol_fir]}, **_BRT_KW
    )
    assert "Cluster narval" in text
    assert "Cluster fir" in text


def test_table_clusters_sorted_alphabetically():
    carol_fir = RecurringUserRow(
        email="carol@mila.quebec",
        display_name="Carol Danvers",
        cluster="fir",
        wasted_current_active_window=500.0,
        cluster_share=0.50,
        cycles=[True, False, False, False, False],
        personalized_action=False,
    )
    text = build_recurring_table(
        {"narval": [_ROW_ALICE], "fir": [carol_fir]}, **_BRT_KW
    )
    assert text.index("Cluster fir") < text.index("Cluster narval")


def test_table_empty_dict_returns_empty_string():
    assert build_recurring_table({}, **_BRT_KW) == ""


def test_table_deterministic():
    data = {"narval": [_ROW_ALICE, _ROW_BOB]}
    assert build_recurring_table(data, **_BRT_KW) == build_recurring_table(
        data, **_BRT_KW
    )


# ── CLI integration: recurring table appears in digest output ─────────────────


def test_dry_run_prints_recurring_table(recurring_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage"])
    out = capsys.readouterr().out
    assert "Recurring underusers" in out


@pytest.mark.parametrize("display_cycles", [4, 6])
def test_dry_run_display_cycles(
    recurring_db, cli_main, monkeypatch, capsys, display_cycles
):
    """CLI must not IndexError when recurrence_display_cycles deviates from the default 5."""
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _TEST_END)
    cfg = {**_NOTIFY_CFG, "recurrence_display_cycles": display_cycles}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        cli_main(["notify", "underusage"])
    out = capsys.readouterr().out
    assert "Recurring underusers" in out


# ── _even_week_anchor ─────────────────────────────────────────────────────────

# 2024-06-24: Monday of ISO week 26 (even)
_EVEN_MON = datetime(2024, 6, 24, tzinfo=UTC)
# 2024-06-30: Sunday of ISO week 26 (even) — _TEST_END
_EVEN_SUN = _TEST_END
# 2024-06-26: Wednesday of ISO week 26 (even)
_EVEN_WED = datetime(2024, 6, 26, tzinfo=UTC)
# 2024-06-17: Monday of ISO week 25 (odd)
_ODD_MON = datetime(2024, 6, 17, tzinfo=UTC)
# 2024-06-19: Wednesday of ISO week 25 (odd)
_ODD_WED = datetime(2024, 6, 19, tzinfo=UTC)


def test_anchor_even_monday_returns_same_day():
    # 2024-06-24 is already the Monday of an even week
    assert _even_week_anchor(_EVEN_MON) == _EVEN_MON.replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def test_anchor_even_midweek_returns_same_day():
    # 2024-06-26 (Wed, wk 26 even) → anchor = same day
    assert _even_week_anchor(_EVEN_WED) == _EVEN_WED.replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def test_anchor_even_sunday_returns_same_day():
    # 2024-06-30 (Sun, wk 26 even) → anchor = same day
    assert _even_week_anchor(_EVEN_SUN) == _EVEN_SUN.replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def test_anchor_odd_monday_shifts_to_next_week():
    # 2024-06-17 (Mon, wk 25 odd) → anchor = 2024-06-24 (Mon, wk 26 even)
    assert _even_week_anchor(_ODD_MON) == _EVEN_MON.replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def test_anchor_odd_midweek_shifts_to_next_week():
    # 2024-06-19 (Wed, wk 25 odd) → anchor = 2024-06-26 (Wed, wk 26 even)
    assert _even_week_anchor(_ODD_WED) == (_ODD_WED + timedelta(weeks=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def test_anchor_result_always_even_week():
    for offset in range(28):
        dt = datetime(2024, 6, 1, tzinfo=UTC) + timedelta(days=offset)
        anchor = _even_week_anchor(dt)
        assert anchor.isocalendar().week % 2 == 0, f"odd week for end={dt.date()}"


# ── get_cycle_dates ───────────────────────────────────────────────────────────


def test_cycle_dates_even_week_five_mondays():
    # end = 2024-06-24 (Mon, wk 26 even)
    dates = get_cycle_dates(_EVEN_MON)
    assert len(dates) == 5
    assert all(isinstance(d, date) for d in dates)
    # Each must be a Monday of an even week, 14 days apart
    for i, d in enumerate(dates):
        dt = datetime(d.year, d.month, d.day, tzinfo=UTC)
        assert dt.weekday() == 0, f"dates[{i}] is not a Monday"
        assert dt.isocalendar().week % 2 == 0, f"dates[{i}] not an even week"
    for i in range(len(dates) - 1):
        assert (dates[i] - dates[i + 1]).days == 14


def test_cycle_dates_even_week_all_not_future():
    # end = 2024-06-24 (Mon, wk 26 even); all cycle dates ≤ end.date()
    end = _EVEN_MON
    dates = get_cycle_dates(end)
    for d in dates:
        assert d <= end.date()


def test_cycle_dates_odd_week_w0_is_future():
    # end = 2024-06-17 (Mon, wk 25 odd); W0 anchor = 2024-06-24 > end
    end = _ODD_MON
    dates = get_cycle_dates(end)
    assert dates[0] > end.date(), "W0 should be in the future for odd-week end"
    for d in dates[1:]:
        assert d < end.date(), f"{d} should be in the past"


# ── get_recurring_underusers — odd-week end ───────────────────────────────────


def test_odd_week_end_w0_is_none(recurring_db):
    # _ODD_MON = 2024-06-17 (wk 25, odd) → anchor = 2024-06-24 > end → w0=None
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _ODD_MON,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            cluster_share_threshold=0.70,
        )
    assert result, "expected selected users for the odd-week window"
    for rows in result.values():
        for row in rows:
            assert row.cycles[0] is None, (
                f"expected cycles[0]=None for odd-week end, got {row.cycles[0]}"
            )


def test_odd_week_end_personalized_action_floor_controls(recurring_db):
    # With w0=None the PA flag is still based on waste in the active window, not cycles.
    # A high floor means nobody qualifies even though users have past-cycle waste.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _ODD_MON,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            cluster_share_threshold=0.70,
            personalized_action_min_rgu_hours=999999.0,
        )
    for rows in result.values():
        for row in rows:
            assert row.personalized_action is False


# ── build_recurring_table with cycle_dates ────────────────────────────────────

_CYCLE_DATES = [
    date(2024, 6, 24),  # W0  (Mon, wk 26 even)
    date(2024, 6, 10),  # W-2 (Mon, wk 24 even)
    date(2024, 5, 27),  # W-4 (Mon, wk 22 even)
    date(2024, 5, 13),  # W-6 (Mon, wk 20 even)
    date(2024, 4, 29),  # W-8 (Mon, wk 18 even)
]

_ROW_FUTURE_W0 = RecurringUserRow(
    email="alice@mila.quebec",
    display_name="Alice Liddell",
    cluster="narval",
    wasted_current_active_window=4200.0,
    cluster_share=0.18,
    cycles=[None, True, True, False, False],
    personalized_action=False,
)


def test_table_with_cycle_dates_renders_mm_dd_headers():
    text = build_recurring_table(
        {"narval": [_ROW_ALICE]}, cycle_dates=_CYCLE_DATES, **_BRT_KW
    )
    assert "06-24" in text
    assert "06-10" in text
    assert "05-27" in text
    assert "05-13" in text
    assert "04-29" in text


def test_table_with_cycle_dates_no_w0_label():
    text = build_recurring_table(
        {"narval": [_ROW_ALICE]}, cycle_dates=_CYCLE_DATES, **_BRT_KW
    )
    assert "W0" not in text
    assert "W-2" not in text


def test_table_none_flag_renders_blank_not_cross(capsys):
    text = build_recurring_table(
        {"narval": [_ROW_FUTURE_W0]}, cycle_dates=_CYCLE_DATES, **_BRT_KW
    )
    # _ROW_FUTURE_W0: w0=None→blank, w2=True→✗, w4=True→✗, w6=False→✓, w8=False→✓
    # True (flagged/underuser) → ✗; False (good usage) → ✓; None (future) → blank
    assert text.count("✓") == 2  # w6 and w8
    assert text.count("✗") == 2  # w2 and w4


def test_table_without_cycle_dates_keeps_w0_label():
    # Backward compat: no cycle_dates → "W0"/"W-2" labels still present
    text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "W0" in text
    assert "W-2" in text


# ── | separator ──────────────────────────────────────────────────────────────


def test_table_contains_separator():
    text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "|" in text


def test_table_separator_between_w4_and_w6():
    text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    # W-4 column appears before | which appears before W-6 column
    assert text.index("W-4") < text.index("|") < text.index("W-6")


# ── per-cycle ⚑ ───────────────────────────────────────────────────────────────

# Row where only W-4 (position 2) is pa-flagged → ⚑ at W-4 only
_ROW_PEAK_AT_W4 = RecurringUserRow(
    email="dave@mila.quebec",
    display_name="Dave Bowie",
    cluster="narval",
    wasted_current_active_window=3000.0,
    cluster_share=0.25,
    cycles=[False, False, True, True, True],
    personalized_action=False,
    pa_flags=[False, False, True],
)

# Row where all 3 active positions are pa-flagged → ⚑ at W0, W-2, W-4
_ROW_ALL_TRUE = RecurringUserRow(
    email="eve@mila.quebec",
    display_name="Eve Online",
    cluster="narval",
    wasted_current_active_window=5000.0,
    cluster_share=0.40,
    cycles=[True, True, True, True, True],
    personalized_action=True,
    pa_flags=[True, True, True],
)


def test_per_cycle_peak_at_w4():
    text = build_recurring_table({"narval": [_ROW_PEAK_AT_W4]}, **_BRT_KW)
    assert "⚑✗" in text


def test_per_cycle_no_peak_at_w0_w2():
    # pa_flags[0]=False and pa_flags[1]=False → no ⚑ at W0 or W-2
    text = build_recurring_table({"narval": [_ROW_PEAK_AT_W4]}, **_BRT_KW)
    assert text.count("⚑✗") == 1


def test_per_cycle_all_active_flagged():
    text = build_recurring_table({"narval": [_ROW_ALL_TRUE]}, **_BRT_KW)
    # pa_flags=[True,True,True] → ⚑ at W0, W-2, W-4
    assert text.count("⚑✗") == 3


def test_per_cycle_w6_w8_never_show_peak():
    # Positions ≥ active_cycles are never eligible regardless of pa_flags
    text = build_recurring_table({"narval": [_ROW_ALL_TRUE]}, **_BRT_KW)
    assert text.count("⚑✗") == 3


def test_per_cycle_no_peak_on_passing_cell():
    # ⚑ never renders on a ✓ cell even when pa_flags[i] is True
    row = RecurringUserRow(
        email="frank@mila.quebec",
        display_name="Frank Test",
        cluster="narval",
        wasted_current_active_window=1000.0,
        cluster_share=0.10,
        cycles=[False, True, True, True, True],
        personalized_action=True,
        pa_flags=[True, True, True],
    )
    text = build_recurring_table({"narval": [row]}, **_BRT_KW)
    # W0 is ✓ (cycles[0]=False) so no ⚑ there; W-2 and W-4 are ✗ with pa_flags → 2 ⚑✗
    assert text.count("⚑✗") == 2


# ── Threshold scaling, true_wasted, personalized_action floor ────────────────


def test_wasted_6w_uses_scaled_waste(recurring_db):
    # firstuser: util=0.05, threshold=0.05 → credited_used=LEAST(rgu_h, rgu_h*1.0)=rgu_h
    # → wasted=0 → excluded from the recurring table entirely
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            cluster_share_threshold=1.0,
            utilization_ceiling=0.05,
        )
    emails = {r.email for rows in result.values() for r in rows}
    assert "firstuser@mila.quebec" not in emails


def test_true_wasted_field_populated(recurring_db):
    # At identity threshold, RecurringUserRow.true_wasted should be positive.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            cluster_share_threshold=1.0,
        )
    row = next(r for r in result["mila"] if r.email == "firstuser@mila.quebec")
    assert row.true_wasted > 0.0


def test_personalized_action_floor(recurring_db):
    # With floor=0.0, any user with positive scaled waste in the active window is flagged.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            cluster_share_threshold=1.0,
            personalized_action_min_rgu_hours=0.0,
        )
        for rows in result.values():
            for row in rows:
                assert row.personalized_action is True

    # With a very high floor, no user crosses the threshold → all False.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            cluster_share_threshold=1.0,
            personalized_action_min_rgu_hours=999999.0,
        )
    for rows in result.values():
        for row in rows:
            assert row.personalized_action is False


# ── Per-anchor pa_flags four-scenario computation test ────────────────────────
# RGU=4.8, util=0.0 → wasted = 4.8 * elapsed_h. elapsed_h = target_rgu_h / 4.8.
# Scenarios (active_cycles=3, cycle_length_weeks=2, PA threshold=30):
#   user1: 10 RGU-h/cycle × 5 → every 3-cycle window = 30 ≥ 30 → pa_flags=[T,T,T]
#   user2: W0=15,W-2=10,W-4=5,W-6=5,W-8=5
#          pos-0=30≥30,pos-1=20<30,pos-2=15<30 → pa_flags=[T,F,F]
#   user3: W0=30 only → pos-0=30,pos-1=0,pos-2=0 → pa_flags=[T,F,F]
#   user4: 5 RGU-h/cycle × 5 → every window = 15 < 30 → pa_flags=[F,F,F]


@pytest.fixture
def pa_scenario_db(read_write_db):
    session = read_write_db
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}
    mila_id = clusters["mila"].id

    u1 = UserDB(display_name="PA Scenario U1", email="pa_u1@mila.quebec")
    u2 = UserDB(display_name="PA Scenario U2", email="pa_u2@mila.quebec")
    u3 = UserDB(display_name="PA Scenario U3", email="pa_u3@mila.quebec")
    u4 = UserDB(display_name="PA Scenario U4", email="pa_u4@mila.quebec")
    for u in (u1, u2, u3, u4):
        session.add(u)
    session.flush()

    job_id_ctr = [80000]

    def _seed(uid, start, rgu_h):
        _add_gpu_job(
            session,
            user_id=uid,
            cluster_id=mila_id,
            elapsed_h=rgu_h / _MILA_RGU,
            submit_time=start,
            job_id=job_id_ctr[0],
            utilization=0.0,
        )
        job_id_ctr[0] += 1

    for start in (_W0_START, _W2_START, _W4_START, _W6_START, _W8_START):
        _seed(u1.id, start, 10)

    for start, rgu in (
        (_W0_START, 15),
        (_W2_START, 10),
        (_W4_START, 5),
        (_W6_START, 5),
        (_W8_START, 5),
    ):
        _seed(u2.id, start, rgu)

    _seed(u3.id, _W0_START, 30)

    for start in (_W0_START, _W2_START, _W4_START, _W6_START, _W8_START):
        _seed(u4.id, start, 5)

    session.commit()
    yield {"u1": u1, "u2": u2, "u3": u3, "u4": u4}


def test_pa_flags_four_scenarios(pa_scenario_db):
    """pa_flags are computed correctly for the four canonical scenarios."""
    users = pa_scenario_db
    result = get_recurring_underusers(
        _TEST_END,
        min_ratio=0.0,
        min_rgu_hours=0.0,
        cluster_share_threshold=1.1,
        recurrence_active_cycles=3,
        recurrence_display_cycles=5,
        cycle_length_weeks=2,
        clusters=["mila"],
        personalized_action_min_rgu_hours=30.0,
    )
    rows = {r.email: r for r in result.get("mila", [])}

    u1 = rows[users["u1"].email]
    u2 = rows[users["u2"].email]
    u3 = rows[users["u3"].email]
    u4 = rows[users["u4"].email]

    assert u1.pa_flags == [True, True, True]
    assert u1.personalized_action is True

    assert u2.pa_flags == [True, False, False]
    assert u2.personalized_action is True

    assert u3.pa_flags == [True, False, False]
    assert u3.personalized_action is True

    assert u4.pa_flags == [False, False, False]
    assert u4.personalized_action is False
