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
    _week_anchor,
    get_cycle_dates,
    get_recurring_underusers,
    usage_cycle_length_weeks,
)
from tests.unittests.notifications._factory import (
    DEFAULT_GPU_TYPE,
    UNDERUSAGE_REPORT_TEMPLATE,
    USAGE_REPORT_TEMPLATE,
    add_gpu_job,
)

# "Today" for all recurring tests.
_TEST_END = datetime(2024, 6, 30, tzinfo=UTC)
_14D = timedelta(days=14)

# Default keyword args for build_recurring_table and get_recurring_underusers callers
# that don't need to exercise specific window/share values.
_BRT_KW = {"cluster_share_threshold": 0.30, "active_cycles": 3}
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

_MIN_WASTE_RATIO = 0.50
_MIN_WASTE_RGU_HOURS = 24

_NOTIFY_CFG = {
    "slack": {
        "description": "test channel",
        "token": "xoxb-test-token",
        "channel": "#test-channel",
    },
    "underusage_report_template": UNDERUSAGE_REPORT_TEMPLATE,
    "usage_report_template": USAGE_REPORT_TEMPLATE,
    "min_waste_ratio": _MIN_WASTE_RATIO,
    "min_waste_rgu_hours": _MIN_WASTE_RGU_HOURS,
    "digest_top_n": 16,
    "recurrence_cluster_share": 0.70,
    "personalized_action_min_waste_rgu_hours": 115.2,  # 24h * A100 80GB RGU
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

    Per-cycle floor (min_waste_rgu_hours=24): firstuser/seconduser/thirduser pass (≥24);
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
            min_waste_ratio=_MIN_WASTE_RATIO,
            min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
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
    # Keep the notifications overlay active across the assertions: the
    # restrictive_action_flags property reads restrictive_action_run_cycles from
    # config, which is only defined inside this overlay.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}) as config:
        ncfg = config.sarc.notifications
        result = get_recurring_underusers(
            _TEST_END,
            min_waste_ratio=_MIN_WASTE_RATIO,
            min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
            cluster_share_threshold=1.0,
            personalized_action_min_waste_rgu_hours=ncfg.personalized_action_min_waste_rgu_hours,
        )
        # Trigger cache on restrictive_action_flags
        for rows in result.values():
            for row in rows:
                row.restrictive_action_flags
    mila_emails = {r.email for r in result.get("mila", [])}
    assert "firstuser@mila.quebec" in mila_emails
    assert "seconduser@mila.quebec" in mila_emails
    assert "thirduser@mila.quebec" in mila_emails
    assert "fourthuser@mila.quebec" in mila_emails

    wastes = [r.wasted_current_active_window for r in result["mila"]]
    assert wastes == sorted(wastes, reverse=True)

    total_share = sum(r.cluster_share for r in result["mila"])
    assert abs(total_share - 1.0) < 1e-6

    # Test firstuser flagged all five cycles. pa_flags now spans all
    # recurrence_display_cycles (5) positions: the 3-cycle PA window ending at
    # position i clears the floor for i=0..3 (W-6+W-8+W-10 = 182.4 ≥ 115.2) but
    # not i=4 (W-8 alone = 91.2 < 115.2).
    row = next(r for r in result["mila"] if r.email == "firstuser@mila.quebec")
    assert row.cycles == [True] * 5
    assert row.pa_flags == [True, True, True, True, False]
    assert row.flagged_for_personalized_action is True
    # A sustained run of four ⚑ peaks (positions 0..3) escalates on the newest cell.
    assert row.restrictive_action_flags == [True, False, False, False, False]

    # Test seconduser only flagged for the first 3 cycles
    row = next(r for r in result["mila"] if r.email == "seconduser@mila.quebec")
    assert row.cycles == [True] * 3 + [False] * 2
    # PA clears the floor only at positions 0 (205.2) and 1 (136.8); no 4-run.
    assert row.pa_flags == [True, True, False, False, False]
    assert row.flagged_for_personalized_action is True
    assert row.restrictive_action_flags == [False] * 5

    # Test thirduser flagged all cycles except for 4th
    row = next(r for r in result["mila"] if r.email == "thirduser@mila.quebec")
    assert row.cycles == [True] * 3 + [False] * 1 + [True] * 1
    # PA clears the floor only at position 0 (136.8).
    assert row.pa_flags == [True, False, False, False, False]
    assert row.flagged_for_personalized_action is True
    assert row.restrictive_action_flags == [False] * 5

    # Test fourthuser only flagged for the last 2 cycles
    row = next(r for r in result["mila"] if r.email == "fourthuser@mila.quebec")
    assert row.cycles == [False] * 3 + [True] * 2
    # fourthuser is NOT a current-cycle underuser at positions 0..2 (cycles False),
    # so PA is suppressed there. At position 3 the PA window (W-6+W-8 = 136.8 ≥
    # 115.2) clears the floor AND they are a single-cycle underuser at W-6, so
    # pa_flags[3] is True. Position 4's window (W-8 alone = 68.4) is below the floor.
    assert row.pa_flags == [False, False, False, True, False]
    assert row.flagged_for_personalized_action is False
    assert row.restrictive_action_flags == [False] * 5


# ── Non-default cycle counts (acceptance criteria) ────────────────────────────


def test_display_cycles_columns(recurring_db):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_waste_ratio=_MIN_WASTE_RATIO,
            min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
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
            min_waste_ratio=_MIN_WASTE_RATIO,
            min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
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
                min_waste_ratio=_MIN_WASTE_RATIO,
                min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
                cluster_share_threshold=1.0,
                recurrence_active_cycles=active_cycles,
                personalized_action_min_waste_rgu_hours=300.0,
            )
            row = next(r for r in result["mila"] if r.email == "firstuser@mila.quebec")
            assert row.flagged_for_personalized_action is expected, (
                f"{expected=} for {active_cycles=} but got {row.flagged_for_personalized_action=}"
            )


# ── Empty / edge cases ────────────────────────────────────────────────────────


def test_empty_db_returns_empty_dict(read_write_db):
    before = datetime(2020, 1, 1, tzinfo=UTC)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            before,
            min_waste_ratio=_MIN_WASTE_RATIO,
            min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
            **_GRU_KW,
        )
    assert result == {}


# ── build_recurring_table ─────────────────────────────────────────────────────

_ROW_ALICE = RecurringUserRow(
    email="alice@mila.quebec",
    display_name="Alice Liddell",
    cluster="narval",
    wasted_current_active_window=4200.0,
    cluster_share=0.18,
    cycles=[True, True, True, True, True],
    flagged_for_personalized_action=True,
)

_ROW_BOB = RecurringUserRow(
    email="bob@mila.quebec",
    display_name="Bob Marley",
    cluster="narval",
    wasted_current_active_window=2600.0,
    cluster_share=0.11,
    cycles=[True, False, True, True, True],
    flagged_for_personalized_action=False,
)

_ROW_CAROL = RecurringUserRow(
    email="carol@mila.quebec",
    display_name="Carol Danvers",
    cluster="narval",
    wasted_current_active_window=1100.0,
    cluster_share=0.05,
    cycles=[True, True, False, False, False],
    flagged_for_personalized_action=False,
)


def test_table_header_contains_cluster():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "Cluster narval" in text


def test_table_header_contains_window_weeks():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table(
            {"narval": [_ROW_ALICE]}, cluster_share_threshold=0.30, active_cycles=3
        )
    assert "last 6 weeks" in text


def test_table_threshold_in_sub_header():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table(
            {"narval": [_ROW_ALICE]}, cluster_share_threshold=0.30, active_cycles=3
        )
    assert "30 %" in text


def test_table_contains_email():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "alice@mila.quebec" in text


def test_table_wasted_formatted_with_space_thousands():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "4 200" in text


def test_table_share_percentage():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_BOB]}, **_BRT_KW)
    assert "11 %" in text


def test_table_has_no_action_column():
    # The Action column was removed; personalized action is now conveyed per-cycle
    # via the ⚑ peak marker, not a trailing summary column.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "Action" not in text
    assert "⚑ personalized" not in text


def test_table_check_and_cross_marks():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_BOB]}, **_BRT_KW)
    assert "✓" in text
    assert "▲" in text


def test_table_tree_chars_multiple_rows():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table(
            {"narval": [_ROW_ALICE, _ROW_BOB, _ROW_CAROL]}, **_BRT_KW
        )
    assert "┌─" in text
    assert "├─" in text
    assert "└─" in text


def test_table_single_row_uses_end_cap():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
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
        flagged_for_personalized_action=False,
    )
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
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
        flagged_for_personalized_action=False,
    )
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table(
            {"narval": [_ROW_ALICE], "fir": [carol_fir]}, **_BRT_KW
        )
    assert text.index("Cluster fir") < text.index("Cluster narval")


def test_table_empty_dict_returns_empty_string():
    assert build_recurring_table({}, **_BRT_KW) == ""


def test_table_deterministic():
    data = {"narval": [_ROW_ALICE, _ROW_BOB]}
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        first = build_recurring_table(data, **_BRT_KW)
        second = build_recurring_table(data, **_BRT_KW)
    assert first == second


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


# ── _week_anchor ──────────────────────────────────────────────────────────────
# "aligned" = ISO week number is a multiple of usage_cycle_length_weeks;
# "off-cycle" = it is not.

# 2024-06-24: Monday of ISO week 26 (aligned)
_ALIGNED_MON = datetime(2024, 6, 24, tzinfo=UTC)
# 2024-06-30: Sunday of ISO week 26 (aligned) — _TEST_END
_ALIGNED_SUN = _TEST_END
# 2024-06-26: Wednesday of ISO week 26 (aligned)
_ALIGNED_WED = datetime(2024, 6, 26, tzinfo=UTC)
# 2024-06-17: Monday of ISO week 25 (off-cycle)
_OFF_CYCLE_MON = datetime(2024, 6, 17, tzinfo=UTC)
# 2024-06-19: Wednesday of ISO week 25 (off-cycle)
_OFF_CYCLE_WED = datetime(2024, 6, 19, tzinfo=UTC)


def test_anchor_aligned_monday_returns_same_day():
    # 2024-06-24 is already the Monday of an aligned week
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        anchor = _week_anchor(_ALIGNED_MON)
    assert anchor == _ALIGNED_MON.replace(hour=0, minute=0, second=0, microsecond=0)


def test_anchor_aligned_midweek_returns_same_day():
    # 2024-06-26 (Wed, wk 26 aligned) → anchor = same day
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        anchor = _week_anchor(_ALIGNED_WED)
    assert anchor == _ALIGNED_WED.replace(hour=0, minute=0, second=0, microsecond=0)


def test_anchor_aligned_sunday_returns_same_day():
    # 2024-06-30 (Sun, wk 26 aligned) → anchor = same day
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        anchor = _week_anchor(_ALIGNED_SUN)
    assert anchor == _ALIGNED_SUN.replace(hour=0, minute=0, second=0, microsecond=0)


def test_anchor_off_cycle_monday_shifts_to_aligned_week():
    # 2024-06-17 (Mon, wk 25 off-cycle) → anchor = 2024-06-24 (Mon, wk 26 aligned)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        anchor = _week_anchor(_OFF_CYCLE_MON)
    assert anchor == _ALIGNED_MON.replace(hour=0, minute=0, second=0, microsecond=0)


def test_anchor_off_cycle_midweek_shifts_to_aligned_week():
    # 2024-06-19 (Wed, wk 25 off-cycle) → anchor = 2024-06-26 (Wed, wk 26 aligned)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        anchor = _week_anchor(_OFF_CYCLE_WED)
    assert anchor == (_OFF_CYCLE_WED + timedelta(weeks=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def test_anchor_result_always_aligned_week():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        for offset in range(28):
            dt = datetime(2024, 6, 1, tzinfo=UTC) + timedelta(days=offset)
            anchor = _week_anchor(dt)
            assert anchor.isocalendar().week % usage_cycle_length_weeks() == 0, (
                f"off-cycle week for end={dt.date()}"
            )


# ── get_cycle_dates ───────────────────────────────────────────────────────────


def test_cycle_dates_aligned_week_five_mondays():
    # end = 2024-06-24 (Mon, wk 26 aligned)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        dates = get_cycle_dates(_ALIGNED_MON)
        assert len(dates) == 5
        assert all(isinstance(d, date) for d in dates)
        # Each must be a Monday of an aligned week, usage_cycle_length_weeks weeks apart
        for i, d in enumerate(dates):
            dt = datetime(d.year, d.month, d.day, tzinfo=UTC)
            assert dt.weekday() == 0, f"dates[{i}] is not a Monday"
            assert dt.isocalendar().week % usage_cycle_length_weeks() == 0, (
                f"dates[{i}] not an aligned week"
            )
        for i in range(len(dates) - 1):
            assert (dates[i] - dates[i + 1]).days == usage_cycle_length_weeks() * 7


def test_cycle_dates_aligned_week_all_not_future():
    # end = 2024-06-24 (Mon, wk 26 aligned); all cycle dates ≤ end.date()
    end = _ALIGNED_MON
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        dates = get_cycle_dates(end)
    for d in dates:
        assert d <= end.date()


def test_cycle_dates_off_cycle_week_w0_is_future():
    # end = 2024-06-17 (Mon, wk 25 off-cycle); W0 anchor = 2024-06-24 > end
    end = _OFF_CYCLE_MON
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        dates = get_cycle_dates(end)
    assert dates[0] > end.date(), "W0 should be in the future for off-cycle-week end"
    for d in dates[1:]:
        assert d < end.date(), f"{d} should be in the past"


# ── non-default usage_cycle_length_weeks (N) ──────────────────────────────────
# Properties are computed from N rather than hardcoded, so a regression that
# ignores the config value (e.g. a hardcoded 2) fails these at N=1 and N=3.


@pytest.mark.parametrize("cycle_weeks", [1, 2, 3])
def test_anchor_always_aligned_for_n(cycle_weeks):
    cfg = {**_NOTIFY_CFG, "usage_cycle_length_weeks": cycle_weeks}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        assert usage_cycle_length_weeks() == cycle_weeks
        for offset in range(35):  # 5-week mid-year sweep
            dt = datetime(2024, 6, 1, tzinfo=UTC) + timedelta(days=offset)
            anchor = _week_anchor(dt)
            assert anchor.isocalendar().week % cycle_weeks == 0, (
                f"off-cycle week for end={dt.date()} at N={cycle_weeks}"
            )
            assert anchor >= dt
            assert anchor - dt < timedelta(weeks=cycle_weeks), (
                "shift should be the minimal forward move to an aligned week"
            )
            if cycle_weeks == 1:
                assert anchor == dt, "every week is aligned when N=1 (identity)"


@pytest.mark.parametrize("cycle_weeks", [1, 2, 3])
def test_cycle_dates_spacing_and_alignment_for_n(cycle_weeks):
    cfg = {**_NOTIFY_CFG, "usage_cycle_length_weeks": cycle_weeks}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        end = _ALIGNED_MON  # 2024-06-24, Mon of ISO week 26
        dates = get_cycle_dates(end, n=5)
        assert len(dates) == 5
        for d in dates:
            dt = datetime(d.year, d.month, d.day, tzinfo=UTC)
            assert dt.isocalendar().week % cycle_weeks == 0, (
                f"{d} not an aligned week at N={cycle_weeks}"
            )
        for i in range(len(dates) - 1):
            assert (dates[i] - dates[i + 1]).days == cycle_weeks * 7
        assert dates[0] == _week_anchor(end).date(), (
            "W0 anchored to current-or-next aligned week"
        )


# ── _week_anchor — pinned year-boundary / week-53 limitation ──────────────────
# See the docstring on _week_anchor (sarc/notifications/underusage.py): the
# naive `(N - remainder) % N` shift is computed from the *original* date's ISO
# week number, so when the shift crosses an ISO year boundary the resulting
# date can land on a week number that is itself not a multiple of N. These
# pin that current (out-of-scope-to-fix) behavior as an exact regression
# check, not a spec to preserve.


@pytest.mark.parametrize(
    "cycle_weeks, end, expected_anchor",
    [
        # 2020-12-28 is ISO week 53 of 2020 (off-cycle for N=2). The naive
        # shift lands on 2021-01-04, ISO week 1 of 2021 — itself off-cycle
        # (1 % 2 != 0), because the shift ignores the year-boundary rollover.
        (2, datetime(2020, 12, 28, tzinfo=UTC), datetime(2021, 1, 4, tzinfo=UTC)),
        # 2024-12-23 is ISO week 52 of 2024 (off-cycle for N=3). The naive
        # shift lands on 2025-01-06, ISO week 2 of 2025 — itself off-cycle
        # (2 % 3 != 0), same year-boundary limitation as above.
        (3, datetime(2024, 12, 23, tzinfo=UTC), datetime(2025, 1, 6, tzinfo=UTC)),
    ],
)
def test_anchor_year_boundary_pinned(cycle_weeks, end, expected_anchor):
    cfg = {**_NOTIFY_CFG, "usage_cycle_length_weeks": cycle_weeks}
    with gifnoc.overlay({"sarc.notifications": cfg}):
        anchor = _week_anchor(end)
        assert anchor == expected_anchor
        assert anchor.isocalendar().week % cycle_weeks != 0, (
            "pinned: rollover misaligns the anchor"
        )


# ── get_recurring_underusers — off-cycle-week end ─────────────────────────────


def test_off_cycle_week_end_w0_is_none(recurring_db):
    # _OFF_CYCLE_MON = 2024-06-17 (wk 25, off-cycle) → anchor = 2024-06-24 > end
    # → w0=None
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _OFF_CYCLE_MON,
            min_waste_ratio=_MIN_WASTE_RATIO,
            min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
            cluster_share_threshold=0.70,
        )
    assert result, "expected selected users for the off-cycle-week window"
    for rows in result.values():
        for row in rows:
            assert row.cycles[0] is None, (
                f"expected cycles[0]=None for off-cycle-week end, got {row.cycles[0]}"
            )


def test_off_cycle_week_end_personalized_action_floor_controls(recurring_db):
    # With w0=None (off-cycle-week end) position-0 PA is suppressed regardless of waste,
    # because cycle_flagged[0] is None. Here a high floor independently keeps everyone
    # below the threshold, so all rows are unflagged.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _OFF_CYCLE_MON,
            min_waste_ratio=_MIN_WASTE_RATIO,
            min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
            cluster_share_threshold=0.70,
            personalized_action_min_waste_rgu_hours=999999.0,
        )
    for rows in result.values():
        for row in rows:
            assert row.flagged_for_personalized_action is False


# ── build_recurring_table with cycle_dates ────────────────────────────────────

_CYCLE_DATES = [
    date(2024, 6, 24),  # W0  (Mon, wk 26 aligned)
    date(2024, 6, 10),  # W-2 (Mon, wk 24 aligned)
    date(2024, 5, 27),  # W-4 (Mon, wk 22 aligned)
    date(2024, 5, 13),  # W-6 (Mon, wk 20 aligned)
    date(2024, 4, 29),  # W-8 (Mon, wk 18 aligned)
]

_ROW_FUTURE_W0 = RecurringUserRow(
    email="alice@mila.quebec",
    display_name="Alice Liddell",
    cluster="narval",
    wasted_current_active_window=4200.0,
    cluster_share=0.18,
    cycles=[None, True, True, False, False],
    flagged_for_personalized_action=False,
)


def test_table_with_cycle_dates_renders_mm_dd_headers():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table(
            {"narval": [_ROW_ALICE]}, cycle_dates=_CYCLE_DATES, **_BRT_KW
        )
    assert "06-24" in text
    assert "06-10" in text
    assert "05-27" in text
    assert "05-13" in text
    assert "04-29" in text


def test_table_with_cycle_dates_no_w0_label():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table(
            {"narval": [_ROW_ALICE]}, cycle_dates=_CYCLE_DATES, **_BRT_KW
        )
    assert "W0" not in text
    assert "W-2" not in text


def test_table_none_flag_renders_blank_not_cross(capsys):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table(
            {"narval": [_ROW_FUTURE_W0]}, cycle_dates=_CYCLE_DATES, **_BRT_KW
        )
    # _ROW_FUTURE_W0: w0=None→blank, w2=True→✗, w4=True→✗, w6=False→✓, w8=False→✓
    # True (flagged/underuser) → ✗; False (good usage) → ✓; None (future) → blank
    assert text.count("✓") == 2  # w6 and w8
    assert text.count("▲") == 2  # w2 and w4


def test_table_without_cycle_dates_keeps_w0_label():
    # Backward compat: no cycle_dates → "W0"/"W-2" labels still present
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "W0" in text
    assert "W-2" in text


# ── | separator ──────────────────────────────────────────────────────────────


def test_table_contains_separator():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_ALICE]}, **_BRT_KW)
    assert "|" in text


def test_table_separator_between_w4_and_w6():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
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
    flagged_for_personalized_action=False,
    pa_flags=[False, False, True, False, False],
)

# Row where the first 3 positions are pa-flagged → ⚑ at W0, W-2, W-4 (no 4-run).
_ROW_ALL_TRUE = RecurringUserRow(
    email="eve@mila.quebec",
    display_name="Eve Online",
    cluster="narval",
    wasted_current_active_window=5000.0,
    cluster_share=0.40,
    cycles=[True, True, True, True, True],
    flagged_for_personalized_action=True,
    pa_flags=[True, True, True, False, False],
)

# Row with a ⚑ peak on a display-only cycle (position 3, right of the "|").
_ROW_PEAK_AT_W6 = RecurringUserRow(
    email="gwen@mila.quebec",
    display_name="Gwen Stacy",
    cluster="narval",
    wasted_current_active_window=2000.0,
    cluster_share=0.15,
    cycles=[False, False, False, True, False],
    flagged_for_personalized_action=False,
    pa_flags=[False, False, False, True, False],
)


def test_per_cycle_peak_at_w4():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_PEAK_AT_W4]}, **_BRT_KW)
    assert "⚑▲" in text


def test_per_cycle_no_peak_at_w0_w2():
    # pa_flags[0]=False and pa_flags[1]=False → no ⚑ at W0 or W-2
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_PEAK_AT_W4]}, **_BRT_KW)
    assert text.count("⚑▲") == 1


def test_per_cycle_all_active_flagged():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_ALL_TRUE]}, **_BRT_KW)
    # pa_flags=[True,True,True] → ⚑ at W0, W-2, W-4
    assert text.count("⚑▲") == 3


def test_per_cycle_display_only_cycle_shows_peak():
    # A ⚑ peak on a display-only cycle (position ≥ active_cycles, right of "|") is
    # now rendered — previously these were suppressed.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_PEAK_AT_W6]}, **_BRT_KW)
    assert text.count("⚑▲") == 1
    # The peak sits after the active/history separator.
    assert text.index("|") < text.index("⚑▲")


def test_per_cycle_no_peak_on_passing_cell():
    # ⚑ never renders on a ✓ cell even when pa_flags[i] is True
    row = RecurringUserRow(
        email="frank@mila.quebec",
        display_name="Frank Test",
        cluster="narval",
        wasted_current_active_window=1000.0,
        cluster_share=0.10,
        cycles=[False, True, True, True, True],
        flagged_for_personalized_action=True,
        pa_flags=[True, True, True, False, False],
    )
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [row]}, **_BRT_KW)
    # W0 is ✓ (cycles[0]=False) so no ⚑ there; W-2 and W-4 are ▲ with pa_flags → 2 ⚑▲
    assert text.count("⚑▲") == 2


# ── Restrictive-action escalation (⚑ run → !!⚑▲) ─────────────────────────────


def _row_with_pa_flags(pa_flags: list[bool]) -> RecurringUserRow:
    """Minimal row for exercising restrictive_action_flags in isolation."""
    return RecurringUserRow(
        email="x@mila.quebec",
        display_name="X",
        cluster="mila",
        wasted_current_active_window=1.0,
        cluster_share=0.1,
        cycles=[bool(f) for f in pa_flags],
        flagged_for_personalized_action=bool(pa_flags[:1] == [True]),
        pa_flags=list(pa_flags),
    )


@pytest.mark.parametrize(
    "pa_flags, expected",
    [
        ([], []),
        ([True, True, True], [False, False, False]),  # run shorter than 4
        ([True, True, True, True], [True, False, False, False]),  # exact 4-run
        ([True, True, True, False], [False, False, False, False]),
        # 5-run → the two newest cells each start a full 4-run.
        ([True, True, True, True, True], [True, True, False, False, False]),
        # Run not anchored at index 0: flag lands on the newest cell of the run.
        ([False, True, True, True, True], [False, True, False, False, False]),
        # A gap breaks the run; the later 4-run flags its own newest cell.
        (
            [True, True, False, True, True, True, True],
            [False, False, False, True, False, False, False],
        ),
    ],
)
def test_restrictive_action_flags(pa_flags, expected):
    # restrictive_action_flags reads the run length from config; the default
    # (4) applies inside this overlay since _NOTIFY_CFG doesn't override it.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        assert _row_with_pa_flags(pa_flags).restrictive_action_flags == expected


def test_restrictive_action_run_cycles_config_override():
    # A 2-cycle ⚑ run: no escalation under the default run length (4), but
    # escalates once restrictive_action_run_cycles is lowered to 2.
    pa_flags = [True, True, False, False, False]
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        assert _row_with_pa_flags(pa_flags).restrictive_action_flags == [False] * 5
    with gifnoc.overlay(
        {"sarc.notifications": {**_NOTIFY_CFG, "restrictive_action_run_cycles": 2}}
    ):
        assert _row_with_pa_flags(pa_flags).restrictive_action_flags == [
            True,
            False,
            False,
            False,
            False,
        ]


def test_table_escalation_marker_on_four_run():
    # pa_flags peaks over 4 consecutive cycles (0..3) → !!⚑▲ on the newest cell.
    row = RecurringUserRow(
        email="hugo@mila.quebec",
        display_name="Hugo Weaving",
        cluster="narval",
        wasted_current_active_window=6000.0,
        cluster_share=0.45,
        cycles=[True, True, True, True, True],
        flagged_for_personalized_action=True,
        pa_flags=[True, True, True, True, False],
    )
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [row]}, **_BRT_KW)
    assert "!!⚑▲" in text
    assert text.count("!!⚑▲") == 1


def test_table_escalation_two_cells_on_five_run():
    # A 5-cycle peak run escalates the two newest cells.
    row = RecurringUserRow(
        email="ivy@mila.quebec",
        display_name="Ivy Green",
        cluster="narval",
        wasted_current_active_window=7000.0,
        cluster_share=0.50,
        cycles=[True, True, True, True, True],
        flagged_for_personalized_action=True,
        pa_flags=[True, True, True, True, True],
    )
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [row]}, **_BRT_KW)
    assert text.count("!!⚑▲") == 2


def test_table_no_escalation_without_four_run():
    # Only 3 consecutive peaks → no escalation marker.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table({"narval": [_ROW_ALL_TRUE]}, **_BRT_KW)
    assert "!!" not in text


# ── Threshold scaling, true_wasted, personalized_action floor ────────────────


def test_wasted_6w_uses_scaled_waste(recurring_db):
    # firstuser: util=0.05, threshold=0.05 → credited_used=LEAST(rgu_h, rgu_h*1.0)=rgu_h
    # → wasted=0 → excluded from the recurring table entirely
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_waste_ratio=_MIN_WASTE_RATIO,
            min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
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
            min_waste_ratio=_MIN_WASTE_RATIO,
            min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
            cluster_share_threshold=1.0,
        )
    row = next(r for r in result["mila"] if r.email == "firstuser@mila.quebec")
    assert row.true_wasted > 0.0


def test_personalized_action_floor(recurring_db):
    # With floor=0.0 the waste floor is met by every user, so PA reduces to the
    # current-cycle-underuse requirement: a user is flagged iff they are present in
    # the most-recent cycle (cycle_flagged[0]). fourthuser clears the floor over the
    # active window but is NOT a current-cycle underuser (cycles == [False]*3 +
    # [True]*2), so PA is suppressed for them while the other three are flagged.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_waste_ratio=_MIN_WASTE_RATIO,
            min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
            cluster_share_threshold=1.0,
            personalized_action_min_waste_rgu_hours=0.0,
        )
    flagged = {
        r.email: r.flagged_for_personalized_action
        for rows in result.values()
        for r in rows
    }
    assert flagged["firstuser@mila.quebec"] is True
    assert flagged["seconduser@mila.quebec"] is True
    assert flagged["thirduser@mila.quebec"] is True
    # Discriminating case for the per-cycle gating: above the floor but absent from
    # the most-recent cycle → not flagged.
    assert flagged["fourthuser@mila.quebec"] is False

    # With a very high floor, no user crosses the threshold → all False.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_waste_ratio=_MIN_WASTE_RATIO,
            min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
            cluster_share_threshold=1.0,
            personalized_action_min_waste_rgu_hours=999999.0,
        )
    for rows in result.values():
        for row in rows:
            assert row.flagged_for_personalized_action is False


# ── Per-anchor pa_flags four-scenario computation test ────────────────────────
# RGU=4.8, util=0.0 → wasted = 4.8 * elapsed_h. elapsed_h = target_rgu_h / 4.8.
# Scenarios (active_cycles=3, display_cycles=5, cycle_length=2w, PA threshold=30);
# pa_flags now spans all 5 displayed positions:
#   user1: 10 RGU-h/cycle × 5 → windows pos0..2 = 30 ≥ 30, pos3 = 20, pos4 = 10
#          → pa_flags=[T,T,T,F,F]
#   user2: W0=15,W-2=10,W-4=5,W-6=5,W-8=5
#          pos0=30≥30, pos1=20, pos2=15, pos3=10, pos4=5 → pa_flags=[T,F,F,F,F]
#   user3: W0=30 only → pos0=30, pos1..4=0 → pa_flags=[T,F,F,F,F]
#   user4: 5 RGU-h/cycle × 5 → every window ≤ 15 < 30 → pa_flags=[F,F,F,F,F]


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
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_waste_ratio=0.0,
            min_waste_rgu_hours=0.0,
            cluster_share_threshold=1.1,
            recurrence_active_cycles=3,
            recurrence_display_cycles=5,
            clusters=["mila"],
            personalized_action_min_waste_rgu_hours=30.0,
        )
    rows = {r.email: r for r in result.get("mila", [])}

    u1 = rows[users["u1"].email]
    u2 = rows[users["u2"].email]
    u3 = rows[users["u3"].email]
    u4 = rows[users["u4"].email]

    assert u1.pa_flags == [True, True, True, False, False]
    assert u1.flagged_for_personalized_action is True

    assert u2.pa_flags == [True, False, False, False, False]
    assert u2.flagged_for_personalized_action is True

    assert u3.pa_flags == [True, False, False, False, False]
    assert u3.flagged_for_personalized_action is True

    assert u4.pa_flags == [False, False, False, False, False]
    assert u4.flagged_for_personalized_action is False

    # None of the scenarios has a four-cycle ⚑ run → no restrictive-action escalation.
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        for u in (u1, u2, u3, u4):
            assert u.restrictive_action_flags == [False] * 5
