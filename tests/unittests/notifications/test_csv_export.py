"""Tests for the recurring-underusers CSV exports."""

import csv
import io
from datetime import date

import gifnoc

from sarc.notifications.csv_export import _dashboard_link, build_recurring_distilled_csv
from sarc.notifications.usage import RecurringUserRow
from tests.unittests.notifications._factory import (
    UNDERUSAGE_REPORT_TEMPLATE,
    USAGE_REPORT_TEMPLATE,
)

_DASHBOARD_URL = "https://sarc-api.example.com/dash/metrics"
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
    "dashboard_url": _DASHBOARD_URL,
}

_CYCLE_DATES = [
    date(2024, 6, 24),
    date(2024, 6, 10),
    date(2024, 5, 27),
    date(2024, 5, 13),
    date(2024, 4, 29),
]

_WINDOW_START = date(2024, 5, 27)
_WINDOW_END = date(2024, 6, 24)

_ROW_ALICE = RecurringUserRow(
    email="alice@mila.quebec",
    display_name="Alice Liddell",
    cluster="narval",
    wasted_current_active_window=4200.0,
    cluster_share=0.18,
    cycles=[True, True, True, True, True],
    flagged_for_personalized_action=True,
    pa_flags=[True, True, True, True, False],
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

_ROW_FUTURE = RecurringUserRow(
    email="carol@mila.quebec",
    display_name="Carol Danvers",
    cluster="narval",
    wasted_current_active_window=1100.0,
    cluster_share=0.05,
    cycles=[None, True, True, False, False],
    flagged_for_personalized_action=False,
)


def _rows(text: str) -> list[list[str]]:
    return list(csv.reader(io.StringIO(text)))


# ── _dashboard_link ────────────────────────────────────────────────────────────


def test_dashboard_link_none_url_returns_empty():
    assert _dashboard_link(None, "alice@mila.quebec", _WINDOW_START, _WINDOW_END) == ""


def test_dashboard_link_urlencodes_email():
    link = _dashboard_link(
        _DASHBOARD_URL, "alice+test@mila.quebec", _WINDOW_START, _WINDOW_END
    )
    assert "alice%2Btest%40mila.quebec" in link
    assert "+test@mila.quebec" not in link


def test_dashboard_link_has_start_end_dates():
    link = _dashboard_link(
        _DASHBOARD_URL, "alice@mila.quebec", _WINDOW_START, _WINDOW_END
    )
    assert "start=2024-05-27" in link
    assert "end=2024-06-24" in link
    assert link.startswith(_DASHBOARD_URL + "?")


# ── build_recurring_distilled_csv ──────────────────────────────────────────────


def test_distilled_empty_recurring_returns_empty_string():
    assert (
        build_recurring_distilled_csv(
            {},
            active_cycles=3,
            cycle_dates=_CYCLE_DATES,
            dashboard_url=_DASHBOARD_URL,
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )
        == ""
    )


def test_distilled_header_shape_and_separator_position():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_distilled_csv(
            {"narval": [_ROW_ALICE]},
            active_cycles=3,
            cycle_dates=_CYCLE_DATES,
            dashboard_url=_DASHBOARD_URL,
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )
    header = _rows(text)[0]
    assert header[:5] == [
        "cluster",
        "User",
        "user dashboard url",
        "Unused RGU-h",
        "Share",
    ]
    # 3 active-cycle date columns, then a blank separator, then the remaining 2.
    assert header[5:8] == ["06-24", "06-10", "05-27"]
    assert header[8] == ""
    assert header[9:11] == ["05-13", "04-29"]


def test_distilled_row_has_raw_numbers_and_markers():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_distilled_csv(
            {"narval": [_ROW_ALICE]},
            active_cycles=3,
            cycle_dates=_CYCLE_DATES,
            dashboard_url=_DASHBOARD_URL,
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )
    row = _rows(text)[1]
    assert row[0] == "narval"
    assert row[1] == "alice@mila.quebec"
    assert row[2].startswith(_DASHBOARD_URL + "?")
    assert row[3] == "4200.0"  # raw float, not "4 200"
    assert row[4] == "0.18"  # raw fraction, not "18 %"
    # cycles=[T,T,T,T,T], pa_flags=[T,T,T,T,F] -> 4-run at 0..3 (default run=4)
    # escalates the newest cell; position 4 has no pa_flags peak.
    assert row[5] == "!!⚑▲"
    assert row[6] == "⚑▲"
    assert row[7] == "⚑▲"
    assert row[8] == ""  # separator
    assert row[9] == "⚑▲"
    assert row[10] == "▲"


def test_distilled_future_cycle_and_not_flagged_markers():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_distilled_csv(
            {"narval": [_ROW_FUTURE]},
            active_cycles=3,
            cycle_dates=_CYCLE_DATES,
            dashboard_url=_DASHBOARD_URL,
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )
    row = _rows(text)[1]
    # cycles=[None, True, True, False, False]
    assert row[5] == ""  # future cycle blank
    assert row[6] == "▲"
    assert row[7] == "▲"
    assert row[8] == ""  # separator
    assert row[9] == "✓"
    assert row[10] == "✓"


def test_distilled_multi_cluster_sorted_alphabetically():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_distilled_csv(
            {"zeta": [_ROW_BOB], "alpha": [_ROW_ALICE]},
            active_cycles=3,
            cycle_dates=_CYCLE_DATES,
            dashboard_url=_DASHBOARD_URL,
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )
    rows = _rows(text)
    assert rows[1][0] == "alpha"
    assert rows[2][0] == "zeta"


def test_distilled_dashboard_url_none_leaves_link_blank():
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_distilled_csv(
            {"narval": [_ROW_ALICE]},
            active_cycles=3,
            cycle_dates=_CYCLE_DATES,
            dashboard_url=None,
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )
    row = _rows(text)[1]
    assert row[2] == ""
