from datetime import UTC, datetime

import gifnoc
import pytest

from sarc.config import ConfigurationError
from sarc.notifications.messages import (
    _date_range,
    _fmt_rgu,
    _jobs_section,
    _pct,
    build_admin_digest,
    build_recurring_table,
    build_usage_report,
    build_user_dm,
)
from sarc.notifications.slack import MENTION_TOKEN
from sarc.notifications.usage import (
    RecurringUserRow,
    UsageClusterBreakdown,
    UsageJob,
    UsageRow,
)
from tests.unittests.notifications._factory import (
    UNDERUSAGE_REPORT_TEMPLATE,
    USAGE_REPORT_TEMPLATE,
)

_BASE_NOTIFY_CFG = {
    "slack_underusage": {
        "description": "test",
        "token": "xoxb-test",
        "channel": "#test",
    },
    "slack_usage": {"description": "test", "token": "xoxb-test", "channel": "#test"},
    "underusage_report_template": UNDERUSAGE_REPORT_TEMPLATE,
    "usage_report_template": USAGE_REPORT_TEMPLATE,
    "dashboard_url": "https://dash.example.com",
}


def _notify_overlay(**overrides):
    """gifnoc.overlay for sarc.notifications with the real repo templates."""
    return gifnoc.overlay({"sarc.notifications": {**_BASE_NOTIFY_CFG, **overrides}})


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
    "enabled": False,
    "underusage_report_template": "",
    "usage_report_template": "",
    "dashboard_url": "https://dash.example.com",
}


# ── build_usage_report fixtures ───────────────────────────────────────────────

_USAGE_JOB_NARVAL_1 = UsageJob(
    job_id=300001,
    cluster="narval",
    submit_time=datetime(2026, 5, 28, tzinfo=UTC),
    wasted=None,
    rgu_hours_used=120.0,
    gpu_sm_occupancy=0.72,
)
_USAGE_JOB_NARVAL_2 = UsageJob(
    job_id=300002,
    cluster="narval",
    submit_time=datetime(2026, 6, 1, tzinfo=UTC),
    wasted=None,
    rgu_hours_used=90.0,
    gpu_sm_occupancy=0.65,
)
_USAGE_JOB_FIR = UsageJob(
    job_id=300003,
    cluster="fir",
    submit_time=datetime(2026, 5, 30, tzinfo=UTC),
    wasted=None,
    rgu_hours_used=50.0,
    gpu_sm_occupancy=None,
)

_USAGE_JOB_NARVAL_3 = UsageJob(
    job_id=300004,
    cluster="narval",
    submit_time=datetime(2026, 6, 2, tzinfo=UTC),
    wasted=80.0,
    rgu_hours_used=10.0,
    gpu_sm_occupancy=0.05,
)
_USAGE_JOB_FIR_2 = UsageJob(
    job_id=300005,
    cluster="fir",
    submit_time=datetime(2026, 5, 31, tzinfo=UTC),
    wasted=40.0,
    rgu_hours_used=5.0,
    gpu_sm_occupancy=0.02,
)

_USAGE_ROW_ALICE = UsageRow(
    email="alice@mila.quebec",
    display_name="Alice Liddell",
    user_id=1,
    rgu_hours=1000.0,
    rgu_hours_used=745.0,
    by_cluster=[
        UsageClusterBreakdown("narval", 700.0, 525.0, 700.0 - 525.0),
        UsageClusterBreakdown("fir", 300.0, 220.0, 300.0 - 220.0),
    ],
    top_jobs=[_USAGE_JOB_NARVAL_1, _USAGE_JOB_NARVAL_2, _USAGE_JOB_FIR],
    bottom_jobs=[_USAGE_JOB_NARVAL_3, _USAGE_JOB_FIR_2],
)

_USAGE_ROW_BOB = UsageRow(
    email="bob@mila.quebec",
    display_name="Bob Marley",
    user_id=2,
    rgu_hours=800.0,
    rgu_hours_used=200.0,
    by_cluster=[UsageClusterBreakdown("fir", 800.0, 200.0, 800.0 - 200.0)],
    top_jobs=[],
)

_WINDOW_START = datetime(2026, 1, 1, tzinfo=UTC)
_WINDOW_END = datetime(2026, 2, 1, tzinfo=UTC)


def _build_usage_report(row, *, window_weeks):
    return build_usage_report(
        row,
        window_weeks=window_weeks,
        window_start=_WINDOW_START,
        window_end=_WINDOW_END,
    )


# ── build_usage_report ────────────────────────────────────────────────────────


def test_usage_report():
    window_weeks = 4
    with _notify_overlay(dashboard_url="https://dash.example.com") as config:
        rgu_unit = config.sarc.notifications.rgu_unit
        user_unit = config.sarc.notifications.user_unit
        text = _build_usage_report(_USAGE_ROW_ALICE, window_weeks=window_weeks)
    # Ensure no formatting braces {} remain
    assert "{" not in text
    assert "}" not in text
    # Test overview contains the mention token, ready for dm_user to substitute
    assert MENTION_TOKEN in text
    # Test overview contains window weeks
    assert f"{window_weeks}" in text
    # Test overview contains the real date range
    assert _date_range(_WINDOW_START, _WINDOW_END) in text
    # Test overview contains rgu used
    assert _fmt_rgu(_USAGE_ROW_ALICE.rgu_hours, rgu_unit.hours_per_unit) in text
    # Test overview contains rgu long unit
    assert rgu_unit.unit_long in text
    # Test overview contains user friendly used
    assert _fmt_rgu(_USAGE_ROW_ALICE.rgu_hours, user_unit.hours_per_unit) in text
    # Test overview contains user friendly long unit
    assert user_unit.unit_long in text
    # Test overview contains utilization
    assert _pct(_USAGE_ROW_ALICE.avg_utilization) in text
    # Test overview contains top jobs count
    assert f"top_jobs_count:{len(_USAGE_ROW_ALICE.top_jobs)}" in text
    # Test top jobs grouped by cluster
    # narval has more total usage than fir → appears first
    assert text.index("Cluster narval") < text.index("Cluster fir")
    # Test job line format
    assert (
        _jobs_section(
            _USAGE_ROW_ALICE.top_jobs,
            rgu_value=lambda j: j.rgu_hours_used,
            hours_per_unit=rgu_unit.hours_per_unit,
            suffix=rgu_unit.unit,
        )
        in text
    )
    # Test overview contains bottom (least efficient) jobs count
    assert f"bottom_jobs_count:{len(_USAGE_ROW_ALICE.bottom_jobs)}" in text
    # Test bottom job line format
    assert (
        _jobs_section(
            _USAGE_ROW_ALICE.bottom_jobs,
            rgu_value=lambda j: j.wasted,
            hours_per_unit=rgu_unit.hours_per_unit,
            suffix=f"{rgu_unit.unit} unused",
        )
        in text
    )
    # Test overview contains dashboard_url with the report window as query params
    assert (
        "https://dash.example.com?start=2026-01-01&end=2026-02-01"
        "&utm_source=slack&utm_medium=notification"
        "&utm_campaign=usage_report&utm_content=dashboard" in text
    )


def test_usage_report_raises_without_notifications_config():
    with pytest.raises(ConfigurationError, match="No notifications configuration"):
        _build_usage_report(_USAGE_ROW_ALICE, window_weeks=2)


# ── Fixtures ──────────────────────────────────────────────────────────────────

_JOB_NARVAL_1 = UsageJob(
    job_id=111111,
    cluster="narval",
    submit_time=datetime(2026, 5, 28, tzinfo=UTC),
    wasted=80.0,
    rgu_hours_used=None,
    gpu_sm_occupancy=0.05,
)
_JOB_NARVAL_2 = UsageJob(
    job_id=111112,
    cluster="narval",
    submit_time=datetime(2026, 5, 30, tzinfo=UTC),
    wasted=60.0,
    rgu_hours_used=None,
    gpu_sm_occupancy=0.12,
)
_JOB_FIR = UsageJob(
    job_id=222222,
    cluster="fir",
    submit_time=datetime(2026, 5, 31, tzinfo=UTC),
    wasted=40.0,
    rgu_hours_used=None,
    gpu_sm_occupancy=None,
)

_ROW_ALICE = UsageRow(
    email="alice@mila.quebec",
    display_name="Alice Liddell",
    user_id=1,
    rgu_hours=1000.0,
    wasted=255.0,
    by_cluster=[
        UsageClusterBreakdown("narval", 700.0, 490.0, 700.0 - 490.0),
        UsageClusterBreakdown("fir", 300.0, 255.0, 300.0 - 255.0),
    ],
    top_jobs=[_JOB_NARVAL_1, _JOB_NARVAL_2, _JOB_FIR],
    bottom_jobs=[_JOB_NARVAL_1, _JOB_NARVAL_2, _JOB_FIR],
)

_ROW_BOB = UsageRow(
    email="bob@mila.quebec",
    display_name="Bob Marley",
    user_id=2,
    rgu_hours=800.0,
    wasted=600.0,
    by_cluster=[UsageClusterBreakdown("fir", 800.0, 200.0, 800.0 - 200.0)],
    top_jobs=[],
)

_ROW_CAROL = UsageRow(
    email="carol@mila.quebec",
    display_name="Carol Danvers",
    user_id=3,
    rgu_hours=700.0,
    wasted=420.0,
    by_cluster=[UsageClusterBreakdown("mila", 700.0, 280.0, 700.0 - 280.0)],
    top_jobs=[],
)


def _build_user_dm(row, *, window_weeks):
    return build_user_dm(
        row,
        window_weeks=window_weeks,
        window_start=_WINDOW_START,
        window_end=_WINDOW_END,
    )


# ── build_user_dm ─────────────────────────────────────────────────────────────


def test_underusage_report():
    window_weeks = 2
    with _notify_overlay() as config:
        rgu_unit = config.sarc.notifications.rgu_unit
        user_unit = config.sarc.notifications.user_unit
        text = _build_user_dm(_ROW_ALICE, window_weeks=window_weeks)
    # Ensure no formatting braces {} remain
    assert "{" not in text
    assert "}" not in text
    # Test greeting contains the mention token, ready for dm_user to substitute
    assert MENTION_TOKEN in text
    # Test overview contains window weeks
    assert f"{window_weeks}" in text
    # Test overview contains the real date range
    assert _date_range(_WINDOW_START, _WINDOW_END) in text
    # Test overview contains rgu used
    assert _fmt_rgu(_ROW_ALICE.rgu_hours, rgu_unit.hours_per_unit) in text
    # Test overview contains rgu long unit
    assert rgu_unit.unit_long in text
    # Test overview contains utilization
    assert _pct(_ROW_ALICE.avg_utilization) in text
    # Test overview contains unused hours
    assert _fmt_rgu(_ROW_ALICE.wasted, rgu_unit.hours_per_unit) in text
    # Test overview contains user friendly used
    assert _fmt_rgu(_ROW_ALICE.wasted, user_unit.hours_per_unit) in text
    # Test overview contains user friendly long unit
    assert user_unit.unit_long in text
    # Test overview contains bottom jobs count
    assert f"bottom_jobs_count:{len(_ROW_ALICE.bottom_jobs)}" in text
    # Test bottom jobs grouped by cluster
    # narval block appears before fir block (narval has more total waste)
    assert text.index("Cluster narval") < text.index("Cluster fir")
    # Test job line format
    assert (
        _jobs_section(
            _ROW_ALICE.bottom_jobs,
            rgu_value=lambda j: j.wasted,
            hours_per_unit=rgu_unit.hours_per_unit,
            suffix=f"{rgu_unit.unit} unused",
        )
        in text
    )
    # Test overview contains dashboard_url with the report window as query params
    assert (
        "https://dash.example.com?start=2026-01-01&end=2026-02-01"
        "&utm_source=slack&utm_medium=notification"
        "&utm_campaign=underusage_report&utm_content=dashboard" in text
    )


def test_dm_raises_without_notifications_config():
    with pytest.raises(ConfigurationError, match="No notifications configuration"):
        _build_user_dm(_ROW_ALICE, window_weeks=2)


# ── build_admin_digest ────────────────────────────────────────────────────────


_DIGEST_KW = {"cluster_share_threshold": 0.30, "active_cycles": 3, "top_n": 16}


def test_digest_header_contains_period():
    with _notify_overlay():
        text = build_admin_digest(
            [_ROW_ALICE, _ROW_BOB], period="2026-05-21 to 2026-06-04", **_DIGEST_KW
        )
    assert "2026-05-21 to 2026-06-04" in text


def test_digest_count_line():
    with _notify_overlay():
        text = build_admin_digest(
            [_ROW_ALICE, _ROW_BOB, _ROW_CAROL], period="…", **_DIGEST_KW
        )
    assert "3 user(s) flagged" in text


def test_digest_ranked_by_wasted_descending():
    with _notify_overlay():
        text = build_admin_digest(
            [_ROW_ALICE, _ROW_BOB, _ROW_CAROL], period="…", **_DIGEST_KW
        )
    # Bob: 600 wasted, Carol: 420, Alice: 245 → Bob is rank 1
    assert (
        text.index("Bob Marley")
        < text.index("Carol Danvers")
        < text.index("Alice Liddell")
    )


def test_digest_capped_at_top_n():
    with _notify_overlay():
        text = build_admin_digest(
            [_ROW_ALICE, _ROW_BOB, _ROW_CAROL], period="…", **{**_DIGEST_KW, "top_n": 2}
        )
    assert "Alice Liddell" not in text  # rank 3 — excluded
    assert "Bob Marley" in text
    assert "Carol Danvers" in text


def test_digest_contains_primary_cluster():
    with _notify_overlay():
        text = build_admin_digest([_ROW_ALICE], period="…", **_DIGEST_KW)
    assert "narval" in text


def test_digest():
    with _notify_overlay() as config:
        rgu_unit = config.sarc.notifications.rgu_unit
        text = build_admin_digest([_ROW_BOB], period="…", **_DIGEST_KW)
    # Test contains unused hours
    assert f"{_fmt_rgu(600.0, rgu_unit.hours_per_unit)} {rgu_unit.unit} unused" in text
    # Test contains waste ratio
    assert "75.0 %" in text


def test_digest_deterministic():
    rows = [_ROW_ALICE, _ROW_BOB]
    with _notify_overlay():
        assert build_admin_digest(rows, period="p", **_DIGEST_KW) == build_admin_digest(
            rows, period="p", **_DIGEST_KW
        )


def test_digest_empty_rows():
    with _notify_overlay():
        text = build_admin_digest([], period="…", **_DIGEST_KW)
    assert "0 user(s) flagged" in text


def test_digest_recurring_header_reflects_share():
    """Non-default cluster_share_threshold propagate to the recurring table header."""
    row = RecurringUserRow(
        email="alice@mila.quebec",
        display_name="Alice Liddell",
        cluster="narval",
        wasted_current_active_window=1000.0,
        cluster_share=0.20,
        cycles=[True, True, True, True, True],
        flagged_for_personalized_action=True,
    )
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_admin_digest(
            [],
            period="…",
            cluster_share_threshold=0.40,
            active_cycles=3,
            top_n=16,
            recurring={"narval": [row]},
        )
    assert "40 %" in text


def test_digest_raises_without_notifications_config():
    with pytest.raises(ConfigurationError, match="No notifications configuration"):
        build_admin_digest(
            [], period="", cluster_share_threshold=0.0, active_cycles=0, top_n=0
        )


# ── build_recurring_table direct tests ───────────────────────────────────────

_RECURRING_ROW_DEFAULT = RecurringUserRow(
    email="bob@mila.quebec",
    display_name="Bob",
    cluster="narval",
    wasted_current_active_window=500.0,
    cluster_share=0.25,
    cycles=[True, True, True, False, False],
    flagged_for_personalized_action=True,
)

_RECURRING_KW_DEFAULT = {"cluster_share_threshold": 0.30, "active_cycles": 3}


def test_recurring_table_default_labels():
    """5-cycle/3-active default: labels W0…W-8, separator after W-4."""
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table(
            {"narval": [_RECURRING_ROW_DEFAULT]}, **_RECURRING_KW_DEFAULT
        )
    assert "W0" in text
    assert "W-2" in text
    assert "W-4" in text
    assert "W-6" in text
    assert "W-8" in text
    assert "W-4" in text and "  |" in text
    # Separator appears in the header between W-4 and W-6
    assert text.index("W-4") < text.index("  |") < text.index("W-6")


def test_recurring_table_4_cycles():
    """4-cycle/2-active: labels W0 W-2 W-4 W-6, separator after W-2."""
    row = RecurringUserRow(
        email="carol@mila.quebec",
        display_name="Carol",
        cluster="beluga",
        wasted_current_active_window=300.0,
        cluster_share=0.15,
        cycles=[True, True, False, False],
        flagged_for_personalized_action=True,
    )
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table(
            {"beluga": [row]}, cluster_share_threshold=0.30, active_cycles=2
        )
    assert "W0" in text
    assert "W-2" in text
    assert "W-4" in text
    assert "W-6" in text
    assert "W-8" not in text
    assert text.index("W-2") < text.index("  |") < text.index("W-4")


def test_recurring_table_6_cycles():
    """6-cycle/3-active with usage_cycle_length_weeks=2: labels include W-10, separator after W-4."""
    row = RecurringUserRow(
        email="dave@mila.quebec",
        display_name="Dave",
        cluster="cedar",
        wasted_current_active_window=800.0,
        cluster_share=0.35,
        cycles=[True, True, True, False, False, False],
        flagged_for_personalized_action=True,
    )
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table(
            {"cedar": [row]}, cluster_share_threshold=0.30, active_cycles=3
        )
    assert "W-10" in text
    assert text.index("W-4") < text.index("  |") < text.index("W-6")


def test_recurring_table_empty_cluster_is_skipped():
    """An empty-row cluster in the dict is omitted; populated clusters still render."""
    row = RecurringUserRow(
        email="carol@mila.quebec",
        display_name="Carol",
        cluster="narval",
        wasted_current_active_window=500.0,
        cluster_share=0.25,
        cycles=[True, True, True, False, False],
        flagged_for_personalized_action=True,
    )
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table(
            {"empty_cluster": [], "narval": [row]}, **_RECURRING_KW_DEFAULT
        )
    assert "narval" in text
    assert "empty_cluster" not in text


def test_recurring_table_all_empty_clusters_returns_empty_string():
    """A dict of all-empty cluster lists returns '' without raising IndexError."""
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        text = build_recurring_table(
            {"cluster1": [], "cluster2": []}, **_RECURRING_KW_DEFAULT
        )
    assert text == ""


def test_recurring_table_raises_without_notifications_config():
    with pytest.raises(ConfigurationError, match="No notifications configuration"):
        build_recurring_table(
            {"cluster1": []}, cluster_share_threshold=0.0, active_cycles=1
        )
