from datetime import UTC, datetime

from sarc.notifications.messages import (
    _first_name,
    build_admin_digest,
    build_recurring_table,
    build_usage_report,
    build_user_dm,
    split_usage_report_recipients,
)
from sarc.notifications.underusage import (
    RecurringUserRow,
    UnderuserRow,
    UsageClusterBreakdown,
    UsageJob,
    UsageRow,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

_JOB_NARVAL_1 = UsageJob(
    job_id=111111,
    cluster="narval",
    submit_time=datetime(2026, 5, 28, tzinfo=UTC),
    wasted=80.0,
    rgu_hours_used=None,
    gpu_utilization=0.05,
)
_JOB_NARVAL_2 = UsageJob(
    job_id=111112,
    cluster="narval",
    submit_time=datetime(2026, 5, 30, tzinfo=UTC),
    wasted=60.0,
    rgu_hours_used=None,
    gpu_utilization=0.12,
)
_JOB_FIR = UsageJob(
    job_id=222222,
    cluster="fir",
    submit_time=datetime(2026, 5, 31, tzinfo=UTC),
    wasted=40.0,
    rgu_hours_used=None,
    gpu_utilization=None,
)

_ROW_ALICE = UnderuserRow(
    email="alice@mila.quebec",
    display_name="Alice Liddell",
    user_id=1,
    rgu_hours=1000.0,
    wasted=255.0,
    requested=1000.0,
    waste_ratio=0.255,
    avg_utilization=0.745,
    by_cluster=[
        UsageClusterBreakdown("narval", 700.0, 490.0),
        UsageClusterBreakdown("fir", 300.0, 255.0),
    ],
    top_jobs=[_JOB_NARVAL_1, _JOB_NARVAL_2, _JOB_FIR],
)

_ROW_BOB = UnderuserRow(
    email="bob@mila.quebec",
    display_name="Bob Marley",
    user_id=2,
    rgu_hours=800.0,
    wasted=600.0,
    requested=800.0,
    waste_ratio=0.75,
    avg_utilization=0.25,
    by_cluster=[UsageClusterBreakdown("fir", 800.0, 200.0)],
    top_jobs=[],
)

_ROW_CAROL = UnderuserRow(
    email="carol@mila.quebec",
    display_name="Carol Danvers",
    user_id=3,
    rgu_hours=700.0,
    wasted=420.0,
    requested=700.0,
    waste_ratio=0.60,
    avg_utilization=0.40,
    by_cluster=[UsageClusterBreakdown("mila", 700.0, 280.0)],
    top_jobs=[],
)


# ── T2: _first_name guard ────────────────────────────────────────────────────


def test_first_name_normal():
    assert _first_name("Alice Foo") == "Alice"


def test_first_name_single_token():
    assert _first_name("Alice") == "Alice"


def test_first_name_empty_string():
    assert _first_name("") == "there"


def test_first_name_none():
    assert _first_name(None) == "there"


# ── build_user_dm ─────────────────────────────────────────────────────────────


def test_dm_greeting_uses_first_name():
    text = build_user_dm(_ROW_ALICE, window_weeks=2)
    assert text.startswith("Hi Alice,")


def test_dm_overview_line_contains_utilization():
    text = build_user_dm(_ROW_ALICE, window_weeks=2)
    assert "74.5 %" in text


def test_dm_overview_line_contains_unused_hours():
    text = build_user_dm(_ROW_ALICE, window_weeks=2)
    assert "255.0 RGU-hours unused" in text


def test_dm_overview_line_contains_window_weeks():
    text = build_user_dm(_ROW_ALICE, window_weeks=2)
    assert "last 2 weeks" in text


def test_dm_top_jobs_section_present():
    text = build_user_dm(_ROW_ALICE, window_weeks=2)
    assert "Jobs with the lowest GPU utilization:" in text


def test_dm_top_jobs_grouped_by_cluster():
    text = build_user_dm(_ROW_ALICE, window_weeks=2)
    # narval block appears before fir block (narval has more total waste)
    assert text.index("Cluster narval") < text.index("Cluster fir")


def test_dm_top_jobs_narval_first_job():
    text = build_user_dm(_ROW_ALICE, window_weeks=2)
    assert "job_111111 (2026-05-28)" in text
    assert "80.0 RGU-h unused" in text
    assert "GPU utilization: 5 %" in text


def test_dm_top_jobs_utilization_none_shows_na():
    text = build_user_dm(_ROW_ALICE, window_weeks=2)
    assert "GPU utilization: n/a" in text


def test_dm_tree_characters_multi_job_cluster():
    text = build_user_dm(_ROW_ALICE, window_weeks=2)
    # Two narval jobs → first uses ┌─, last uses └─
    assert "┌─ job_111111" in text
    assert "└─ job_111112" in text


def test_dm_tree_character_single_job_cluster():
    text = build_user_dm(_ROW_ALICE, window_weeks=2)
    # Single fir job → uses └─
    assert "└─ job_222222" in text


def test_dm_no_dashboard_url_by_default():
    text = build_user_dm(_ROW_ALICE, window_weeks=2)
    assert "Track your usage" not in text


def test_dm_dashboard_url_included_when_provided():
    text = build_user_dm(
        _ROW_ALICE, window_weeks=2, dashboard_url="https://dash.example.com"
    )
    assert "Track your usage over time: https://dash.example.com" in text


def test_dm_no_help_section_by_default():
    text = build_user_dm(_ROW_ALICE, window_weeks=2)
    assert "IDT Team" not in text


def test_dm_help_section_appended_when_provided():
    help_text = "Need help? Ask in #idt-support — IDT Team"
    text = build_user_dm(_ROW_ALICE, window_weeks=2, help_section=help_text)
    assert text.endswith(help_text)


def test_dm_no_jobs_omits_jobs_section():
    text = build_user_dm(_ROW_BOB, window_weeks=2)
    assert "Jobs with the lowest" not in text


def test_dm_deterministic():
    a = build_user_dm(
        _ROW_ALICE, window_weeks=2, dashboard_url="https://x", help_section="help"
    )
    b = build_user_dm(
        _ROW_ALICE, window_weeks=2, dashboard_url="https://x", help_section="help"
    )
    assert a == b


# ── build_admin_digest ────────────────────────────────────────────────────────


_DIGEST_KW = {
    "cluster_share_threshold": 0.30,
    "cycle_length_weeks": 2,
    "active_cycles": 3,
    "top_n": 16,
}


def test_digest_header_contains_period():
    text = build_admin_digest(
        [_ROW_ALICE, _ROW_BOB], period="2026-05-21 to 2026-06-04", **_DIGEST_KW
    )
    assert "2026-05-21 to 2026-06-04" in text


def test_digest_count_line():
    text = build_admin_digest(
        [_ROW_ALICE, _ROW_BOB, _ROW_CAROL], period="…", **_DIGEST_KW
    )
    assert "3 user(s) flagged" in text


def test_digest_ranked_by_wasted_descending():
    text = build_admin_digest(
        [_ROW_ALICE, _ROW_BOB, _ROW_CAROL], period="…", **_DIGEST_KW
    )
    # Bob: 600 wasted, Carol: 420, Alice: 245 → Bob is rank 1
    assert text.index("Bob Marley") < text.index("Carol Danvers")
    assert text.index("Carol Danvers") < text.index("Alice Liddell")


def test_digest_capped_at_top_n():
    rows = [_ROW_ALICE, _ROW_BOB, _ROW_CAROL]
    text = build_admin_digest(rows, period="…", **{**_DIGEST_KW, "top_n": 2})
    assert "Alice Liddell" not in text  # rank 3 — excluded
    assert "Bob Marley" in text
    assert "Carol Danvers" in text


def test_digest_contains_primary_cluster():
    text = build_admin_digest([_ROW_ALICE], period="…", **_DIGEST_KW)
    assert "narval" in text


def test_digest_contains_unused_hours():
    text = build_admin_digest([_ROW_BOB], period="…", **_DIGEST_KW)
    assert "600.0 RGU-h unused" in text


def test_digest_contains_waste_ratio():
    text = build_admin_digest([_ROW_BOB], period="…", **_DIGEST_KW)
    assert "75.0 %" in text


def test_digest_deterministic():
    rows = [_ROW_ALICE, _ROW_BOB]
    assert build_admin_digest(rows, period="p", **_DIGEST_KW) == build_admin_digest(
        rows, period="p", **_DIGEST_KW
    )


def test_digest_empty_rows():
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
        personalized_action=True,
    )
    text = build_admin_digest(
        [],
        period="…",
        cluster_share_threshold=0.40,
        cycle_length_weeks=2,
        active_cycles=3,
        top_n=16,
        recurring={"narval": [row]},
    )
    assert "40 %" in text


# ── build_recurring_table direct tests ───────────────────────────────────────

_RECURRING_ROW_DEFAULT = RecurringUserRow(
    email="bob@mila.quebec",
    display_name="Bob",
    cluster="narval",
    wasted_current_active_window=500.0,
    cluster_share=0.25,
    cycles=[True, True, True, False, False],
    personalized_action=True,
)

_RECURRING_KW_DEFAULT = {
    "cluster_share_threshold": 0.30,
    "cycle_length_weeks": 2,
    "active_cycles": 3,
}


def test_recurring_table_default_labels():
    """5-cycle/3-active default: labels W0…W-8, separator after W-4."""
    text = build_recurring_table(
        {"narval": [_RECURRING_ROW_DEFAULT]}, **_RECURRING_KW_DEFAULT
    )
    assert "W0" in text
    assert "W-2" in text
    assert "W-4" in text
    assert "W-6" in text
    assert "W-8" in text
    # Separator appears in the header between W-4 (index 2) and W-6 (index 3)
    assert "W-4" in text and "  |" in text
    idx_w4 = text.index("W-4")
    idx_sep = text.index("  |")
    idx_w6 = text.index("W-6")
    assert idx_w4 < idx_sep < idx_w6


def test_recurring_table_4_cycles():
    """4-cycle/2-active: labels W0 W-2 W-4 W-6, separator after W-2."""
    row = RecurringUserRow(
        email="carol@mila.quebec",
        display_name="Carol",
        cluster="beluga",
        wasted_current_active_window=300.0,
        cluster_share=0.15,
        cycles=[True, True, False, False],
        personalized_action=True,
    )
    text = build_recurring_table(
        {"beluga": [row]},
        cluster_share_threshold=0.30,
        cycle_length_weeks=2,
        active_cycles=2,
    )
    assert "W0" in text
    assert "W-2" in text
    assert "W-4" in text
    assert "W-6" in text
    assert "W-8" not in text
    idx_w2 = text.index("W-2")
    idx_sep = text.index("  |")
    idx_w4 = text.index("W-4")
    assert idx_w2 < idx_sep < idx_w4


def test_recurring_table_6_cycles():
    """6-cycle/3-active with cycle_length_weeks=2: labels include W-10, separator after W-4."""
    row = RecurringUserRow(
        email="dave@mila.quebec",
        display_name="Dave",
        cluster="cedar",
        wasted_current_active_window=800.0,
        cluster_share=0.35,
        cycles=[True, True, True, False, False, False],
        personalized_action=True,
    )
    text = build_recurring_table(
        {"cedar": [row]},
        cluster_share_threshold=0.30,
        cycle_length_weeks=2,
        active_cycles=3,
    )
    assert "W-10" in text
    idx_w4 = text.index("W-4")
    idx_sep = text.index("  |")
    idx_w6 = text.index("W-6")
    assert idx_w4 < idx_sep < idx_w6


def test_recurring_table_empty_cluster_is_skipped():
    """An empty-row cluster in the dict is omitted; populated clusters still render."""
    row = RecurringUserRow(
        email="carol@mila.quebec",
        display_name="Carol",
        cluster="narval",
        wasted_current_active_window=500.0,
        cluster_share=0.25,
        cycles=[True, True, True, False, False],
        personalized_action=True,
    )
    text = build_recurring_table(
        {"empty_cluster": [], "narval": [row]}, **_RECURRING_KW_DEFAULT
    )
    assert "narval" in text
    assert "empty_cluster" not in text


def test_recurring_table_all_empty_clusters_returns_empty_string():
    """A dict of all-empty cluster lists returns '' without raising IndexError."""
    text = build_recurring_table(
        {"cluster1": [], "cluster2": []}, **_RECURRING_KW_DEFAULT
    )
    assert text == ""


# ── build_usage_report fixtures ───────────────────────────────────────────────

_USAGE_JOB_NARVAL_1 = UsageJob(
    job_id=300001,
    cluster="narval",
    submit_time=datetime(2026, 5, 28, tzinfo=UTC),
    wasted=None,
    rgu_hours_used=120.0,
    gpu_utilization=0.72,
)
_USAGE_JOB_NARVAL_2 = UsageJob(
    job_id=300002,
    cluster="narval",
    submit_time=datetime(2026, 6, 1, tzinfo=UTC),
    wasted=None,
    rgu_hours_used=90.0,
    gpu_utilization=0.65,
)
_USAGE_JOB_FIR = UsageJob(
    job_id=300003,
    cluster="fir",
    submit_time=datetime(2026, 5, 30, tzinfo=UTC),
    wasted=None,
    rgu_hours_used=50.0,
    gpu_utilization=None,
)

_USAGE_ROW_ALICE = UsageRow(
    email="alice@mila.quebec",
    display_name="Alice Liddell",
    user_id=1,
    rgu_hours=1000.0,
    rgu_hours_used=745.0,
    avg_utilization=0.745,
    by_cluster=[
        UsageClusterBreakdown("narval", 700.0, 525.0),
        UsageClusterBreakdown("fir", 300.0, 220.0),
    ],
    top_jobs=[_USAGE_JOB_NARVAL_1, _USAGE_JOB_NARVAL_2, _USAGE_JOB_FIR],
)

_USAGE_ROW_BOB = UsageRow(
    email="bob@mila.quebec",
    display_name="Bob Marley",
    user_id=2,
    rgu_hours=800.0,
    rgu_hours_used=200.0,
    avg_utilization=0.25,
    by_cluster=[UsageClusterBreakdown("fir", 800.0, 200.0)],
    top_jobs=[],
)


# ── build_usage_report ────────────────────────────────────────────────────────


def test_usage_report_greeting_uses_first_name():
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4)
    assert text.startswith("Hi Alice,")


def test_usage_report_neutral_wording_used_not_unused():
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4)
    assert "unused" not in text
    assert "waste" not in text
    assert "used on average" in text


def test_usage_report_overview_contains_utilization():
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4)
    assert "74.5 %" in text


def test_usage_report_overview_contains_rgu_used():
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4)
    assert "745.0 RGU-hours total" in text


def test_usage_report_overview_contains_window_weeks():
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4)
    assert "last 4 weeks" in text


def test_usage_report_top_jobs_section_present():
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4)
    assert "Your top jobs by GPU usage:" in text


def test_usage_report_top_jobs_grouped_by_cluster():
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4)
    # narval has more total usage than fir → appears first
    assert text.index("Cluster narval") < text.index("Cluster fir")


def test_usage_report_job_line_format():
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4)
    assert "job_300001 (2026-05-28)" in text
    assert "120.0 RGU-h" in text
    assert "GPU utilization: 72 %" in text


def test_usage_report_job_utilization_none_shows_na():
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4)
    assert "GPU utilization: n/a" in text


def test_usage_report_tree_characters_multi_job_cluster():
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4)
    assert "┌─ job_300001" in text
    assert "└─ job_300002" in text


def test_usage_report_tree_character_single_job_cluster():
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4)
    assert "└─ job_300003" in text


def test_usage_report_no_dashboard_url_by_default():
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4)
    assert "Track your usage" not in text


def test_usage_report_dashboard_url_included_when_provided():
    text = build_usage_report(
        _USAGE_ROW_ALICE, window_weeks=4, dashboard_url="https://dash.example.com"
    )
    assert "Track your usage over time: https://dash.example.com" in text


def test_usage_report_help_section_appended_when_provided():
    help_text = "Need help? Ask in #idt-support — IDT Team"
    text = build_usage_report(_USAGE_ROW_ALICE, window_weeks=4, help_section=help_text)
    assert text.endswith(help_text)


def test_usage_report_no_jobs_omits_jobs_section():
    text = build_usage_report(_USAGE_ROW_BOB, window_weeks=4)
    assert "Your top jobs" not in text


def test_usage_report_deterministic():
    a = build_usage_report(
        _USAGE_ROW_ALICE, window_weeks=4, dashboard_url="https://x", help_section="help"
    )
    b = build_usage_report(
        _USAGE_ROW_ALICE, window_weeks=4, dashboard_url="https://x", help_section="help"
    )
    assert a == b


# ── split_usage_report_recipients ────────────────────────────────────────────


def test_split_underuser_excluded_from_report():
    report, skipped = split_usage_report_recipients(
        [_USAGE_ROW_ALICE, _USAGE_ROW_BOB], underuser_emails={"alice@mila.quebec"}
    )
    assert all(r.email != "alice@mila.quebec" for r in report)
    assert any(r.email == "alice@mila.quebec" for r in skipped)


def test_split_non_underuser_included_in_report():
    report, skipped = split_usage_report_recipients(
        [_USAGE_ROW_ALICE, _USAGE_ROW_BOB], underuser_emails={"alice@mila.quebec"}
    )
    assert any(r.email == "bob@mila.quebec" for r in report)
    assert all(r.email != "bob@mila.quebec" for r in skipped)


def test_split_empty_underusers_all_get_report():
    report, skipped = split_usage_report_recipients(
        [_USAGE_ROW_ALICE, _USAGE_ROW_BOB], underuser_emails=set()
    )
    assert len(report) == 2
    assert len(skipped) == 0


def test_split_all_are_underusers_none_get_report():
    report, skipped = split_usage_report_recipients(
        [_USAGE_ROW_ALICE, _USAGE_ROW_BOB],
        underuser_emails={"alice@mila.quebec", "bob@mila.quebec"},
    )
    assert len(report) == 0
    assert len(skipped) == 2


def test_split_lists_are_disjoint_and_cover_all():
    rows = [_USAGE_ROW_ALICE, _USAGE_ROW_BOB]
    report, skipped = split_usage_report_recipients(
        rows, underuser_emails={"alice@mila.quebec"}
    )
    assert len(report) + len(skipped) == len(rows)
    report_emails = {r.email for r in report}
    skipped_emails = {r.email for r in skipped}
    assert report_emails.isdisjoint(skipped_emails)
