from datetime import UTC, datetime

from sarc.notifications.messages import build_admin_digest, build_user_dm
from sarc.notifications.underusage import ClusterBreakdown, UnderuserJob, UnderuserRow

# ── Fixtures ──────────────────────────────────────────────────────────────────

_JOB_NARVAL_1 = UnderuserJob(
    job_id=111111,
    cluster="narval",
    submit_time=datetime(2026, 5, 28, tzinfo=UTC),
    gpu_hours_unused=80.0,
    gpu_utilization=0.05,
)
_JOB_NARVAL_2 = UnderuserJob(
    job_id=111112,
    cluster="narval",
    submit_time=datetime(2026, 5, 30, tzinfo=UTC),
    gpu_hours_unused=60.0,
    gpu_utilization=0.12,
)
_JOB_FIR = UnderuserJob(
    job_id=222222,
    cluster="fir",
    submit_time=datetime(2026, 5, 31, tzinfo=UTC),
    gpu_hours_unused=40.0,
    gpu_utilization=None,
)

_ROW_ALICE = UnderuserRow(
    email="alice@mila.quebec",
    display_name="Alice Liddell",
    user_id=1,
    gpu_hours=1000.0,
    wasted=255.0,
    requested=1000.0,
    waste_ratio=0.255,
    avg_utilization=0.745,
    gpu_hours_unused=255.0,
    by_cluster=[
        ClusterBreakdown("narval", 700.0, 210.0, 700.0),
        ClusterBreakdown("fir", 300.0, 45.0, 300.0),
    ],
    top_jobs=[_JOB_NARVAL_1, _JOB_NARVAL_2, _JOB_FIR],
)

_ROW_BOB = UnderuserRow(
    email="bob@mila.quebec",
    display_name="Bob Marley",
    user_id=2,
    gpu_hours=800.0,
    wasted=600.0,
    requested=800.0,
    waste_ratio=0.75,
    avg_utilization=0.25,
    gpu_hours_unused=600.0,
    by_cluster=[
        ClusterBreakdown("fir", 800.0, 600.0, 800.0),
    ],
    top_jobs=[],
)

_ROW_CAROL = UnderuserRow(
    email="carol@mila.quebec",
    display_name="Carol Danvers",
    user_id=3,
    gpu_hours=700.0,
    wasted=420.0,
    requested=700.0,
    waste_ratio=0.60,
    avg_utilization=0.40,
    gpu_hours_unused=420.0,
    by_cluster=[ClusterBreakdown("mila", 700.0, 420.0, 700.0)],
    top_jobs=[],
)


# ── build_user_dm ─────────────────────────────────────────────────────────────


def test_dm_greeting_uses_first_name():
    text = build_user_dm(_ROW_ALICE, window_days=14)
    assert text.startswith("Hi Alice,")


def test_dm_overview_line_contains_utilization():
    text = build_user_dm(_ROW_ALICE, window_days=14)
    assert "74.5 %" in text


def test_dm_overview_line_contains_unused_hours():
    text = build_user_dm(_ROW_ALICE, window_days=14)
    assert "255.0 GPU-hours unused" in text


def test_dm_overview_line_contains_window_days():
    text = build_user_dm(_ROW_ALICE, window_days=14)
    assert "last 14 days" in text


def test_dm_top_jobs_section_present():
    text = build_user_dm(_ROW_ALICE, window_days=14)
    assert "Jobs with the lowest GPU utilization:" in text


def test_dm_top_jobs_grouped_by_cluster():
    text = build_user_dm(_ROW_ALICE, window_days=14)
    # narval block appears before fir block (narval has more total waste)
    assert text.index("Cluster narval") < text.index("Cluster fir")


def test_dm_top_jobs_narval_first_job():
    text = build_user_dm(_ROW_ALICE, window_days=14)
    assert "job_111111 (2026-05-28)" in text
    assert "80.0 GPU-h unused" in text
    assert "GPU utilization: 5 %" in text


def test_dm_top_jobs_utilization_none_shows_na():
    text = build_user_dm(_ROW_ALICE, window_days=14)
    assert "GPU utilization: n/a" in text


def test_dm_tree_characters_multi_job_cluster():
    text = build_user_dm(_ROW_ALICE, window_days=14)
    # Two narval jobs → first uses ┌─, last uses └─
    assert "┌─ job_111111" in text
    assert "└─ job_111112" in text


def test_dm_tree_character_single_job_cluster():
    text = build_user_dm(_ROW_ALICE, window_days=14)
    # Single fir job → uses └─
    assert "└─ job_222222" in text


def test_dm_no_dashboard_url_by_default():
    text = build_user_dm(_ROW_ALICE, window_days=14)
    assert "Track your usage" not in text


def test_dm_dashboard_url_included_when_provided():
    text = build_user_dm(_ROW_ALICE, window_days=14, dashboard_url="https://dash.example.com")
    assert "Track your usage over time: https://dash.example.com" in text


def test_dm_no_help_section_by_default():
    text = build_user_dm(_ROW_ALICE, window_days=14)
    assert "IDT Team" not in text


def test_dm_help_section_appended_when_provided():
    help_text = "Need help? Ask in #idt-support — IDT Team"
    text = build_user_dm(_ROW_ALICE, window_days=14, help_section=help_text)
    assert text.endswith(help_text)


def test_dm_no_jobs_omits_jobs_section():
    text = build_user_dm(_ROW_BOB, window_days=14)
    assert "Jobs with the lowest" not in text


def test_dm_deterministic():
    a = build_user_dm(_ROW_ALICE, window_days=14, dashboard_url="https://x", help_section="help")
    b = build_user_dm(_ROW_ALICE, window_days=14, dashboard_url="https://x", help_section="help")
    assert a == b


# ── build_admin_digest ────────────────────────────────────────────────────────


def test_digest_header_contains_period():
    text = build_admin_digest([_ROW_ALICE, _ROW_BOB], period="2026-05-21 to 2026-06-04")
    assert "2026-05-21 to 2026-06-04" in text


def test_digest_count_line():
    text = build_admin_digest([_ROW_ALICE, _ROW_BOB, _ROW_CAROL], period="…")
    assert "3 user(s) flagged" in text


def test_digest_ranked_by_wasted_descending():
    text = build_admin_digest([_ROW_ALICE, _ROW_BOB, _ROW_CAROL], period="…")
    # Bob: 600 wasted, Carol: 420, Alice: 245 → Bob is rank 1
    assert text.index("Bob Marley") < text.index("Carol Danvers")
    assert text.index("Carol Danvers") < text.index("Alice Liddell")


def test_digest_capped_at_top_n():
    rows = [_ROW_ALICE, _ROW_BOB, _ROW_CAROL]
    text = build_admin_digest(rows, period="…", top_n=2)
    assert "Alice Liddell" not in text  # rank 3 — excluded
    assert "Bob Marley" in text
    assert "Carol Danvers" in text


def test_digest_contains_primary_cluster():
    text = build_admin_digest([_ROW_ALICE], period="…")
    assert "narval" in text


def test_digest_contains_wasted_hours():
    text = build_admin_digest([_ROW_BOB], period="…")
    assert "600.0 GPU-h wasted" in text


def test_digest_contains_waste_ratio():
    text = build_admin_digest([_ROW_BOB], period="…")
    assert "75.0 %" in text


def test_digest_deterministic():
    rows = [_ROW_ALICE, _ROW_BOB]
    assert build_admin_digest(rows, period="p") == build_admin_digest(rows, period="p")


def test_digest_empty_rows():
    text = build_admin_digest([], period="…")
    assert "0 user(s) flagged" in text
