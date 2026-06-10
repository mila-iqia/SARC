"""Tests for T3b: recurring-underusers table (get_recurring_underusers + builder)."""

import copy
from datetime import UTC, datetime, timedelta

import gifnoc
import pytest
from sqlmodel import select

from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.users import UserDB
from sarc.notifications.messages import build_recurring_table
from sarc.notifications.underusage import RecurringUserRow, get_recurring_underusers
from tests.db.factory import base_job

# "Today" for all recurring tests.
_TEST_END = datetime(2024, 6, 30, tzinfo=UTC)
_14D = timedelta(days=14)

# Cycle windows (rolling from _TEST_END)
# W0:  [2024-06-16, 2024-06-30]
# W-2: [2024-06-02, 2024-06-16]
# W-4: [2024-05-19, 2024-06-02]
# W-6: [2024-05-05, 2024-05-19]
# 6-week aggregate: [2024-05-19, 2024-06-30] = W-4 + W-2 + W0

_W0_START = _TEST_END - 1 * _14D      # 2024-06-16
_W2_START = _TEST_END - 2 * _14D      # 2024-06-02
_W4_START = _TEST_END - 3 * _14D      # 2024-05-19
_W6_START = _TEST_END - 4 * _14D      # 2024-05-05

_MILA_GPU_TYPE = "A100-SXM4-80GB"
_MILA_RGU = 4.8

_MIN_RATIO = 0.50
_MIN_RGU_HOURS = 100.0

_NOTIFY_CFG = {
    "slack": {
        "description": "test channel",
        "token": "xoxb-test-token",
        "channel": "#test-channel",
    },
    "min_ratio": _MIN_RATIO,
    "min_rgu_hours": _MIN_RGU_HOURS,
    "window_days": 14,
    "digest_top_n": 16,
    "recurrence_window_weeks": 6,
    "recurrence_cluster_share": 0.30,
}


def _add_gpu_job(
    session,
    *,
    user_id: int,
    cluster_id: int,
    elapsed_h: float,
    submit_time: datetime,
    job_id: int,
    gpu_type: str = _MILA_GPU_TYPE,
    utilization: float = 0.0,
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
def recurring_db(read_write_db):
    """Seed multi-cluster, multi-cycle data for recurring-underusers tests.

    Layout (mila cluster, util=0.05 so rgu_used > 0 for exclude_zero_usage=True):
      rgu_h = 4.8 * elapsed_h; wasted = rgu_h * 0.95; waste_ratio = 0.95 ≥ 0.50 ✓

      - petitbonhomme: W0+W-2+W-4 @ 24h → wasted 3*109.44=328.32 RGU-h over 6w (highest)
                       ALSO W-6 @ 24h   → flagged in all 4 cycles → personalized_action=True
      - fourthuser:    W0+W-2+W-4 @ 23h → 3*104.88=314.64 RGU-h (2nd)
                       NO W-6            → flagged only W0+W-2+W-4 → personalized_action=False
      - bramin:        W0+W-2+W-4 @ 22h → 3*100.32=300.96 RGU-h (not selected at 30%)
      - beaubonhomme:  W0+W-2+W-4 @ 21h → 3*95.76=287.28 RGU-h (not selected; below floor)

    Cluster total (6w) = 1231.2 RGU-h.
    Selection at 30%:
      petitbonhomme (26.7%) < 30% → continue
      +fourthuser   (52.2%) ≥ 30% → stop  (bramin + beaubonhomme excluded)

    Per-cycle floor (min_rgu_hours=100): petitbonhomme/fourthuser/bramin pass (≥100);
    beaubonhomme (95.76) does not — but beaubonhomme is never selected anyway.
    """
    session = read_write_db
    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}
    mila_id = clusters["mila"].id

    # Add a 4th test user not in the default fixture.
    fourthuser = UserDB(display_name="M/Ms Fourthuser", email="fourthuser@mila.quebec")
    session.add(fourthuser)
    session.flush()

    petitbonhomme_id = users["petitbonhomme"].id
    beaubonhomme_id = users["beaubonhomme"].id
    bramin_id = users["bramin"].id
    fourthuser_id = fourthuser.id

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
            utilization=0.05,
        )
        job_id += 1

    # petitbonhomme: W0, W-2, W-4 (6w aggregate) + W-6 (cycle flag only)
    for start in (_W0_START, _W2_START, _W4_START, _W6_START):
        _seed(petitbonhomme_id, start, 24)

    # fourthuser: W0, W-2, W-4 only
    for start in (_W0_START, _W2_START, _W4_START):
        _seed(fourthuser_id, start, 23)

    # bramin: W0, W-2, W-4 (not selected at 30%)
    for start in (_W0_START, _W2_START, _W4_START):
        _seed(bramin_id, start, 22)

    # beaubonhomme: W0, W-2, W-4 (not selected; per-cycle wasted < floor)
    for start in (_W0_START, _W2_START, _W4_START):
        _seed(beaubonhomme_id, start, 21)

    session.commit()
    yield session


# ── Selection / 30 % cutoff ───────────────────────────────────────────────────


def test_selection_stops_at_threshold(recurring_db):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            window_weeks=6,
            cluster_share_threshold=0.30,
        )
    mila_rows = result.get("mila", [])
    selected_emails = {r.email for r in mila_rows}
    # petitbonhomme (highest) + fourthuser together cross 30%; bramin/beaubonhomme not included.
    assert "petitbonhomme@mila.quebec" in selected_emails
    assert "fourthuser@mila.quebec" in selected_emails
    assert "bramin@mila.quebec" not in selected_emails
    assert "beaubonhomme@mila.quebec" not in selected_emails


def test_selection_cumulative_share_reaches_threshold(recurring_db):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            window_weeks=6,
            cluster_share_threshold=0.30,
        )
    mila_rows = result["mila"]
    cumulative = sum(r.cluster_share for r in mila_rows)
    assert cumulative >= 0.30


def test_all_users_included_when_threshold_above_one(recurring_db):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            window_weeks=6,
            cluster_share_threshold=1.1,
        )
    mila_emails = {r.email for r in result.get("mila", [])}
    assert "petitbonhomme@mila.quebec" in mila_emails
    assert "fourthuser@mila.quebec" in mila_emails
    assert "bramin@mila.quebec" in mila_emails
    assert "beaubonhomme@mila.quebec" in mila_emails


def test_rows_sorted_desc_by_wasted(recurring_db):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            window_weeks=6,
            cluster_share_threshold=1.1,
        )
    wastes = [r.wasted_6w for r in result["mila"]]
    assert wastes == sorted(wastes, reverse=True)


# ── Cluster share values ──────────────────────────────────────────────────────


def test_cluster_share_sum_to_one_when_all_selected(recurring_db):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            window_weeks=6,
            cluster_share_threshold=1.1,
        )
    total_share = sum(r.cluster_share for r in result["mila"])
    assert abs(total_share - 1.0) < 1e-6


# ── Cycle flags ───────────────────────────────────────────────────────────────


def test_petitbonhomme_flagged_all_four_cycles(recurring_db):
    """petitbonhomme has jobs in W0+W-2+W-4+W-6 → all cycle flags True."""
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            window_weeks=6,
            cluster_share_threshold=1.1,
        )
    row = next(r for r in result["mila"] if r.email == "petitbonhomme@mila.quebec")
    assert row.w0 is True
    assert row.w2 is True
    assert row.w4 is True
    assert row.w6 is True


def test_petitbonhomme_personalized_action(recurring_db):
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            window_weeks=6,
            cluster_share_threshold=1.1,
        )
    row = next(r for r in result["mila"] if r.email == "petitbonhomme@mila.quebec")
    assert row.personalized_action is True


def test_fourthuser_missing_w6_cycle(recurring_db):
    """fourthuser has no W-6 job → w6=False, personalized_action=False."""
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            _TEST_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            window_weeks=6,
            cluster_share_threshold=1.1,
        )
    row = next(r for r in result["mila"] if r.email == "fourthuser@mila.quebec")
    assert row.w0 is True
    assert row.w2 is True
    assert row.w4 is True
    assert row.w6 is False
    assert row.personalized_action is False


# ── Empty / edge cases ────────────────────────────────────────────────────────


def test_empty_db_returns_empty_dict(read_write_db):
    before = datetime(2020, 1, 1, tzinfo=UTC)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        result = get_recurring_underusers(
            before,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
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
            )


# ── build_recurring_table ─────────────────────────────────────────────────────

_ROW_ALICE = RecurringUserRow(
    email="alice@mila.quebec",
    display_name="Alice Liddell",
    cluster="narval",
    wasted_6w=4200.0,
    cluster_share=0.18,
    w0=True,
    w2=True,
    w4=True,
    w6=True,
    personalized_action=True,
)

_ROW_BOB = RecurringUserRow(
    email="bob@mila.quebec",
    display_name="Bob Marley",
    cluster="narval",
    wasted_6w=2600.0,
    cluster_share=0.11,
    w0=True,
    w2=False,
    w4=True,
    w6=True,
    personalized_action=False,
)

_ROW_CAROL = RecurringUserRow(
    email="carol@mila.quebec",
    display_name="Carol Danvers",
    cluster="narval",
    wasted_6w=1100.0,
    cluster_share=0.05,
    w0=True,
    w2=True,
    w4=False,
    w6=False,
    personalized_action=False,
)


def test_table_header_contains_cluster():
    text = build_recurring_table({"narval": [_ROW_ALICE]})
    assert "Cluster narval" in text


def test_table_header_contains_window_weeks():
    text = build_recurring_table({"narval": [_ROW_ALICE]}, window_weeks=6)
    assert "last 6 weeks" in text


def test_table_threshold_in_sub_header():
    text = build_recurring_table({"narval": [_ROW_ALICE]}, cluster_share_threshold=0.30)
    assert "30 %" in text


def test_table_contains_email():
    text = build_recurring_table({"narval": [_ROW_ALICE]})
    assert "alice@mila.quebec" in text


def test_table_wasted_formatted_with_space_thousands():
    text = build_recurring_table({"narval": [_ROW_ALICE]})
    assert "4 200" in text


def test_table_share_percentage():
    text = build_recurring_table({"narval": [_ROW_BOB]})
    assert "11 %" in text


def test_table_personalized_action_flag():
    text = build_recurring_table({"narval": [_ROW_ALICE]})
    assert "⚑ personalized" in text


def test_table_no_action_flag_when_not_all_cycles():
    text = build_recurring_table({"narval": [_ROW_BOB]})
    assert "⚑ personalized" not in text


def test_table_check_and_cross_marks():
    text = build_recurring_table({"narval": [_ROW_BOB]})
    assert "✓" in text
    assert "✗" in text


def test_table_tree_chars_multiple_rows():
    text = build_recurring_table({"narval": [_ROW_ALICE, _ROW_BOB, _ROW_CAROL]})
    assert "┌─" in text
    assert "├─" in text
    assert "└─" in text


def test_table_single_row_uses_end_cap():
    text = build_recurring_table({"narval": [_ROW_ALICE]})
    assert "└─" in text
    assert "┌─" not in text


def test_table_multiple_clusters_rendered():
    carol_fir = RecurringUserRow(
        email="carol@mila.quebec",
        display_name="Carol Danvers",
        cluster="fir",
        wasted_6w=500.0,
        cluster_share=0.50,
        w0=True,
        w2=False,
        w4=False,
        w6=False,
        personalized_action=False,
    )
    text = build_recurring_table(
        {"narval": [_ROW_ALICE], "fir": [carol_fir]}
    )
    assert "Cluster narval" in text
    assert "Cluster fir" in text


def test_table_clusters_sorted_alphabetically():
    carol_fir = RecurringUserRow(
        email="carol@mila.quebec",
        display_name="Carol Danvers",
        cluster="fir",
        wasted_6w=500.0,
        cluster_share=0.50,
        w0=True,
        w2=False,
        w4=False,
        w6=False,
        personalized_action=False,
    )
    text = build_recurring_table(
        {"narval": [_ROW_ALICE], "fir": [carol_fir]}
    )
    assert text.index("Cluster fir") < text.index("Cluster narval")


def test_table_empty_dict_returns_empty_string():
    assert build_recurring_table({}) == ""


def test_table_deterministic():
    data = {"narval": [_ROW_ALICE, _ROW_BOB]}
    assert build_recurring_table(data) == build_recurring_table(data)


# ── CLI integration: recurring table appears in digest output ─────────────────


def test_dry_run_prints_recurring_table(recurring_db, cli_main, monkeypatch, capsys):
    monkeypatch.setattr("sarc.cli.notify.underusage._now_utc", lambda: _TEST_END)
    with gifnoc.overlay({"sarc.notifications": _NOTIFY_CFG}):
        cli_main(["notify", "underusage", "--window-days", "14"])
    out = capsys.readouterr().out
    assert "Recurring underusers" in out
