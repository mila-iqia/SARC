"""Tests for get_underusers().

RGU values used:
  mila  cluster: billing_is_gpu=True  →  rgu = 1 * GpuRguDB.rgu
                 gpu_type "A100-SXM4-80GB"  →  rgu = 4.8
  raisin cluster: billing rate for "A100" = 100
                 allocated_billing=100 → gpu_count_normalized = 100/100 = 1
                 gpu_type "A100"  →  rgu = 1 * 4.0 = 4.0   (inserted in fixture)
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlmodel import select

from sarc.db.cluster import SlurmClusterDB
from sarc.db.support import GpuRguDB
from sarc.db.users import UserDB
from sarc.notifications.underusage import get_all_users_usage, get_underusers
from tests.unittests.notifications._factory import add_gpu_job

_WINDOW_START = datetime(2024, 6, 1, tzinfo=UTC)
_WINDOW_END = datetime(2024, 6, 30, tzinfo=UTC)
_MIN_RATIO = 0.50
_MIN_RGU_HOURS = 672.0  # RGU-hours floor
_TOP_JOBS_PER_USER = 5

# mila: billing_is_gpu=True, gpu_type A100-SXM4-80GB → rgu = 4.8
_MILA_GPU_TYPE = "A100-SXM4-80GB"
_MILA_RGU = 4.8

# raisin: billing "A100"=100, allocated_billing=100 → gpu_count_norm=1 → rgu=4.0
_RAISIN_GPU_TYPE = "A100"
_RAISIN_RGU = 4.0
_RAISIN_BILLING = 100


def _add_gpu_job(session, *, elapsed_h: float, end_offset_h: int = 0, **kwargs):
    """Seed a job whose end_time is `_WINDOW_START + end_offset_h`."""
    submit_time = (
        _WINDOW_START - timedelta(hours=elapsed_h) + timedelta(hours=end_offset_h)
    )
    return add_gpu_job(session, submit_time=submit_time, elapsed_h=elapsed_h, **kwargs)


@pytest.fixture
def underusage_db(read_write_db):
    session = read_write_db
    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}

    mila_id = clusters["mila"].id
    raisin_id = clusters["raisin"].id

    petitbonhomme_id = users["petitbonhomme"].id
    beaubonhomme_id = users["beaubonhomme"].id
    bramin_id = users["bramin"].id

    # Add RGU entry for raisin's "A100" GPU type (not in the default migration data).
    session.add(GpuRguDB(name=_RAISIN_GPU_TYPE, rgu=_RAISIN_RGU, drac_rgu=_RAISIN_RGU))
    session.flush()

    # High waster: 700 GPU-hours on mila, 10 % utilisation
    # rgu_hours = 4.8 * 700 = 3360,  waste_ratio = 0.90 ≥ 0.50
    # total rgu_hours ≥ 672 (floor) ✓
    _add_gpu_job(
        session,
        user_id=petitbonhomme_id,
        cluster_id=mila_id,
        elapsed_h=700,
        requested_gres=1,
        allocated_gres=1,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.10,
        job_id=80001,
        end_offset_h=0,
    )

    # Low waster: 700 GPU-hours on mila, 80 % utilisation
    # rgu_hours = 4.8 * 700 = 3360,  waste_ratio = 0.20 < 0.50 → excluded
    _add_gpu_job(
        session,
        user_id=beaubonhomme_id,
        cluster_id=mila_id,
        elapsed_h=700,
        requested_gres=1,
        allocated_gres=1,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.80,
        job_id=80002,
        end_offset_h=1,
    )

    # Below floor: 100 GPU-hours, 0 % utilisation
    # rgu_hours = 4.8 * 100 = 480 < 672 → excluded
    _add_gpu_job(
        session,
        user_id=bramin_id,
        cluster_id=mila_id,
        elapsed_h=100,
        requested_gres=1,
        allocated_gres=1,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.0,
        job_id=80003,
        end_offset_h=2,
    )

    # Multi-cluster: petitbonhomme on raisin — large job at 0 % util to be the
    # top-waste cluster (raisin wasted = 4.0 * 2000 = 8000 > mila ~4152).
    # allocated_billing=100 so gpu_count_normalized = 100/100 = 1.
    _add_gpu_job(
        session,
        user_id=petitbonhomme_id,
        cluster_id=raisin_id,
        elapsed_h=2000,
        requested_gres=1,
        allocated_gres=1,
        gpu_type=_RAISIN_GPU_TYPE,
        utilization=0.0,
        job_id=80004,
        end_offset_h=10,
        allocated_billing=_RAISIN_BILLING,
    )

    # 6 extra mila jobs for petitbonhomme — used to verify top-5 capping.
    # Together with the two mila jobs above, that is 8 total GPU jobs for this user.
    for i, util in enumerate([0.05, 0.15, 0.20, 0.25, 0.30, 0.35], start=5):
        _add_gpu_job(
            session,
            user_id=petitbonhomme_id,
            cluster_id=mila_id,
            elapsed_h=50,
            requested_gres=1,
            allocated_gres=1,
            gpu_type=_MILA_GPU_TYPE,
            utilization=util,
            job_id=80000 + i,
            end_offset_h=20 + i,
        )

    session.commit()
    yield session


# ── Threshold filtering ───────────────────────────────────────────────────────


def test_underusers_filtering_and_top_jobs(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    # Test high waster is returned
    assert "petitbonhomme@mila.quebec" in {r.email for r in results}
    # Test below floor is excluded
    assert "bramin@mila.quebec" not in {r.email for r in results}
    # Test low waster is excluded
    assert "beaubonhomme@mila.quebec" not in {r.email for r in results}

    # Test by cluster ordered desc by waste
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    # raisin: 4.0 * 2000 * 1.0 = 80 RGU-h wasted
    # mila:   4.8 * 700 * 0.9 + extras ≈ 4152 RGU-h wasted
    assert row.by_cluster[0].cluster == "raisin"

    # Test top jobs capped at five
    assert len(row.top_jobs) == _TOP_JOBS_PER_USER

    # Test top jobs ordered desc by rgu hours unused
    unused = [j.wasted for j in row.top_jobs]
    assert unused == sorted(unused, reverse=True)

    # Test top jobs have utilization
    assert all(j.gpu_utilization is not None for j in row.top_jobs)


# ── Overview fields ───────────────────────────────────────────────────────────


def test_underusers_overview_fields(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.0,
        min_rgu_hours=0.0,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    row = next(r for r in results if r.email == "beaubonhomme@mila.quebec")
    # rgu_used / rgu_requested = 0.80
    assert row.avg_utilization == pytest.approx(0.80)

    # 4.8 * 700 * (1 - 0.80) = 672.0 RGU-h unused
    assert row.wasted == pytest.approx(672.0)

    assert row.waste_ratio == pytest.approx(0.20)


# ── Edge cases ────────────────────────────────────────────────────────────────


def test_outside_window_excluded(underusage_db):
    before = datetime(2020, 1, 1, tzinfo=UTC)
    after = datetime(2020, 12, 31, tzinfo=UTC)
    assert (
        get_underusers(
            before,
            after,
            min_ratio=0.0,
            min_rgu_hours=0.0,
            top_jobs_per_user=_TOP_JOBS_PER_USER,
        )
        == []
    )


def test_unsupported_resource_raises(underusage_db):
    with pytest.raises(ValueError, match="Unsupported resource"):
        get_underusers(
            _WINDOW_START,
            _WINDOW_END,
            min_ratio=_MIN_RATIO,
            min_rgu_hours=_MIN_RGU_HOURS,
            top_jobs_per_user=_TOP_JOBS_PER_USER,
            resource="cpu",
        )


# ── get_all_users_usage ───────────────────────────────────────────────────────


def test_usage_all_users_overview_and_top_jobs(underusage_db):
    # All 3 users have GPU jobs in the window — no threshold filtering.
    results = get_all_users_usage(
        _WINDOW_START, _WINDOW_END, top_jobs_per_user=_TOP_JOBS_PER_USER
    )
    emails = {r.email for r in results}
    assert "petitbonhomme@mila.quebec" in emails
    assert "beaubonhomme@mila.quebec" in emails
    assert "bramin@mila.quebec" in emails

    # Test overview rgu hours used
    row = next(r for r in results if r.email == "beaubonhomme@mila.quebec")
    # 4.8 * 700 * 0.80 = 2688.0 RGU-h used
    assert row.rgu_hours_used == pytest.approx(2688.0)

    # Test overview avg utilization
    assert row.avg_utilization == pytest.approx(0.80)

    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    # petitbonhomme has 8 mila jobs + 1 raisin job = 9 total, capped at 5.
    assert len(row.top_jobs) == _TOP_JOBS_PER_USER

    # Test top jobs ordered desc by rgu hours used
    used = [j.rgu_hours_used for j in row.top_jobs]
    assert used == sorted(used, reverse=True)


def test_usage_outside_window_excluded(underusage_db):
    before = datetime(2020, 1, 1, tzinfo=UTC)
    after = datetime(2020, 12, 31, tzinfo=UTC)
    assert (
        get_all_users_usage(before, after, top_jobs_per_user=_TOP_JOBS_PER_USER) == []
    )


def test_usage_unsupported_resource_raises(underusage_db):
    with pytest.raises(ValueError, match="Unsupported resource"):
        get_all_users_usage(
            _WINDOW_START,
            _WINDOW_END,
            top_jobs_per_user=_TOP_JOBS_PER_USER,
            resource="cpu",
        )


# ── top_jobs_per_user is config-driven ───────────────────────────────────────


def test_top_jobs_per_user_3(underusage_db):
    # petitbonhomme has >5 jobs; verify the cap follows the param.
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=3,
    )
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    assert len(row.top_jobs) == 3


def test_usage_top_jobs_per_user_3(underusage_db):
    # petitbonhomme has 9 jobs total; verify the cap follows the param.
    results = get_all_users_usage(_WINDOW_START, _WINDOW_END, top_jobs_per_user=3)
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    assert len(row.top_jobs) == 3


# ── Cluster filter ────────────────────────────────────────────────────────────


def test_clusters_filter_excludes_other_clusters(underusage_db):
    # petitbonhomme has jobs on both mila and raisin; restrict to mila only.
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        clusters=["mila"],
    )
    petitbonhomme = next(r for r in results if "petitbonhomme" in r.email)
    assert {c.cluster for c in petitbonhomme.by_cluster} == {"mila"}


# ── Scaled waste + true_* reference fields ────────────────────────────────────


def test_true_wasted_field_at_identity(underusage_db):
    # At threshold=1.0, true_wasted must equal wasted on every row.
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.0,
        min_rgu_hours=0.0,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        utilization_ceiling=1.0,
    )
    for row in results:
        assert row.true_wasted == pytest.approx(row.wasted)
        assert row.true_waste_ratio == pytest.approx(row.waste_ratio)
        for c in row.by_cluster:
            assert c.true_wasted == pytest.approx(c.wasted)


def test_scaled_waste_less_than_true_waste_below_threshold(underusage_db):
    # petitbonhomme: m=0.10 on mila, rgu_h≈3360.  At threshold=0.80,
    # credited_used = LEAST(rgu_h, rgu_h*(1-0.80+0.10)) = rgu_h*0.30  → wasted=70%
    # true_used = rgu_h*0.10  → true_wasted=90%
    # So scaled waste < true waste.
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        utilization_ceiling=0.80,
    )
    row = next(r for r in results if "petitbonhomme" in r.email)
    assert row.wasted < row.true_wasted
    assert row.waste_ratio < row.true_waste_ratio


def test_subtractive_formula_exact_waste_ratio(underusage_db):
    # Petitbonhomme total: rgu_h=12800 (mila 4800 + raisin 8000).
    # Subtractive waste per job = rgu_h * max(0, T - m).
    # At T=0.80: sum(waste) = 9592 → waste_ratio = 9592/12800.
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        utilization_ceiling=0.80,
    )
    row = next(r for r in results if "petitbonhomme" in r.email)
    assert row.waste_ratio == pytest.approx(9592 / 12800, rel=1e-4)

    # At T=0.20: sum(waste) = 1984 → waste_ratio = 1984/12800 < _MIN_RATIO;
    # pass min_ratio=0.10 so petitbonhomme still appears.
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.10,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        utilization_ceiling=0.20,
    )
    row = next(r for r in results if "petitbonhomme" in r.email)
    assert row.waste_ratio == pytest.approx(1984 / 12800, rel=1e-4)


def test_subtractive_formula_boundary_zero_waste(underusage_db):
    # Beaubonhomme: single mila job, m=0.80, rgu_h=3360.
    # Subtractive: waste_ratio = max(0, T - m). Allocation-independent.
    # At T=0.80: m == T → waste = 0.
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.0,
        min_rgu_hours=0.0,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        utilization_ceiling=0.80,
    )
    row = next(r for r in results if "beaubonhomme" in r.email)
    assert row.wasted == pytest.approx(0.0, abs=1e-6)

    # At T=0.90: waste_ratio = 0.90 - 0.80 = 0.10 exactly.
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.05,
        min_rgu_hours=0.0,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        utilization_ceiling=0.90,
    )
    row = next(r for r in results if "beaubonhomme" in r.email)
    assert row.waste_ratio == pytest.approx(0.10, abs=1e-6)


def test_top_job_gpu_utilization_is_raw_mean(underusage_db):
    # At T=0.80, displayed gpu_utilization must be raw m, independent of T.
    # Raisin job m=0.0: shows 0.0. Mila top job m=0.10: shows 0.10 (not 0.125 =
    # 0.10/0.80).
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        utilization_ceiling=0.80,
    )
    row = next(r for r in results if "petitbonhomme" in r.email)
    raisin_job = next(j for j in row.top_jobs if j.cluster == "raisin")
    mila_top = next(j for j in row.top_jobs if j.cluster == "mila")
    assert raisin_job.gpu_utilization == pytest.approx(0.0, abs=1e-6)
    assert mila_top.gpu_utilization == pytest.approx(0.10, abs=1e-6)


# ── Usage report floor ────────────────────────────────────────────────────────


def test_usage_floor_excludes_below_threshold(underusage_db):
    # bramin: 100h * 4.8 rgu = 480 rgu_h total_requested → below 500 floor → excluded
    results = get_all_users_usage(
        _WINDOW_START,
        _WINDOW_END,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        usage_report_min_rgu_hours=500.0,
    )
    emails = {r.email for r in results}
    assert "bramin@mila.quebec" not in emails
    assert "petitbonhomme@mila.quebec" in emails


def test_usage_floor_at_boundary_is_excluded(underusage_db):
    # bramin ≈ 480 rgu_h; floor=481 is just above → excluded
    results = get_all_users_usage(
        _WINDOW_START,
        _WINDOW_END,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        usage_report_min_rgu_hours=481.0,
    )
    emails = {r.email for r in results}
    assert "bramin@mila.quebec" not in emails


# ── Missing-utilization semantics (lenient rule) ──────────────────────────────


@pytest.fixture
def missing_util_db(read_write_db):
    session = read_write_db
    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}
    # Large job with no utilization stat — would trip both floors if missing-util
    # were treated as fully wasted (rgu=4.8 * 700h = 3360 RGU-h >> 672 floor).
    _add_gpu_job(
        session,
        user_id=users["bramin"].id,
        cluster_id=clusters["mila"].id,
        elapsed_h=700,
        requested_gres=1,
        allocated_gres=1,
        gpu_type=_MILA_GPU_TYPE,
        utilization=None,
        job_id=90001,
    )
    session.commit()
    yield session


def test_missing_util_not_flagged(missing_util_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    assert "bramin@mila.quebec" not in {r.email for r in results}


def test_missing_util_zero_waste(missing_util_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.0,
        min_rgu_hours=0.0,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    row = next(r for r in results if r.email == "bramin@mila.quebec")
    assert row.wasted == pytest.approx(0.0)
    assert row.true_wasted == pytest.approx(0.0)


def test_missing_util_non_negative_waste_at_sub_threshold(missing_util_db):
    # Regression guard: at threshold < 1, the NaN/NULL else-branch must keep
    # credited_used == rgu_h (zero waste) and not apply the subtractive
    # adjustment, which would yield rgu_h * (1 - T + NaN) = NaN → undefined
    # waste.
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.0,
        min_rgu_hours=0.0,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        utilization_ceiling=0.8,
    )
    row = next(r for r in results if r.email == "bramin@mila.quebec")
    assert row.wasted >= 0.0
    assert row.wasted == pytest.approx(0.0)


def test_missing_util_usage_rgu_hours_used_equals_requested(missing_util_db):
    results = get_all_users_usage(
        _WINDOW_START, _WINDOW_END, top_jobs_per_user=_TOP_JOBS_PER_USER
    )
    row = next(r for r in results if r.email == "bramin@mila.quebec")
    assert row.rgu_hours_used == pytest.approx(row.rgu_hours)
