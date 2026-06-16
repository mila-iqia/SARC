"""Tests for get_underusers().

RGU values used:
  mila  cluster: billing_is_gpu=True  →  rgu = 1 * GpuRguDB.rgu
                 gpu_type "A100-SXM4-80GB"  →  rgu = 4.8
  raisin cluster: billing rate for "A100" = 100
                 allocated_billing=100 → gpu_count_normalized = 100/100 = 1
                 gpu_type "A100"  →  rgu = 1 * 4.0 = 4.0   (inserted in fixture)
"""

import copy
from datetime import UTC, datetime, timedelta

import pytest
from sqlmodel import select

from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.support import GpuRguDB
from sarc.db.users import UserDB
from sarc.notifications.underusage import get_all_users_usage, get_underusers
from tests.db.factory import base_job

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


def _add_gpu_job(
    session,
    *,
    user_id: int,
    cluster_id: int,
    elapsed_h: float,
    requested_gres: int,
    allocated_gres: int,
    gpu_type: str,
    utilization: float | None = None,
    job_id: int,
    submit_offset_h: int = 0,
    allocated_billing: int | None = None,
) -> SlurmJobDB:
    submit = _WINDOW_START + timedelta(hours=submit_offset_h)
    job_data = copy.deepcopy(base_job)
    job_data.pop("cluster_name")
    job_data.update(
        {
            "sarc_user_id": user_id,
            "cluster_id": cluster_id,
            "elapsed_time": int(elapsed_h * 3600),
            "submit_time": submit,
            "start_time": submit + timedelta(seconds=60),
            "end_time": submit + timedelta(hours=elapsed_h),
            "job_id": job_id,
            "requested_gres_gpu": requested_gres,
            "allocated_gres_gpu": allocated_gres,
            "allocated_gpu_type": gpu_type,
            "job_state": "COMPLETED",
        }
    )
    if allocated_billing is not None:
        job_data["allocated_billing"] = allocated_billing
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
        submit_offset_h=0,
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
        submit_offset_h=1,
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
        submit_offset_h=2,
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
        submit_offset_h=10,
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
            submit_offset_h=20 + i,
        )

    session.commit()
    yield session


# ── Threshold filtering ───────────────────────────────────────────────────────


def test_high_waster_is_returned(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    assert "petitbonhomme@mila.quebec" in {r.email for r in results}


def test_low_waster_is_excluded(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    assert "beaubonhomme@mila.quebec" not in {r.email for r in results}


def test_below_floor_is_excluded(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    assert "bramin@mila.quebec" not in {r.email for r in results}


# ── Cluster ordering ──────────────────────────────────────────────────────────


def test_by_cluster_ordered_desc_by_waste(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    # raisin: 4.0 * 2000 * 1.0 = 80 RGU-h wasted
    # mila:   4.8 * 700 * 0.9 + extras ≈ 4152 RGU-h wasted
    assert row.by_cluster[0].cluster == "raisin"


# ── Overview fields ───────────────────────────────────────────────────────────


def test_overview_avg_utilization(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.0,
        min_rgu_hours=0.0,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    row = next(r for r in results if r.email == "beaubonhomme@mila.quebec")
    # rgu_used / rgu_requested = 0.80
    assert abs(row.avg_utilization - 0.80) < 1e-6


def test_overview_rgu_hours_unused(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.0,
        min_rgu_hours=0.0,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    row = next(r for r in results if r.email == "beaubonhomme@mila.quebec")
    # 4.8 * 700 * (1 - 0.80) = 672.0 RGU-h unused
    assert abs(row.rgu_hours_unused - 672.0) < 0.1


def test_waste_ratio_value(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.0,
        min_rgu_hours=0.0,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    row = next(r for r in results if r.email == "beaubonhomme@mila.quebec")
    assert abs(row.waste_ratio - 0.20) < 1e-6


# ── Top-5 jobs ────────────────────────────────────────────────────────────────


def test_top_jobs_capped_at_five(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    assert len(row.top_jobs) == 5


def test_top_jobs_ordered_desc_by_rgu_hours_unused(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    unused = [j.rgu_hours_unused for j in row.top_jobs]
    assert unused == sorted(unused, reverse=True)


def test_top_jobs_have_utilization(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
    )
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    assert all(j.gpu_utilization is not None for j in row.top_jobs)


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


def test_all_active_users_returned(underusage_db):
    # All 3 users have GPU jobs in the window — no threshold filtering.
    results = get_all_users_usage(
        _WINDOW_START, _WINDOW_END, top_jobs_per_user=_TOP_JOBS_PER_USER
    )
    emails = {r.email for r in results}
    assert "petitbonhomme@mila.quebec" in emails
    assert "beaubonhomme@mila.quebec" in emails
    assert "bramin@mila.quebec" in emails


def test_usage_outside_window_excluded(underusage_db):
    before = datetime(2020, 1, 1, tzinfo=UTC)
    after = datetime(2020, 12, 31, tzinfo=UTC)
    assert (
        get_all_users_usage(before, after, top_jobs_per_user=_TOP_JOBS_PER_USER) == []
    )


def test_usage_overview_rgu_hours_used(underusage_db):
    results = get_all_users_usage(
        _WINDOW_START, _WINDOW_END, top_jobs_per_user=_TOP_JOBS_PER_USER
    )
    row = next(r for r in results if r.email == "beaubonhomme@mila.quebec")
    # 4.8 * 700 * 0.80 = 2688.0 RGU-h used
    assert abs(row.rgu_hours_used - 2688.0) < 0.1


def test_usage_overview_avg_utilization(underusage_db):
    results = get_all_users_usage(
        _WINDOW_START, _WINDOW_END, top_jobs_per_user=_TOP_JOBS_PER_USER
    )
    row = next(r for r in results if r.email == "beaubonhomme@mila.quebec")
    assert abs(row.avg_utilization - 0.80) < 1e-6


def test_usage_top_jobs_capped_at_five(underusage_db):
    # petitbonhomme has 8 mila jobs + 1 raisin job = 9 total, capped at 5.
    results = get_all_users_usage(
        _WINDOW_START, _WINDOW_END, top_jobs_per_user=_TOP_JOBS_PER_USER
    )
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    assert len(row.top_jobs) == 5


def test_usage_top_jobs_ordered_desc_by_rgu_hours_used(underusage_db):
    results = get_all_users_usage(
        _WINDOW_START, _WINDOW_END, top_jobs_per_user=_TOP_JOBS_PER_USER
    )
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    used = [j.rgu_hours_used for j in row.top_jobs]
    assert used == sorted(used, reverse=True)


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
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        threshold=1.0,
    )
    for row in results:
        assert row.true_wasted == pytest.approx(row.wasted)
        assert row.true_waste_ratio == pytest.approx(row.waste_ratio)
        for c in row.by_cluster:
            assert c.true_wasted == pytest.approx(c.wasted)


def test_scaled_waste_less_than_true_waste_below_threshold(underusage_db):
    # petitbonhomme: m=0.10 on mila, rgu_h≈3360.  At threshold=0.20,
    # scaled_used = LEAST(rgu_h, rgu_h*0.10/0.20) = rgu_h*0.50  → wasted=50%
    # true_used = rgu_h*0.10  → true_wasted=90%
    # So scaled waste < true waste.
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_rgu_hours=_MIN_RGU_HOURS,
        top_jobs_per_user=_TOP_JOBS_PER_USER,
        threshold=0.20,
    )
    row = next(r for r in results if "petitbonhomme" in r.email)
    assert row.wasted < row.true_wasted
    assert row.waste_ratio < row.true_waste_ratio
