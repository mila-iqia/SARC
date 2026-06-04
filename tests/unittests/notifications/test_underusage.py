import copy
from datetime import UTC, datetime, timedelta

import pytest
from sqlmodel import select

from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.users import UserDB
from sarc.notifications.underusage import get_underusers
from tests.db.factory import base_job

_WINDOW_START = datetime(2024, 6, 1, tzinfo=UTC)
_WINDOW_END = datetime(2024, 6, 30, tzinfo=UTC)
_MIN_RATIO = 0.50
_MIN_GPU_HOURS = 672.0  # 4 GPUs × 7 days


def _add_gpu_job(
    session,
    *,
    user_id: int,
    cluster_id: int,
    elapsed_h: float,
    requested_gres: int,
    allocated_gres: int,
    utilization: float | None = None,
    job_id: int,
    submit_offset_h: int = 0,
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
def underusage_db(read_write_db):
    session = read_write_db
    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}

    mila_id = clusters["mila"].id
    raisin_id = clusters["raisin"].id

    petitbonhomme_id = users["petitbonhomme"].id
    beaubonhomme_id = users["beaubonhomme"].id
    bramin_id = users["bramin"].id

    # High waster: 700 GPU-hours on mila, 10% utilization
    # waste_ratio = 0.90 ≥ 0.50, gpu_hours = 700 ≥ 672 → included
    _add_gpu_job(
        session,
        user_id=petitbonhomme_id,
        cluster_id=mila_id,
        elapsed_h=700,
        requested_gres=1,
        allocated_gres=1,
        utilization=0.10,
        job_id=80001,
        submit_offset_h=0,
    )

    # Low waster: 700 GPU-hours on mila, 80% utilization
    # waste_ratio = 0.20 < 0.50 → excluded
    _add_gpu_job(
        session,
        user_id=beaubonhomme_id,
        cluster_id=mila_id,
        elapsed_h=700,
        requested_gres=1,
        allocated_gres=1,
        utilization=0.80,
        job_id=80002,
        submit_offset_h=1,
    )

    # Below floor: 100 GPU-hours, 0% utilization
    # waste_ratio = 1.0 ≥ 0.50, but gpu_hours = 100 < 672 → excluded
    _add_gpu_job(
        session,
        user_id=bramin_id,
        cluster_id=mila_id,
        elapsed_h=100,
        requested_gres=1,
        allocated_gres=1,
        utilization=0.0,
        job_id=80003,
        submit_offset_h=2,
    )

    # Multi-cluster: petitbonhomme also has a job on raisin with larger waste
    # raisin: 1000 GPU-hours wasted at 0% → by_cluster should list raisin first
    _add_gpu_job(
        session,
        user_id=petitbonhomme_id,
        cluster_id=raisin_id,
        elapsed_h=1000,
        requested_gres=1,
        allocated_gres=1,
        utilization=0.0,
        job_id=80004,
        submit_offset_h=10,
    )

    # Extra jobs for top-5 test: 6 more jobs for petitbonhomme on mila to exceed top-5
    for i, util in enumerate([0.05, 0.15, 0.20, 0.25, 0.30, 0.35], start=5):
        _add_gpu_job(
            session,
            user_id=petitbonhomme_id,
            cluster_id=mila_id,
            elapsed_h=50,
            requested_gres=1,
            allocated_gres=1,
            utilization=util,
            job_id=80000 + i,
            submit_offset_h=20 + i,
        )

    session.commit()
    return session


def test_high_waster_is_returned(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_gpu_hours=_MIN_GPU_HOURS,
    )
    emails = {r.email for r in results}
    assert "petitbonhomme@mila.quebec" in emails


def test_low_waster_is_excluded(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_gpu_hours=_MIN_GPU_HOURS,
    )
    emails = {r.email for r in results}
    assert "beaubonhomme@mila.quebec" not in emails


def test_below_floor_is_excluded(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_gpu_hours=_MIN_GPU_HOURS,
    )
    emails = {r.email for r in results}
    assert "bramin@mila.quebec" not in emails


def test_by_cluster_ordered_desc_by_waste(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_gpu_hours=_MIN_GPU_HOURS,
    )
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    # raisin: 1000 GPU-hours wasted; mila: 700*(1-0.10) + 6*50*(1-util) GPU-hours
    assert row.by_cluster[0].cluster == "raisin"


def test_overview_avg_utilization(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.0,
        min_gpu_hours=0.0,
    )
    row = next(r for r in results if r.email == "beaubonhomme@mila.quebec")
    # 700h requested, 0.80 utilization → avg_utilization = 0.80
    assert abs(row.avg_utilization - 0.80) < 1e-6


def test_overview_gpu_hours_unused(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.0,
        min_gpu_hours=0.0,
    )
    row = next(r for r in results if r.email == "beaubonhomme@mila.quebec")
    # 700h × (1 - 0.80) = 140 GPU-hours unused
    assert abs(row.gpu_hours_unused - 140.0) < 1e-3


def test_top_jobs_capped_at_five(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_gpu_hours=_MIN_GPU_HOURS,
    )
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    assert len(row.top_jobs) == 5


def test_top_jobs_ordered_desc_by_gpu_hours_unused(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_gpu_hours=_MIN_GPU_HOURS,
    )
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    unused = [j.gpu_hours_unused for j in row.top_jobs]
    assert unused == sorted(unused, reverse=True)


def test_top_jobs_have_utilization(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=_MIN_RATIO,
        min_gpu_hours=_MIN_GPU_HOURS,
    )
    row = next(r for r in results if r.email == "petitbonhomme@mila.quebec")
    # All test jobs have stats so all should have non-None utilization
    assert all(j.gpu_utilization is not None for j in row.top_jobs)


def test_outside_window_excluded(underusage_db):
    before = datetime(2020, 1, 1, tzinfo=UTC)
    after = datetime(2020, 12, 31, tzinfo=UTC)
    results = get_underusers(before, after, min_ratio=0.0, min_gpu_hours=0.0)
    assert results == []


def test_waste_ratio_value(underusage_db):
    results = get_underusers(
        _WINDOW_START,
        _WINDOW_END,
        min_ratio=0.0,
        min_gpu_hours=0.0,
    )
    row = next(r for r in results if r.email == "beaubonhomme@mila.quebec")
    assert abs(row.waste_ratio - 0.20) < 1e-6


def test_unsupported_resource_raises(underusage_db):
    with pytest.raises(ValueError, match="Unsupported resource"):
        get_underusers(
            _WINDOW_START,
            _WINDOW_END,
            min_ratio=_MIN_RATIO,
            min_gpu_hours=_MIN_GPU_HOURS,
            resource="cpu",
        )
