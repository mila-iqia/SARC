"""Tests for get_historical_stats() and the historical section in build_admin_digest().

Seed layout (mila cluster, A100-SXM4-80GB → rgu=4.8):

  petitbonhomme — 700 h @ 10 % util in each of months M-5 … M-1 (5 months)
    rgu_h = 4.8 * 700 = 3360  waste_ratio = 0.90 ≥ 0.50  rgu_h ≥ 672  → above threshold
  beaubonhomme  — 700 h @ 80 % util in month M-1 only
    rgu_h = 3360  waste_ratio = 0.20 < 0.50  → NOT above threshold

  Month M-6 has NO jobs at all.
  Last-year period: no jobs → yoy_months should be None.
"""

import copy
from datetime import UTC, datetime, timedelta

import pytest
from sqlmodel import select

from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.users import UserDB
from sarc.notifications.messages import build_admin_digest
from sarc.notifications.underusage import (
    HistoricalStats,
    MonthlyStats,
    get_historical_stats,
)
from tests.db.factory import base_job

_MILA_GPU_TYPE = "A100-SXM4-80GB"
_MILA_RGU = 4.8

_MIN_RATIO = 0.50
_MIN_RGU_HOURS = 672.0

# "Today" for the tests: 2025-07-15.  The 6 complete months before it are
# 2025-01, 2025-02, 2025-03, 2025-04, 2025-05, 2025-06.
_END = datetime(2025, 7, 15, tzinfo=UTC)
_MONTHS_WITH_DATA = ["2025-02", "2025-03", "2025-04", "2025-05", "2025-06"]
_MONTH_NO_DATA = "2025-01"


def _month_midpoint(year: int, month: int) -> datetime:
    return datetime(year, month, 15, tzinfo=UTC)


def _add_gpu_job(
    session,
    *,
    user_id: int,
    cluster_id: int,
    elapsed_h: float,
    gpu_type: str,
    utilization: float | None,
    job_id: int,
    submit_time: datetime,
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
def historical_db(read_write_db):
    session = read_write_db
    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}
    mila_id = clusters["mila"].id
    petitbonhomme_id = users["petitbonhomme"].id
    beaubonhomme_id = users["beaubonhomme"].id

    job_counter = 90000
    # petitbonhomme: high-waste job in each of the 5 data months (Feb–Jun 2025)
    for month in range(2, 7):
        job_counter += 1
        _add_gpu_job(
            session,
            user_id=petitbonhomme_id,
            cluster_id=mila_id,
            elapsed_h=700,
            gpu_type=_MILA_GPU_TYPE,
            utilization=0.10,
            job_id=job_counter,
            submit_time=_month_midpoint(2025, month),
        )

    # beaubonhomme: low-waste job only in Jun 2025
    job_counter += 1
    _add_gpu_job(
        session,
        user_id=beaubonhomme_id,
        cluster_id=mila_id,
        elapsed_h=700,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.80,
        job_id=job_counter,
        submit_time=_month_midpoint(2025, 6),
    )

    session.commit()
    yield session


# ── _iter_months / bucket edges ───────────────────────────────────────────────


def test_iter_months_count():
    from sarc.notifications.underusage import _iter_months

    buckets = _iter_months(_END, 6)
    assert len(buckets) == 6


def test_iter_months_oldest_first():
    from sarc.notifications.underusage import _iter_months

    buckets = _iter_months(_END, 6)
    starts = [s for s, _ in buckets]
    assert starts == sorted(starts)


def test_iter_months_contiguous():
    from sarc.notifications.underusage import _iter_months

    buckets = _iter_months(_END, 6)
    for (_, e1), (s2, _) in zip(buckets, buckets[1:]):
        assert e1 == s2


def test_iter_months_covers_expected_labels():
    from sarc.notifications.underusage import _iter_months

    buckets = _iter_months(_END, 6)
    labels = [s.strftime("%Y-%m") for s, _ in buckets]
    assert labels == ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06"]


# ── monthly aggregates ────────────────────────────────────────────────────────


def test_months_with_data_have_nonzero_ratio(historical_db):
    result = get_historical_stats(
        _END, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS
    )
    ratios = {m.label: m.avg_waste_ratio for m in result.months}
    for label in _MONTHS_WITH_DATA:
        assert ratios[label] > 0.0, f"{label} should have nonzero avg_waste_ratio"


def test_empty_month_has_zero_ratio(historical_db):
    result = get_historical_stats(
        _END, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS
    )
    ratios = {m.label: m.avg_waste_ratio for m in result.months}
    assert ratios[_MONTH_NO_DATA] == 0.0


def test_above_threshold_count_in_data_months(historical_db):
    result = get_historical_stats(
        _END, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS
    )
    counts = {m.label: m.above_threshold_count for m in result.months}
    # petitbonhomme is above threshold in all data months
    for label in _MONTHS_WITH_DATA:
        assert counts[label] == 1, f"{label}: expected 1 above-threshold user"


def test_above_threshold_count_empty_month(historical_db):
    result = get_historical_stats(
        _END, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS
    )
    counts = {m.label: m.above_threshold_count for m in result.months}
    assert counts[_MONTH_NO_DATA] == 0


def test_below_threshold_user_not_counted(historical_db):
    # beaubonhomme (20 % waste) must not inflate above_threshold_count
    result = get_historical_stats(
        _END, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS
    )
    jun = next(m for m in result.months if m.label == "2025-06")
    # Both petitbonhomme (90 %) and beaubonhomme (20 %) are in Jun; only 1 above threshold
    assert jun.above_threshold_count == 1


def test_result_has_six_months(historical_db):
    result = get_historical_stats(
        _END, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS
    )
    assert len(result.months) == 6


def test_months_ordered_chronologically(historical_db):
    result = get_historical_stats(
        _END, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS
    )
    labels = [m.label for m in result.months]
    assert labels == sorted(labels)


# ── YoY handling ──────────────────────────────────────────────────────────────


def test_yoy_absent_when_no_prior_year_data(historical_db):
    # No jobs exist in 2024 → yoy_months should be None
    result = get_historical_stats(
        _END, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS
    )
    assert result.yoy_months is None


def test_yoy_present_when_prior_year_has_data(read_write_db):
    """Seed a job in Jul 2024 (one year before _END's range) to trigger YoY."""
    session = read_write_db
    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}
    mila_id = clusters["mila"].id
    petitbonhomme_id = users["petitbonhomme"].id

    _add_gpu_job(
        session,
        user_id=petitbonhomme_id,
        cluster_id=mila_id,
        elapsed_h=700,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.10,
        job_id=99999,
        # 2024-06-15 falls in the YoY window (Jun 2024 = Jun 2025 - 1 year)
        submit_time=datetime(2024, 6, 15, tzinfo=UTC),
    )
    session.commit()

    result = get_historical_stats(
        _END, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS
    )
    assert result.yoy_months is not None
    assert len(result.yoy_months) == 6
    jun_yoy = next(m for m in result.yoy_months if m.label == "2024-06")
    assert jun_yoy.avg_waste_ratio > 0.0


def test_yoy_has_six_months_when_present(read_write_db):
    session = read_write_db
    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    clusters = {c.name: c for c in session.exec(select(SlurmClusterDB)).all()}

    _add_gpu_job(
        session,
        user_id=users["petitbonhomme"].id,
        cluster_id=clusters["mila"].id,
        elapsed_h=700,
        gpu_type=_MILA_GPU_TYPE,
        utilization=0.10,
        job_id=99998,
        submit_time=datetime(2024, 3, 15, tzinfo=UTC),
    )
    session.commit()

    result = get_historical_stats(
        _END, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS
    )
    assert result.yoy_months is not None
    assert len(result.yoy_months) == 6


# ── unsupported resource ──────────────────────────────────────────────────────


def test_unsupported_resource_raises(historical_db):
    with pytest.raises(ValueError, match="Unsupported resource"):
        get_historical_stats(
            _END, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS, resource="cpu"
        )


# ── build_admin_digest with historical ────────────────────────────────────────


def _make_stats(with_yoy: bool = False) -> HistoricalStats:
    months = [
        MonthlyStats(
            label=f"2025-0{i}", avg_waste_ratio=0.5 + i * 0.01, above_threshold_count=i
        )
        for i in range(1, 7)
    ]
    yoy = (
        [
            MonthlyStats(
                label=f"2024-0{i}",
                avg_waste_ratio=0.6 + i * 0.01,
                above_threshold_count=i + 1,
            )
            for i in range(1, 7)
        ]
        if with_yoy
        else None
    )
    return HistoricalStats(months=months, yoy_months=yoy)


_DIGEST_KW = {
    "cluster_share_threshold": 0.30,
    "cycle_length_weeks": 2,
    "active_cycles": 3,
    "top_n": 16,
}


def test_digest_no_historical_by_default():
    from sarc.notifications.underusage import ClusterBreakdown, UnderuserRow

    row = UnderuserRow(
        email="x@mila.quebec",
        display_name="X Y",
        user_id=1,
        rgu_hours=1000.0,
        wasted=500.0,
        requested=1000.0,
        waste_ratio=0.5,
        avg_utilization=0.5,
        by_cluster=[ClusterBreakdown("mila", 1000.0, 500.0, 1000.0)],
        top_jobs=[],
    )
    text = build_admin_digest([row], period="…", **_DIGEST_KW)
    assert "6-Month Trend" not in text


def test_digest_historical_section_present():
    text = build_admin_digest([], period="…", historical=_make_stats(), **_DIGEST_KW)
    assert "6-Month Trend" in text


def test_digest_historical_title_follows_count():
    """Title derives from len(stats.months), not a hardcoded '6'."""
    stats = HistoricalStats(
        months=[
            MonthlyStats(
                label=f"2025-0{i}", avg_waste_ratio=0.5, above_threshold_count=1
            )
            for i in range(1, 4)  # 3 months
        ],
        yoy_months=None,
    )
    text = build_admin_digest([], period="…", historical=stats, **_DIGEST_KW)
    assert "3-Month Trend" in text
    assert "6-Month Trend" not in text


def test_digest_historical_month_labels():
    text = build_admin_digest([], period="…", historical=_make_stats(), **_DIGEST_KW)
    for i in range(1, 7):
        assert f"2025-0{i}" in text


def test_digest_historical_waste_ratio_rendered():
    text = build_admin_digest([], period="…", historical=_make_stats(), **_DIGEST_KW)
    # first month: 0.51 → "51.0 %"
    assert "51.0 %" in text


def test_digest_historical_yoy_section_present():
    text = build_admin_digest(
        [], period="…", historical=_make_stats(with_yoy=True), **_DIGEST_KW
    )
    assert "Year-over-Year" in text


def test_digest_historical_no_yoy_when_absent():
    text = build_admin_digest(
        [], period="…", historical=_make_stats(with_yoy=False), **_DIGEST_KW
    )
    assert "Year-over-Year" not in text


def test_digest_historical_yoy_labels():
    text = build_admin_digest(
        [], period="…", historical=_make_stats(with_yoy=True), **_DIGEST_KW
    )
    for i in range(1, 7):
        assert f"2024-0{i}" in text


def test_digest_deterministic_with_historical():
    stats = _make_stats(with_yoy=True)
    a = build_admin_digest([], period="p", historical=stats, **_DIGEST_KW)
    b = build_admin_digest([], period="p", historical=stats, **_DIGEST_KW)
    assert a == b


# ── Scaled threshold ──────────────────────────────────────────────────────────


def test_scaled_threshold_reduces_above_count(historical_db):
    # petitbonhomme: m=0.10. At threshold=0.10, scaled_used = LEAST(rgu_h, rgu_h*1.0)
    # = rgu_h → scaled_wasted=0 → waste_ratio=0 → NOT above threshold.
    result = get_historical_stats(
        _END, min_ratio=_MIN_RATIO, min_rgu_hours=_MIN_RGU_HOURS, threshold=0.10
    )
    for m in result.months:
        assert m.above_threshold_count == 0, (
            f"{m.label}: expected 0 but got {m.above_threshold_count}"
        )
