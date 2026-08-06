"""job_series_view's per-stat columns: each must read its own (stat name, aggregate)."""

import pytest
from sqlmodel import Session, col, select

from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.job_series import JobSeriesDB

# One mean and one max per stat, all 8 values distinct: a column reading the wrong
# stat name -- or mean where max is expected -- then reads a wrong value instead of
# an accidentally matching one.
_STATS = {
    "gpu_sm_occupancy": (0.11, 0.22),
    "gpu_utilization": (0.33, 0.44),
    "gpu_memory": (0.55, 0.66),
    "cpu_utilization": (0.77, 0.88),
}

# The factory's only job whose harmonized GPU type is known to GpuRguDB, so its
# RGU-based costs -- and the waste derived from them -- are not NULL.
_HARMONIZED_GPU = "A100-SXM4-80GB"


def _gpu_job_id(sess: Session) -> int:
    job = sess.exec(
        select(SlurmJobDB).where(col(SlurmJobDB.harmonized_gpu_type) == _HARMONIZED_GPU)
    ).one()
    assert job.id is not None
    return job.id


def _series(sess: Session, job_db_id: int) -> JobSeriesDB:
    return sess.exec(
        select(JobSeriesDB).where(col(JobSeriesDB.job_db_id) == job_db_id)
    ).one()


def _stat(job_db_id: int, name: str, mean: float, maximum: float) -> JobStatisticDB:
    # The view reads mean and max only; the other aggregates stay NULL.
    return JobStatisticDB(
        job_id=job_db_id,
        name=name,
        mean=mean,
        max=maximum,
        std=None,
        q05=None,
        q25=None,
        median=None,
        q75=None,
    )


def test_stat_columns_are_null_without_stats(read_write_db: Session):
    """No scraped stat for the job: every stat column is NULL, and so is every waste."""
    series = _series(read_write_db, _gpu_job_id(read_write_db))

    assert series.usage_metric is None
    assert series.gpu_sm_occupancy_mean is None
    assert series.gpu_sm_occupancy_max is None
    assert series.gpu_utilization_mean is None
    assert series.gpu_memory_max is None

    # The costs are computable, so a NULL waste means a missing measurement,
    # not zero waste.
    assert series.requested_gpu_cost is not None
    assert series.allocated_gpu_cost is not None
    assert series.requested_cpu_cost is not None
    assert series.requested_gpu_waste is None
    assert series.allocated_gpu_waste is None
    assert series.requested_cpu_waste is None
    assert series.allocated_cpu_waste is None


def test_stat_columns_read_their_own_stat(read_write_db: Session):
    job_db_id = _gpu_job_id(read_write_db)
    for name, (mean, maximum) in _STATS.items():
        read_write_db.add(_stat(job_db_id, name, mean, maximum))
    read_write_db.commit()

    series = _series(read_write_db, job_db_id)
    sm_mean, sm_max = _STATS["gpu_sm_occupancy"]
    cpu_mean = _STATS["cpu_utilization"][0]

    assert series.gpu_sm_occupancy_mean == sm_mean
    assert series.gpu_sm_occupancy_max == sm_max
    assert series.gpu_utilization_mean == _STATS["gpu_utilization"][0]
    assert series.gpu_memory_max == _STATS["gpu_memory"][1]

    gpu_req, gpu_alloc = series.requested_gpu_cost, series.allocated_gpu_cost
    cpu_req, cpu_alloc = series.requested_cpu_cost, series.allocated_cpu_cost
    assert gpu_req is not None and gpu_alloc is not None
    assert cpu_req is not None and cpu_alloc is not None

    # usage_metric is currently the sm-occupancy mean, and GPU waste follows it --
    # not gpu_utilization, the other GPU mean the view now exposes.
    assert series.usage_metric == sm_mean
    assert series.requested_gpu_waste == pytest.approx((1 - sm_mean) * gpu_req)
    assert series.allocated_gpu_waste == pytest.approx((1 - sm_mean) * gpu_alloc)
    # CPU waste keeps reading cpu_utilization.
    assert series.requested_cpu_waste == pytest.approx((1 - cpu_mean) * cpu_req)
    assert series.allocated_cpu_waste == pytest.approx((1 - cpu_mean) * cpu_alloc)
