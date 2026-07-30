import logging
from datetime import UTC, datetime

import pytest

from sarc.db.job import SlurmJobDB
from sarc.models.job import SlurmState
from sarc.scraping.dcgm import (
    DCGM_FP64_BLANK,
    DCGM_FP64_NOT_FOUND,
    DCGM_FP64_NOT_PERMISSIONED,
    DCGM_FP64_NOT_SUPPORTED,
)
from sarc.scraping.series import compute_job_statistics, compute_metric_statistics
from tests.db.factory import base_job

T0 = int(datetime(2023, 1, 1, tzinfo=UTC).timestamp())


def _series(values, delta=30, name="some_metric", **labels):
    """One raw Prometheus series as returned by custom_query."""
    return {
        "metric": {"__name__": name, **labels},
        "values": [[T0 + i * delta, str(v)] for i, v in enumerate(values)],
    }


def _job(**patch):
    """A detached SlurmJobDB built from the factory's base fields."""
    fields = {k: v for k, v in base_job.items() if k != "cluster_name"}
    fields.update(cluster_id=1, sarc_user_id=1, job_state=SlurmState.CANCELLED)
    fields.update(patch)
    return SlurmJobDB(**fields)


def test_compute_metric_statistics(captrace):
    stats = compute_metric_statistics([_series(range(100), instance="cn-c002")])
    assert stats == {
        "mean": 99 / 2,
        "std": pytest.approx(28.86607004772212),
        "max": 99.0,
        "q05": pytest.approx(4.95),
        "q25": pytest.approx(24.75),
        "median": 99 / 2,
        "q75": pytest.approx(74.25),
    }

    # Check trace
    spans = captrace.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "compute_metric_statistics"


def test_compute_metric_statistics_no_series():
    assert compute_metric_statistics([]) is None


def test_compute_metric_statistics_normalization():
    stats = compute_metric_statistics(
        [_series(range(100), instance="cn-c002")], normalization=lambda x: x * 10
    )
    assert stats["mean"] == 10 * 99 / 2


def test_compute_metric_statistics_single_sample_std_is_zero():
    stats = compute_metric_statistics([_series([5.0], instance="cn-c002")])
    assert stats["mean"] == 5.0
    assert stats["std"] == 0.0


@pytest.mark.parametrize(["delta"], [[30], [60]])
def test_compute_metric_statistics_time_counter(delta):
    # Two sources counting at different paces: rates must be computed per
    # (instance, core) series, then pooled.
    results = [
        _series(
            [75e9 * i for i in range(100)], delta=delta, instance="cn-c002", core="0"
        ),
        _series(
            [15e9 * i for i in range(100)], delta=delta, instance="cn-c007", core="0"
        ),
    ]
    stats = compute_metric_statistics(results, is_time_counter=True)
    assert stats["mean"] == (75 / delta + 15 / delta) / 2


def test_compute_metric_statistics_time_counter_same_labels_concatenated():
    # Two chunks of the same source (identical labels) must be differenced as
    # one series, including across the chunk boundary.
    results = [
        _series([0.0, 30e9], instance="cn-c002", core="0"),
        {
            "metric": {"__name__": "some_metric", "instance": "cn-c002", "core": "0"},
            "values": [[T0 + 60, "90e9"], [T0 + 90, "150e9"]],
        },
    ]
    stats = compute_metric_statistics(results, is_time_counter=True)
    # rates: 1.0 within the first chunk, 2.0 across the boundary, 2.0 within
    # the second chunk.
    assert stats["mean"] == (1.0 + 2.0 + 2.0) / 3


def test_compute_metric_statistics_time_counter_too_short_returns_none():
    # Single-sample sources cannot be differenced: no rate, no statistics
    # (the former pandas implementation stored all-NaN statistics instead).
    stats = compute_metric_statistics(
        [_series([5e9], instance="cn-c002", core="0")], is_time_counter=True
    )
    assert stats is None


def test_compute_metric_statistics_time_counter_all_blank_returns_none():
    stats = compute_metric_statistics(
        [_series([DCGM_FP64_BLANK] * 5, instance="cn-c002", core="0")],
        is_time_counter=True,
    )
    assert stats is None


def test_compute_metric_statistics_time_counter_unlabeled_series_ignored():
    # The second series lacks the (instance, core) group labels used by this
    # metric: its samples cannot be attributed to a source and are dropped.
    results = [
        _series([0.0, 30e9], instance="cn-c002", core="0"),  # rate 1.0
        _series([0.0, 999e9]),
    ]
    stats = compute_metric_statistics(results, is_time_counter=True)
    assert stats["mean"] == 1.0


def test_compute_metric_statistics_filters_dcgm_blank():
    # Mix of valid values and DCGM sentinels (BLANK + the three error
    # variants). All sentinels must be discarded so that stats reflect only
    # the valid samples.
    sentinels = [
        DCGM_FP64_BLANK,
        DCGM_FP64_NOT_FOUND,
        DCGM_FP64_NOT_SUPPORTED,
        DCGM_FP64_NOT_PERMISSIONED,
    ]
    stats = compute_metric_statistics(
        [_series([1.0, 2.0, 3.0, *sentinels], instance="cn-c002")]
    )
    assert stats["mean"] == 2.0
    assert stats["max"] == 3.0


def test_compute_metric_statistics_filters_nan_samples():
    stats = compute_metric_statistics(
        [_series([1.0, float("nan"), 3.0], instance="cn-c002")]
    )
    assert stats["mean"] == 2.0
    assert stats["max"] == 3.0


def test_compute_metric_statistics_all_blank_returns_none():
    stats = compute_metric_statistics(
        [_series([DCGM_FP64_BLANK] * 5, instance="cn-c002")]
    )
    assert stats is None


@pytest.mark.parametrize("mem", [0, None])
def test_compute_job_statistics_without_allocated_mem(mem, caplog):
    # A None or zero allocation cannot normalize memory usage: no
    # system_memory statistic, and a warning is logged.
    job = _job(allocated_mem=mem)
    memory_series = _series(
        [1e9, 2e9], name="slurm_job_memory_usage", instance="cn-c002"
    )
    with caplog.at_level(logging.WARNING):
        stats = compute_job_statistics(job, [memory_series])
    assert stats == {}
    assert f"job.allocated_mem is None or 0 for job {job.job_id}" in caplog.text
