import math
from datetime import UTC, datetime

import pytest

from sarc.scraping.dcgm import (
    DCGM_FP64_BLANK,
    DCGM_FP64_NOT_FOUND,
    DCGM_FP64_NOT_PERMISSIONED,
    DCGM_FP64_NOT_SUPPORTED,
)
from sarc.scraping.series import compute_metric_statistics

T0 = int(datetime(2023, 1, 1, tzinfo=UTC).timestamp())


def _series(values, delta=30, **labels):
    """One raw Prometheus series as returned by custom_query."""
    return {
        "metric": {"__name__": "some_metric", **labels},
        "values": [[T0 + i * delta, str(v)] for i, v in enumerate(values)],
    }


def test_compute_metric_statistics(captrace):
    stats = compute_metric_statistics([_series(range(100), instance="cn-c002")])
    assert stats == {
        "mean": 99 / 2,
        "std": pytest.approx(29.011491975882016),
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


def test_compute_metric_statistics_single_sample_std_is_nan():
    stats = compute_metric_statistics([_series([5.0], instance="cn-c002")])
    assert stats["mean"] == 5.0
    assert math.isnan(stats["std"])


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


def test_compute_metric_statistics_time_counter_too_short_gives_nan():
    # Single-sample sources cannot be differenced: statistics exist but are
    # all NaN (behavior inherited from the pandas implementation).
    stats = compute_metric_statistics(
        [_series([5e9], instance="cn-c002", core="0")], is_time_counter=True
    )
    assert stats is not None
    assert all(math.isnan(v) for v in stats.values())


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
