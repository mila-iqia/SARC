from collections import defaultdict

import pytest

from sarc.scraping.dcgm import DCGM_FP64_BLANK
from sarc.scraping.gpu_util_x100 import FIXED_SINCE, X100_SINCE, rescale_gpu_utilization

BEFORE_X100 = X100_SINCE - 86400
BEFORE_FIX = X100_SINCE + 86400
AFTER_FIX = FIXED_SINCE + 86400

UTIL = "slurm_job_utilization_gpu"
SM_ACTIVE = "slurm_job_sm_active_gpu"
MEMORY = "slurm_job_utilization_gpu_memory"


def _series(name, values, start, instance="cn-a001", delta=30):
    return {
        "metric": {"__name__": name, "instance": instance, "gpu": "0"},
        "values": [[start + i * delta, str(v)] for i, v in enumerate(values)],
    }


def _rescale(*series):
    """The job's rescaled utilization values, or None."""
    metric_to_data = defaultdict(list)
    for s in series:
        metric_to_data[s["metric"]["__name__"]].append(s)
    rescaled = rescale_gpu_utilization(metric_to_data[UTIL], metric_to_data)
    if rescaled is None:
        return None
    return [float(v) for s in rescaled for _, v in s["values"]]


def test_before_0_5_0_untouched():
    assert _rescale(_series(UTIL, [4000, 100], BEFORE_X100)) == [4000, 100]


@pytest.mark.parametrize(
    ("before", "expected"), [(100, [100, 1, 40]), (37, [37, 1, 40])]
)
def test_samples_before_0_5_0_ignored(before, expected):
    # The first sample predates 0.5.0: neither divided nor used to decide.
    assert _rescale(_series(UTIL, [before, 100, 4000], X100_SINCE - 30)) == expected


def test_nvml_untouched():
    values = _rescale(
        _series(UTIL, [100, 0], BEFORE_FIX), _series(MEMORY, [10, 10], BEFORE_FIX)
    )
    assert values == [100, 0]


def test_0_5_1_untouched():
    values = _rescale(
        _series(UTIL, [100, 37, 0], AFTER_FIX), _series(SM_ACTIVE, [1, 1, 1], AFTER_FIX)
    )
    assert values == [100, 37, 0]


def test_mixed_versions_only_above_100_divided():
    # 0.5.0 then 0.5.1: the 100 may be 1 %, but is kept.
    assert _rescale(_series(UTIL, [4000, 100, 37], AFTER_FIX)) == [40, 100, 37]


def test_0_5_0_divided():
    values = _rescale(
        _series(UTIL, [100, 4000, 0], AFTER_FIX),
        _series(SM_ACTIVE, [1, 1, 1], AFTER_FIX),
    )
    assert values == [1, 40, 0]


def test_zeros_only_untouched():
    assert _rescale(_series(UTIL, [0, 0], AFTER_FIX)) == [0, 0]


def test_hundreds_undecided():
    # Zeros and 100 only: no scale is ruled out, even before 0.5.1 was published.
    values = _rescale(
        _series(UTIL, [100, 0], BEFORE_FIX), _series(SM_ACTIVE, [1, 1], BEFORE_FIX)
    )
    assert values is None


def test_dcgm_blank_untouched():
    values = _rescale(_series(UTIL, [DCGM_FP64_BLANK, 4000], BEFORE_FIX))
    assert values == [DCGM_FP64_BLANK, 40]


def test_job_judged_as_a_whole():
    # The 37 from another node settles the 100.
    values = _rescale(
        _series(UTIL, [100, 0], AFTER_FIX, instance="cn-a001"),
        _series(UTIL, [37], AFTER_FIX, instance="cn-a002"),
    )
    assert values == [100, 0, 37]
