import logging
from collections import defaultdict

import pytest

from sarc.scraping.dcgm import DCGM_FP64_BLANK
from sarc.scraping.gpu_util_x100 import X100_SINCE, X100_UNTIL, rescale_gpu_utilization

BEFORE_X100 = X100_SINCE - 86400
# rorqual on 0.5.0, then on 0.5.1.
DURING_X100 = X100_SINCE + 86400
AFTER_X100 = X100_UNTIL["rorqual"] + 86400

UTIL = "slurm_job_utilization_gpu"
SM_ACTIVE = "slurm_job_sm_active_gpu"
MEMORY = "slurm_job_utilization_gpu_memory"


def _series(name, values, start, instance="cn-a001", delta=30):
    return {
        "metric": {"__name__": name, "instance": instance, "gpu": "0"},
        "values": [[start + i * delta, str(v)] for i, v in enumerate(values)],
    }


def _rescale(*series, cluster="rorqual"):
    """The job's rescaled utilization values, or None."""
    metric_to_data = defaultdict(list)
    for s in series:
        metric_to_data[s["metric"]["__name__"]].append(s)
    rescaled = rescale_gpu_utilization(metric_to_data[UTIL], metric_to_data, cluster)
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
        _series(UTIL, [100, 0], DURING_X100), _series(MEMORY, [10, 10], DURING_X100)
    )
    assert values == [100, 0]


def test_0_5_1_untouched():
    values = _rescale(
        _series(UTIL, [100, 37, 0], AFTER_X100),
        _series(SM_ACTIVE, [1, 1, 1], AFTER_X100),
    )
    assert values == [100, 37, 0]


def test_mixed_versions_only_above_100_divided():
    # 0.5.0 then 0.5.1: the 100 may be 1 %, but is kept.
    start = X100_UNTIL["rorqual"] - 30
    assert _rescale(_series(UTIL, [4000, 100, 37], start)) == [40, 100, 37]


def test_0_5_0_divided():
    values = _rescale(
        _series(UTIL, [100, 4000, 0], DURING_X100),
        _series(SM_ACTIVE, [1, 1, 1], DURING_X100),
    )
    assert values == [1, 40, 0]


@pytest.mark.parametrize(
    ("cluster", "start", "logged"),
    [
        ("rorqual", DURING_X100, False),
        ("rorqual", AFTER_X100, True),
        ("mila", DURING_X100, True),
    ],
)
def test_x100_outside_x100_until_logged(caplog, cluster, start, logged):
    with caplog.at_level(logging.WARNING):
        values = _rescale(_series(UTIL, [4000], start), cluster=cluster)
    # Divided anyway: only 0.5.0 sends more than 100.
    assert values == [40]
    assert ("outside X100_UNTIL" in caplog.text) == logged


def test_zeros_only_untouched():
    assert _rescale(_series(UTIL, [0, 0], AFTER_X100)) == [0, 0]


def test_hundreds_undecided():
    # Zeros and 100 only, while rorqual ran 0.5.0: no scale is ruled out.
    values = _rescale(
        _series(UTIL, [100, 0], DURING_X100), _series(SM_ACTIVE, [1, 1], DURING_X100)
    )
    assert values is None


def test_hundreds_straddling_x100_until_undecided():
    # The first 100 is still from while rorqual ran 0.5.0.
    start = X100_UNTIL["rorqual"] - 30
    assert _rescale(_series(UTIL, [100, 100], start)) is None


def test_hundreds_after_x100_until_kept():
    assert _rescale(_series(UTIL, [100, 0], AFTER_X100)) == [100, 0]


def test_hundreds_on_cluster_without_x100_kept():
    values = _rescale(_series(UTIL, [100, 0], DURING_X100), cluster="mila")
    assert values == [100, 0]


def test_dcgm_blank_untouched():
    values = _rescale(_series(UTIL, [DCGM_FP64_BLANK, 4000], DURING_X100))
    assert values == [DCGM_FP64_BLANK, 40]


def test_job_judged_as_a_whole():
    # The 37 from another node settles the 100.
    values = _rescale(
        _series(UTIL, [100, 0], DURING_X100, instance="cn-a001"),
        _series(UTIL, [37], DURING_X100, instance="cn-a002"),
    )
    assert values == [100, 0, 37]
