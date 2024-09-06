"""
Initial jobs in read_only_db (for reference):

|    |    job_id | cluster_name   | nodes                             |   allocated.gres_gpu | allocated.gpu_type   | start_time                | end_time                  |   elapsed_time |
|---:|----------:|:---------------|:----------------------------------|---------------------:|:---------------------|:--------------------------|:--------------------------|---------------:|
|  0 |         1 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-14 00:01:00-05:00 | 2023-02-14 12:01:00-05:00 |          43200 |
|  1 |         2 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-14 06:01:00-05:00 | 2023-02-14 18:01:00-05:00 |          43200 |
|  2 |         3 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-14 12:01:00-05:00 | 2023-02-15 00:01:00-05:00 |          43200 |
|  3 |         4 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-14 18:01:00-05:00 | 2023-02-15 06:01:00-05:00 |          43200 |
|  4 |         5 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-15 00:01:00-05:00 | 2023-02-15 12:01:00-05:00 |          43200 |
|  5 |         6 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-15 06:01:00-05:00 | 2023-02-15 18:01:00-05:00 |          43200 |
|  6 |         7 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-11-21 07:00:00-05:00 | 2023-11-21 19:00:00-05:00 |          43200 |
|  7 |         8 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-11-21 07:00:00-05:00 | 2023-11-21 19:00:00-05:00 |          43200 |
|  8 |         9 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-16 00:01:00-05:00 | 2023-02-16 12:01:00-05:00 |          43200 |
|  9 |        10 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-16 00:01:00-05:00 | 2023-02-16 12:01:00-05:00 |          43200 |
| 10 |        11 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-16 00:01:00-05:00 | 2023-02-16 12:01:00-05:00 |          43200 |
| 11 |        12 | raisin         | ['bart']                          |                    1 |                      | 2023-02-16 18:01:00-05:00 | 2023-02-17 06:01:00-05:00 |          43200 |
| 12 |        13 | raisin         | ['cn-c021', 'cn-c022', 'cn-d001'] |                    1 |                      | 2023-02-17 00:01:00-05:00 | 2023-02-17 12:01:00-05:00 |          43200 |
| 13 |        14 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-17 06:01:00-05:00 | 2023-02-17 18:01:00-05:00 |          43200 |
| 14 |        15 | fromage        | ['cn-c021']                       |                    1 |                      | 2023-02-17 12:01:00-05:00 | 2023-02-18 00:01:00-05:00 |          43200 |
| 15 |        16 | patate         | ['cn-c021']                       |                    1 |                      | 2023-02-17 18:01:00-05:00 | 2023-02-18 06:01:00-05:00 |          43200 |
| 16 |        17 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-18 00:01:00-05:00 | 2023-02-18 12:01:00-05:00 |          43200 |
| 17 |        18 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-18 06:01:00-05:00 | 2023-02-18 18:01:00-05:00 |          43200 |
| 18 |        19 | mila           | ['cn-c021']                       |                    1 |                      | 2023-02-18 12:01:00-05:00 | 2023-02-19 00:01:00-05:00 |          43200 |
| 19 |        20 | raisin         | ['cn-c021']                       |                    1 |                      | 2023-02-18 18:01:00-05:00 | 2023-02-19 06:01:00-05:00 |          43200 |
| 20 |   1000000 | raisin         | ['cn-c017']                       |                    1 |                      | 2023-02-19 00:01:00-05:00 | 2023-02-19 12:01:00-05:00 |          43200 |
| 21 |   1000000 | raisin         | ['cn-b099']                       |                    1 |                      | 2023-02-19 06:01:00-05:00 | 2023-02-19 18:01:00-05:00 |          43200 |
| 22 |        23 | raisin         | ['cn-c021']                       |                    2 | A100                 | 2023-02-19 12:01:00-05:00 | 2023-02-20 00:01:00-05:00 |          43200 |
| 23 | 999999999 | mila           | ['cn-c021']                       |                    0 |                      | 2023-02-19 18:01:00-05:00 | 2023-02-20 12:01:00-05:00 |          64800 |
"""

import functools
from datetime import timedelta

import pytest

from sarc.alerts.usage_alerts.gpu_usage import check_gpu_type_usage_per_node
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME

from .common import _get_warnings

get_warnings = functools.partial(
    _get_warnings, module="sarc.alerts.usage_alerts.gpu_usage:gpu_usage.py"
)


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize(
    "params,expected",
    [
        (
            # Check GPU A100 with no interval (i.e. all jobs)
            dict(gpu_type="A100", time_interval=None, minimum_runtime=None),
            [
                "[fromage][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[mila][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[patate][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][bart] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][cn-b099] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][cn-c017] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][cn-c021] insufficient usage for GPU A100: 5.88 % (1/17), minimum required: 100.0 %",
                "[raisin][cn-c022] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][cn-d001] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            ],
        ),
        (
            # Check GPU A100 with no interval (i.e. all jobs) and minimum runtime
            dict(
                gpu_type="A100",
                time_interval=None,
                minimum_runtime=timedelta(seconds=43200),
            ),
            [
                "[fromage][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[mila][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[patate][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][bart] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][cn-b099] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][cn-c017] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][cn-c021] insufficient usage for GPU A100: 5.88 % (1/17), minimum required: 100.0 %",
                "[raisin][cn-c022] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][cn-d001] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            ],
        ),
        (
            # Check GPU A100 with no interval (i.e. all jobs) and minimum runtime too high
            dict(
                gpu_type="A100",
                time_interval=None,
                minimum_runtime=timedelta(seconds=43200 + 1),
            ),
            [],
        ),
        (
            # Check GPU A100 for all jobs with a greater threshold.
            dict(
                gpu_type="A100",
                time_interval=None,
                minimum_runtime=None,
                threshold=5 / 100,
            ),
            [
                "[fromage][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
                "[mila][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
                "[patate][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
                "[raisin][bart] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
                "[raisin][cn-b099] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
                "[raisin][cn-c017] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
                # "[raisin][cn-c021] insufficient usage for GPU A100: 5.88 % (1/17), minimum required: 5.0 %",
                "[raisin][cn-c022] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
                "[raisin][cn-d001] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
            ],
        ),
        (
            # Check GPU A100 for all jobs with threshold zero.
            dict(
                gpu_type="A100", time_interval=None, minimum_runtime=None, threshold=0
            ),
            [],
        ),
        (
            # Check GPU A100 for all jobs, a greater threshold, and minimum number of jobs per drac node set to 2.
            dict(
                gpu_type="A100",
                time_interval=None,
                minimum_runtime=None,
                threshold=10 / 100,
                min_tasks=2,
            ),
            [
                # "[fromage][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
                "[mila][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
                # "[patate][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
                # "[raisin][bart] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
                # "[raisin][cn-b099] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
                # "[raisin][cn-c017] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
                "[raisin][cn-c021] insufficient usage for GPU A100: 5.88 % (1/17), minimum required: 10.0 %",
                # "[raisin][cn-c022] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
                # "[raisin][cn-d001] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
            ],
        ),
        (
            # Check GPU A100 with default intervals (24 hours).
            # Only 2 jobs (6 and 7) will match for current frozen mock time.
            dict(gpu_type="A100"),
            [
                "[raisin][cn-c021] insufficient usage for GPU A100: 0.0 % (0/2), minimum required: 100.0 %",
            ],
        ),
        (
            # Check unknown GPU.
            dict(gpu_type="unknown", time_interval=None),
            [
                "[fromage][cn-c021] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
                "[mila][cn-c021] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
                "[patate][cn-c021] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][bart] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][cn-b099] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][cn-c017] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][cn-c021] insufficient usage for GPU unknown: 0.0 % (0/17), minimum required: 100.0 %",
                "[raisin][cn-c022] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
                "[raisin][cn-d001] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
            ],
        ),
    ],
)
def test_check_gpu_type_usage_per_node(params, expected, caplog):
    check_gpu_type_usage_per_node(**params)
    assert get_warnings(caplog.text) == expected
