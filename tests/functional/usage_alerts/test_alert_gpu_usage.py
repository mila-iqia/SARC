import functools

import pytest
import time_machine

from tests.functional.common import MOCK_TIME, _get_warnings

get_warnings = functools.partial(
    _get_warnings,
    modules=[
        "sarc.alerts.usage_alerts.gpu_usage:gpu_usage.py",
        "sarc.alerts.common:common.py",
    ],
)


PARAMS = [
    (
        # Check GPU A100 with no interval (i.e. all jobs)
        # dict(gpu_type="A100", time_interval=None, minimum_runtime=None)
        "node_gpu_usage_0",
        [
            "[fromage][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[patate][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][bart] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-b099] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-c017] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-c021] insufficient usage for GPU A100: 6.25 % (1/16), minimum required: 100.0 %",
            "[raisin][cn-c022] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-d001] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[node_gpu_usage_0] FAILURE: node_gpu_usage_0",
        ],
    ),
    (
        # Check GPU A100 with no interval (i.e. all jobs) and minimum runtime
        # dict(gpu_type="A100", time_interval=None, minimum_runtime=timedelta(seconds=43200))
        "node_gpu_usage_1",
        [
            "[fromage][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[patate][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][bart] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-b099] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-c017] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-c021] insufficient usage for GPU A100: 6.25 % (1/16), minimum required: 100.0 %",
            "[raisin][cn-c022] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-d001] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[node_gpu_usage_1] FAILURE: node_gpu_usage_1",
        ],
    ),
    (
        # Check GPU A100 with no interval (i.e. all jobs) and minimum runtime too high
        # dict(gpu_type="A100", time_interval=None, minimum_runtime=timedelta(seconds=43200 + 1))
        "node_gpu_usage_2",
        [],
    ),
    (
        # Check GPU A100 for all jobs with a greater threshold.
        # dict(gpu_type="A100", time_interval=None, minimum_runtime=None, threshold=5 / 100)
        "node_gpu_usage_3",
        [
            "[fromage][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
            "[patate][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
            "[raisin][bart] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
            "[raisin][cn-b099] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
            "[raisin][cn-c017] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
            # "[raisin][cn-c021] insufficient usage for GPU A100: 5.88 % (1/17), minimum required: 5.0 %",
            "[raisin][cn-c022] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
            "[raisin][cn-d001] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 5.0 %",
            "[node_gpu_usage_3] FAILURE: node_gpu_usage_3",
        ],
    ),
    (
        # Check GPU A100 for all jobs with threshold zero.
        # dict(gpu_type="A100", time_interval=None, minimum_runtime=None, threshold=0)
        "node_gpu_usage_4",
        [],
    ),
    (
        # Check GPU A100 for all jobs, a greater threshold, and minimum number of jobs per drac node set to 2.
        # dict(gpu_type="A100", time_interval=None, minimum_runtime=None, threshold=10 / 100, min_tasks=2)
        "node_gpu_usage_5",
        [
            # "[fromage][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
            # "[patate][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
            # "[raisin][bart] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
            # "[raisin][cn-b099] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
            # "[raisin][cn-c017] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
            "[raisin][cn-c021] insufficient usage for GPU A100: 6.25 % (1/16), minimum required: 10.0 %",
            # "[raisin][cn-c022] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
            # "[raisin][cn-d001] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 10.0 %",
            "[node_gpu_usage_5] FAILURE: node_gpu_usage_5",
        ],
    ),
    (
        # Check GPU A100 with default intervals (24 hours).
        # dict(gpu_type="A100")
        "node_gpu_usage_6",
        [],
    ),
    (
        # Check unknown GPU.
        # dict(gpu_type="unknown", time_interval=None)
        "node_gpu_usage_7",
        [
            "[fromage][cn-c021] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
            "[patate][cn-c021] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][bart] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-b099] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-c017] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-c021] insufficient usage for GPU unknown: 0.0 % (0/16), minimum required: 100.0 %",
            "[raisin][cn-c022] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-d001] insufficient usage for GPU unknown: 0.0 % (0/1), minimum required: 100.0 %",
            "[node_gpu_usage_7] FAILURE: node_gpu_usage_7",
        ],
    ),
    (
        # Check GPU A100 with minimum_runtime = 12h.
        # This is same as params 1.
        "node_gpu_usage_8",
        [
            "[fromage][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[patate][cn-c021] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][bart] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-b099] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-c017] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-c021] insufficient usage for GPU A100: 6.25 % (1/16), minimum required: 100.0 %",
            "[raisin][cn-c022] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[raisin][cn-d001] insufficient usage for GPU A100: 0.0 % (0/1), minimum required: 100.0 %",
            "[node_gpu_usage_8] FAILURE: node_gpu_usage_8",
        ],
    ),
    (
        # Check GPU A100 with a small time_interval (1 hour).
        # MOCK_TIME is 2023-11-22T00:00:00+00:00.
        # Job 7 (RUNNING) submit/start is 2023-02-15T17:01:00+00:00, it's NOT in the interval [MOCK_TIME-1h, MOCK_TIME].
        # So NO jobs should match.
        "node_gpu_usage_9",
        [],
    ),
]


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_only_db", "health_config")
@pytest.mark.parametrize(
    "check_name,expected", PARAMS, ids=[f"params{i}" for i in range(len(PARAMS))]
)
def test_check_gpu_type_usage_per_node(check_name, expected, caplog, cli_main):
    assert cli_main(["health", "run", "--check", check_name]) == 0
    assert sorted(get_warnings(caplog.text)) == sorted(expected)
