import re

import pytest

from sarc.client import get_jobs
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME
from ..jobs.test_func_job_statistics import generate_fake_timeseries

PARAMS = [
    # Check with default params. In last 7 days from now (mock time: 2023-11-22),
    # there is only 2 jobs, both with no gpu_utilization, so, no warnings.
    "gpu_util_per_user_0",
    # Check with no time_interval and a threshold to 7 days
    "gpu_util_per_user_1",
    # Check with no time_interval and threshold to 6 days
    "gpu_util_per_user_2",
    # Check with a valid time_interval
    "gpu_util_per_user_3",
    # Check will all params, including minimum_runtime
    "gpu_util_per_user_4",
]


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "health_config")
@pytest.mark.parametrize(
    "check_name", PARAMS, ids=[f"params{i}" for i in range(len(PARAMS))]
)
def test_alert_gpu_util_per_user(
    check_name, caplog, monkeypatch, file_regression, cli_main
):
    monkeypatch.setattr(
        "sarc.jobs.series.get_job_time_series", generate_fake_timeseries
    )

    for job in get_jobs():
        job.statistics(save=True)

    assert cli_main(["health", "run", "--check", check_name]) == 0
    file_regression.check(
        re.sub(
            r"ERROR +sarc\.alerts\.usage_alerts\.gpu_util_per_user:gpu_util_per_user.py:[0-9]+ +",
            "",
            caplog.text,
        )
    )
