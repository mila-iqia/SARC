import re

import pytest
import time_machine

from sarc.client import get_jobs
from sarc.jobs.series import compute_job_statistics
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME

from ..jobs.test_func_job_statistics import generate_fake_timeseries

PARAMS = [
    # Check with default params. In last 7 days from now (mock time: 2023-11-22),
    # there is only 2 jobs, both with no gpu_utilization, so, no warnings.
    # dict(threshold=timedelta())
    "gpu_util_per_user_0",
    # Check with no time_interval and a threshold to 7 days
    # dict(threshold=timedelta(hours=7), time_interval=None)
    "gpu_util_per_user_1",
    # Check with no time_interval and threshold to 6 days
    # dict(threshold=timedelta(hours=6), time_interval=None)
    "gpu_util_per_user_2",
    # Check with a valid time_interval
    # dict(threshold=timedelta(hours=8), time_interval=timedelta(days=276))
    "gpu_util_per_user_3",
    # Check will all params, including minimum_runtime
    # dict(threshold=timedelta(hours=8), time_interval=timedelta(days=276), minimum_runtime=timedelta(seconds=39000))
    "gpu_util_per_user_4",
]


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
@pytest.mark.parametrize(
    "check_name", PARAMS, ids=[f"params{i}" for i in range(len(PARAMS))]
)
def test_alert_gpu_util_per_user(check_name, caplog, file_regression, cli_main):
    for job in get_jobs():
        if job.end_time is not None:
            stats = compute_job_statistics(job, generate_fake_timeseries(job))
            if not stats.empty():
                job.stored_statistics = stats
                job.save()

    assert cli_main(["health", "run", "--check", check_name]) == 0
    file_regression.check(re.sub(r"ERROR +.+\.py:[0-9]+ +", "", caplog.text))
