import re
from datetime import timedelta

import pytest

from sarc.alerts.usage_alerts.gpu_util_per_user import check_gpu_util_per_user
from sarc.client import get_jobs
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME

from ..jobs.test_func_job_statistics import generate_fake_timeseries


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize(
    "params",
    [
        # Check with default params. In last 7 days from now (mock time: 2023-11-22),
        # there is only 2 jobs, both with no gpu_utilization, so, no warnings.
        dict(threshold=timedelta()),
        # Check with no time_interval and a threshold to 7 days
        dict(threshold=timedelta(hours=7), time_interval=None),
        # Check with no time_interval and threshold to 6 days
        dict(threshold=timedelta(hours=6), time_interval=None),
        # Check with a valid time_interval
        dict(threshold=timedelta(hours=8), time_interval=timedelta(days=276)),
        # Check will all params, including minimum_runtime
        dict(
            threshold=timedelta(hours=8),
            time_interval=timedelta(days=276),
            minimum_runtime=timedelta(seconds=39000),
        ),
    ],
)
def test_alert_gpu_util_per_user(params, caplog, monkeypatch, file_regression):
    monkeypatch.setattr(
        "sarc.jobs.series.get_job_time_series", generate_fake_timeseries
    )

    for job in get_jobs():
        job.statistics(save=True)

    check_gpu_util_per_user(**params)
    file_regression.check(
        re.sub(
            r"WARNING +sarc\.alerts\.usage_alerts\.gpu_util_per_user:gpu_util_per_user.py:[0-9]+ +",
            "",
            caplog.text,
        )
    )
