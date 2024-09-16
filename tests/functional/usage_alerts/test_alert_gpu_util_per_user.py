import functools
from datetime import timedelta

import pytest

from sarc.alerts.usage_alerts.gpu_util_per_user import check_gpu_util_per_user
from sarc.client import get_jobs
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME

from ..jobs.test_func_job_statistics import generate_fake_timeseries
from .common import _get_warnings

get_warnings = functools.partial(
    _get_warnings,
    module="sarc.alerts.usage_alerts.gpu_util_per_user:gpu_util_per_user.py",
)


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize(
    "params,expected",
    [
        # Check with default params. In last 7 days from now (mock time: 2023-11-22),
        # there is only 2 jobs, both with no gpu_utilization, so, no warnings.
        (dict(threshold=timedelta()), []),
        # Check with no time_interval and a threshold to 7 days
        (
            dict(threshold=timedelta(hours=7), time_interval=None),
            [
                "[beaubonhomme] insufficient average gpu_util: 21585.0 GPU-seconds; minimum required: 7:00:00 (25200.0 GPU-seconds)",
                "[bonhomme] insufficient average gpu_util: 21585.0 GPU-seconds; minimum required: 7:00:00 (25200.0 GPU-seconds)",
                "[grosbonhomme] insufficient average gpu_util: 21585.0 GPU-seconds; minimum required: 7:00:00 (25200.0 GPU-seconds)",
                "[petitbonhomme] insufficient average gpu_util: 22784.166666666668 GPU-seconds; minimum required: 7:00:00 (25200.0 GPU-seconds)",
            ],
        ),
        # Check with no time_interval and threshold to 6 days
        (
            dict(threshold=timedelta(hours=6), time_interval=None),
            [
                "[beaubonhomme] insufficient average gpu_util: 21585.0 GPU-seconds; minimum required: 6:00:00 (21600.0 GPU-seconds)",
                "[bonhomme] insufficient average gpu_util: 21585.0 GPU-seconds; minimum required: 6:00:00 (21600.0 GPU-seconds)",
                "[grosbonhomme] insufficient average gpu_util: 21585.0 GPU-seconds; minimum required: 6:00:00 (21600.0 GPU-seconds)",
                # "[petitbonhomme]
            ],
        ),
        # Check with a valid time_interval
        (
            dict(threshold=timedelta(hours=8), time_interval=timedelta(days=276)),
            [
                "[beaubonhomme] insufficient average gpu_util: 19816.229166666668 GPU-seconds; minimum required: 8:00:00 (28800.0 GPU-seconds)",
                "[grosbonhomme] insufficient average gpu_util: 9023.729166666666 GPU-seconds; minimum required: 8:00:00 (28800.0 GPU-seconds)",
                "[petitbonhomme] insufficient average gpu_util: 28780.0 GPU-seconds; minimum required: 8:00:00 (28800.0 GPU-seconds)",
            ],
        ),
        # Check will all params, including minimum_runtime
        (
            dict(
                threshold=timedelta(hours=8),
                time_interval=timedelta(days=276),
                minimum_runtime=timedelta(seconds=39000),
            ),
            [
                "[beaubonhomme] insufficient average gpu_util: 19816.229166666668 GPU-seconds; minimum required: 8:00:00 (28800.0 GPU-seconds)",
                # "[grosbonhomme] insufficient average gpu_util: 9023.729166666666 GPU-seconds; minimum required: 8:00:00 (28800.0 GPU-seconds)",
                "[petitbonhomme] insufficient average gpu_util: 28780.0 GPU-seconds; minimum required: 8:00:00 (28800.0 GPU-seconds)",
            ],
        ),
    ],
)
def test_alert_gpu_util_per_user(params, expected, caplog, monkeypatch):
    monkeypatch.setattr(
        "sarc.jobs.series.get_job_time_series", generate_fake_timeseries
    )

    for job in get_jobs():
        job.statistics(save=True)

    check_gpu_util_per_user(**params)
    assert get_warnings(caplog.text) == expected
