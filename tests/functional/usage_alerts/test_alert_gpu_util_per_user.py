import re

import pytest
import sqlmodel
import time_machine

from sarc.config import config
from sarc.db.job import SlurmJobDB
from sarc.jobs.series import compute_job_statistics
from tests.functional.common import MOCK_TIME, generate_fake_timeseries

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
    with config().db.session() as sess:
        for job in sess.exec(sqlmodel.select(SlurmJobDB)).all():
            if job.end_time is not None and job.nodes:
                stats = compute_job_statistics(job, generate_fake_timeseries(job))
                if len(stats) != 0:
                    job.statistics = stats
                    sess.merge(job)
        sess.commit()

    assert cli_main(["health", "run", "--check", check_name]) == 0
    file_regression.check(re.sub(r"ERROR +.+\.py:[0-9]+ +", "", caplog.text))


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_alert_gpu_util_per_user_no_stats(caplog, cli_main):
    # Setup: No statistics generated for any job.
    # The check should fail for all users who have GPU jobs.
    assert cli_main(["health", "run", "--check", "gpu_util_per_user_1"]) == 0
    with config().db.session() as sess:
        users = sess.exec(sqlmodel.select(sqlmodel.distinct(SlurmJobDB.cluster_user))).all()
        assert len(users) > 0
        for user in users:
            assert (
                f"[{user}] average gpu_util cannot be computed (no statistics found for matching jobs)."
                in caplog.text
            )
    assert "[gpu_util_per_user_1] FAILURE: gpu_util_per_user_1" in caplog.text


@pytest.mark.usefixtures("empty_read_write_db", "health_config")
def test_alert_gpu_util_per_user_empty_db(caplog, cli_main):
    assert cli_main(["health", "run", "--check", "gpu_util_per_user_0"]) == 0
    assert "No jobs in database" in caplog.text
    assert "[gpu_util_per_user_0] FAILURE: gpu_util_per_user_0" in caplog.text
