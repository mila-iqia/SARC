import re

import pytest

from sarc.alerts.usage_alerts.cluster_scraping import check_nb_jobs_per_cluster_per_time

from ..jobs.test_func_load_job_series import MOCK_TIME


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize(
    "params",
    [
        # Check with default params. In last 7 days from now (mock time: 2023-11-22),
        # there is only 2 jobs from 1 cluster in 1 timestamp. So, threshold will be 0.
        dict(verbose=True),
        # Check with no time interval (i.e. all jobs).
        dict(time_interval=None, verbose=True),
        # Check with a supplementary cluster `another_cluster` which is not in data frame.
        dict(
            time_interval=None,
            cluster_names=[
                "fromage",
                "mila",
                "patate",
                "raisin",
                "another_cluster",
            ],
            verbose=True,
        ),
        # Check above case with 2 clusters ignored.
        dict(
            time_interval=None,
            cluster_names=[
                "mila",
                "raisin",
                "another_cluster",
            ],
        ),
    ],
)
def test_check_nb_jobs_per_cluster_per_time(params, capsys, caplog, file_regression):
    check_nb_jobs_per_cluster_per_time(**params)
    file_regression.check(
        re.sub(
            r"WARNING +sarc\.alerts\.usage_alerts\.cluster_scraping:cluster_scraping.py:[0-9]+ +",
            "",
            f"{capsys.readouterr().err}\n{caplog.text}",
        )
    )
