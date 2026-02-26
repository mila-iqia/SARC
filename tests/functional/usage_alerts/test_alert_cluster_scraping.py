import re

import pytest
import time_machine

from ..jobs.test_func_load_job_series import MOCK_TIME

PARAMS = [
    # Check with default params. In last 7 days from now (mock time: 2023-11-22),
    # there is only 2 jobs from 1 cluster in 1 timestamp. So, threshold will be 0.
    # dict(verbose=True)
    "cluster_scraping_0",
    # Check with no time interval (i.e. all jobs).
    # dict(time_interval=None, verbose=True)
    "cluster_scraping_1",
    # Check with a supplementary cluster `another_cluster` which is not in data frame.
    # dict(time_interval=None, cluster_names=["fromage", "mila", "patate", "raisin", "another_cluster"], verbose=True)
    "cluster_scraping_2",
    # Check above case with 2 clusters ignored.
    # dict(time_interval=None, cluster_names=["mila", "raisin", "another_cluster"])
    "cluster_scraping_3",
]


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_only_db", "health_config")
@pytest.mark.parametrize(
    "check_name", PARAMS, ids=[f"params{i}" for i in range(len(PARAMS))]
)
def test_check_nb_jobs_per_cluster_per_time(
    check_name, capsys, caplog, file_regression, cli_main
):
    assert cli_main(["health", "run", "--check", check_name]) == 0
    file_regression.check(
        re.sub(
            r"ERROR +sarc\.alerts\.usage_alerts\.cluster_scraping:cluster_scraping.py:[0-9]+ +",
            "",
            f"{capsys.readouterr().err}\n{caplog.text}",
        )
    )
