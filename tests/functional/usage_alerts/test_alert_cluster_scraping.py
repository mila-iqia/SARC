import re

import gifnoc
import pytest
import time_machine

from tests.functional.usage_alerts.common import MOCK_TIME

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
            r"ERROR +.+\.py:[0-9]+ +", "", f"{capsys.readouterr().err}\n{caplog.text}"
        )
    )


@pytest.mark.usefixtures("read_only_db")
def test_invalid_time_unit(cli_main, caplog):
    with gifnoc.overlay(
        {
            "sarc.health_monitor.checks": {
                "cluster_scraping": {
                    "$class": "sarc.alerts.usage_alerts.cluster_scraping:ClusterScrapingCheck",
                    "active": True,
                    "time_unit": "0s",
                }
            }
        }
    ):
        assert cli_main(["health", "run", "--check", "cluster_scraping"]) == 0
        assert bool(
            re.search(
                r"ERROR +.+\.py:[0-9]+ +Invalid time unit \(must be > 0\) for cluster usage checking: 0:00:00",
                caplog.text,
            )
        )
        assert bool(
            re.search(
                r"ERROR +.+\.py:[0-9]+ +\[cluster_scraping] FAILURE: cluster_scraping",
                caplog.text,
            )
        )


@pytest.mark.usefixtures("read_only_db")
def test_invalid_nb_stddev(cli_main, caplog):
    with gifnoc.overlay(
        {
            "sarc.health_monitor.checks": {
                "cluster_scraping": {
                    "$class": "sarc.alerts.usage_alerts.cluster_scraping:ClusterScrapingCheck",
                    "active": True,
                    "nb_stddev": -21,
                }
            }
        }
    ):
        assert cli_main(["health", "run", "--check", "cluster_scraping"]) == 0
        assert bool(
            re.search(
                r"ERROR +.+\.py:[0-9]+ +Invalid nb_stddev \(must be >= 0\) for cluster usage checking: -21",
                caplog.text,
            )
        )
        assert bool(
            re.search(
                r"ERROR +.+\.py:[0-9]+ +\[cluster_scraping] FAILURE: cluster_scraping",
                caplog.text,
            )
        )


@pytest.mark.usefixtures("empty_read_write_db")
def test_empty_database(cli_main, caplog):
    with gifnoc.overlay(
        {
            "sarc.health_monitor.checks": {
                "cluster_scraping": {
                    "$class": "sarc.alerts.usage_alerts.cluster_scraping:ClusterScrapingCheck",
                    "active": True,
                }
            }
        }
    ):
        assert cli_main(["health", "run", "--check", "cluster_scraping"]) == 0
        assert bool(
            re.search(r"ERROR +.+\.py:[0-9]+ +No jobs in database", caplog.text)
        )
        assert bool(
            re.search(
                r"ERROR +.+\.py:[0-9]+ +\[cluster_scraping] FAILURE: cluster_scraping",
                caplog.text,
            )
        )
