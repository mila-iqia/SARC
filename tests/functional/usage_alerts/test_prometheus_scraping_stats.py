import re

import pytest
import time_machine

from sarc.client import get_jobs
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME
from ..jobs.test_func_job_statistics import generate_fake_timeseries

PARAMS = [
    # Check with default params. In last 7 days from now (mock time: 2023-11-22),
    # there is only 2 jobs from 1 cluster in 1 timestamp, both with no cpu_utilization
    # and no system_memory. So threshold will be 0 everywhere, and no warning will be printed.
    # dict()
    "prometheus_cpu_stat_0",
    # Check with no time_interval.
    # dict(time_interval=None)
    "prometheus_cpu_stat_1",
    # Check with no time_interval and low amount of stddev (0.25), to get more warnings.
    # dict(time_interval=None, nb_stddev=0.25)
    "prometheus_cpu_stat_2",
    # Check with no time_interval, 0.25 stddev, and 1 extra cluster.
    # Expected 1 more warning, no other changes.
    # dict(time_interval=None, nb_stddev=0.25, cluster_names=["raisin", "patate", "fromage", "mila", "invisible-cluster"])
    "prometheus_cpu_stat_3",
    # Check with no time_interval, 0.25 stddev, with only 2 clusters. Thresholds will change.
    # dict(time_interval=None, nb_stddev=0.25, cluster_names=["raisin", "mila"])
    "prometheus_cpu_stat_4",
    # Check with no time_interval, 0.25 stddev, and no group_by_node.
    # dict(time_interval=None, nb_stddev=0.25, group_by_node=())
    "prometheus_cpu_stat_5",
    # Check with no time_interval, 0.25 stddev, and group_by_node for all clusters.
    # Many changes.
    # dict(time_interval=None, nb_stddev=0.25, group_by_node=["raisin", "patate", "fromage", "mila"])
    "prometheus_cpu_stat_6",
    # Check with no time_interval, 0.25 stddev, group_by_node for all clusters, and min jobs to 2.
    # dict(time_interval=None, nb_stddev=0.25, group_by_node=["raisin", "patate", "fromage", "mila"], min_jobs_per_group=2)
    "prometheus_cpu_stat_7",
    # Check with no time_interval, 0.25 stddev, group_by_node for all clusters, and min jobs set for one cluster.
    # dict(time_interval=None, nb_stddev=0.25, group_by_node=["raisin", "patate", "fromage", "mila"], min_jobs_per_group={"raisin": 3})
    "prometheus_cpu_stat_8",
    # Check with no time_interval, 0.25 stddev, group_by_node for all clusters (group_by_node = True),
    # and min jobs set for one cluster.
    # dict(time_interval=None, nb_stddev=0.25, group_by_node=True, min_jobs_per_group={"raisin": 3})
    "prometheus_cpu_stat_9",
]


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_only_db_with_many_cpu_jobs", "health_config")
@pytest.mark.parametrize(
    "check_name", PARAMS, ids=[f"params{i}" for i in range(len(PARAMS))]
)
def test_check_prometheus_scraping_stats(
    check_name, monkeypatch, caplog, file_regression, cli_main
):
    monkeypatch.setattr(
        "sarc.jobs.series.get_job_time_series", generate_fake_timeseries
    )

    for job in get_jobs():
        job.statistics(save=True)

    assert cli_main(["health", "run", "--check", check_name]) == 0
    file_regression.check(
        re.sub(
            r"ERROR +sarc\.alerts\.usage_alerts\.prometheus_stats_occurrences:prometheus_stats_occurrences.py:[0-9]+ +",
            "",
            caplog.text,
        )
    )
