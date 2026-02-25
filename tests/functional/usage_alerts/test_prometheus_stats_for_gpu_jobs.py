import re

import pytest
import time_machine

from sarc.client import get_jobs
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME
from ..jobs.test_func_job_statistics import generate_fake_timeseries

PARAMS = {
    # Check with default params. In last 7 days from now (mock time: 2023-11-22),
    # there is only 2 jobs from 1 cluster in 1 timestamp, both with no GPU stats.
    # So threshold will be 0 everywhere, and no warning will be printed.
    # dict()
    "default": "prometheus_gpu_stat_default",
    # Check with no time_interval.
    # dict(time_interval=None)
    "no_time_interval": "prometheus_gpu_stat_no_time_interval",
    # Check with no time_interval and low amount of stddev (0.25).
    # dict(time_interval=None, nb_stddev=0.25)
    "std_025": "prometheus_gpu_stat_std_025",
    # Check with no time_interval, 0.25 stddev, and 1 extra cluster.
    # Expected 1 more warning, no other changes .
    # dict(time_interval=None, nb_stddev=0.25, cluster_names=["raisin", "patate", "fromage", "mila", "invisible-cluster"])
    "std_025_clusters_extra": "prometheus_gpu_stat_std_025_clusters_extra",
    # Check with no time_interval, 0.25 stddev, with only 2 clusters. Thresholds will change.
    # dict(time_interval=None, nb_stddev=0.25, cluster_names=["raisin", "mila"])
    "std_025_clusters_2": "prometheus_gpu_stat_std_025_clusters_2",
    # Check with no time_interval, 0.25 stddev, and no group_by_node.
    # dict(time_interval=None, nb_stddev=0.25, group_by_node=())
    "std_025_group_none": "prometheus_gpu_stat_std_025_group_none",
    # Check with no time_interval, 0.25 stddev, and group_by_node = False.
    # Should be same as std_025_group_none
    # dict(time_interval=None, nb_stddev=0.25, group_by_node=False)
    "std_025_group_false": "prometheus_gpu_stat_std_025_group_false",
    # Check with no time_interval, 0.25 stddev, and group_by_node for all clusters.
    # Sams as if group_by_node is not specified, as only `raisin` triggers some warnings.
    # dict(time_interval=None, nb_stddev=0.25, group_by_node=["raisin", "patate", "fromage", "mila"])
    "std_025_group_full": "prometheus_gpu_stat_std_025_group_full",
    # Check with no time_interval, 0.25 stddev, group_by_node for all clusters, and min jobs to 2.
    # dict(time_interval=None, nb_stddev=0.25, group_by_node=True, min_jobs_per_group=2)
    "std_025_group_full_min_jobs_2": "prometheus_gpu_stat_std_025_group_full_min_jobs_2",
    # Check with no time_interval, 0.25 stddev, group_by_node for all clusters,
    # and min jobs set to 3 for only `raisin`.
    # No warning, since timestamp when `raisin` triggers warnings has only 2 jobs on this cluster.
    # dict(time_interval=None, nb_stddev=0.25, group_by_node=True, min_jobs_per_group={"raisin": 3})
    "std_025_group_full_min_jobs_raisin": "prometheus_gpu_stat_std_025_group_full_min_jobs_raisin",
}


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_only_db", "health_config")
@pytest.mark.parametrize("check_name", PARAMS.values(), ids=PARAMS.keys())
def test_check_prometheus_stats_for_gpu_jobs(check_name, monkeypatch, caplog, file_regression, cli_main):
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
