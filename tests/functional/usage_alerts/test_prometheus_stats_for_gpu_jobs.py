import functools
import re

import pytest

from sarc.alerts.usage_alerts.prometheus_stats_occurrences import (
    check_prometheus_stats_for_gpu_jobs,
)
from sarc.client import get_jobs
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME

from ..jobs.test_func_job_statistics import generate_fake_timeseries

PARAMS = {
    # Check with default params. In last 7 days from now (mock time: 2023-11-22),
    # there is only 2 jobs from 1 cluster in 1 timestamp, both with no GPU stats.
    # So threshold will be 0 everywhere, and no warning will be printed.
    "default": dict(),
    # Check with no time_interval.
    "no_time_interval": dict(time_interval=None),
    # Check with no time_interval and low amount of stddev (0.25).
    "std_025": dict(time_interval=None, nb_stddev=0.25),
    # Check with no time_interval, 0.25 stddev, and 1 extra cluster.
    # Expected 1 more warning, no other changes .
    "std_025_clusters_extra": dict(
        time_interval=None,
        nb_stddev=0.25,
        cluster_names=[
            "raisin",
            "patate",
            "fromage",
            "mila",
            "invisible-cluster",
        ],
    ),
    # Check with no time_interval, 0.25 stddev, with only 2 clusters. Thresholds will change.
    "std_025_clusters_2": dict(
        time_interval=None, nb_stddev=0.25, cluster_names=["raisin", "mila"]
    ),
    # Check with no time_interval, 0.25 stddev, and no group_by_node.
    "std_025_group_none": dict(time_interval=None, nb_stddev=0.25, group_by_node=()),
    # Check with no time_interval, 0.25 stddev, and group_by_node for all clusters.
    # Sams as if group_by_node is not specified, as only `raisin` triggers some warnings.
    "std_025_group_full": dict(
        time_interval=None,
        nb_stddev=0.25,
        group_by_node=["raisin", "patate", "fromage", "mila"],
    ),
    # Check with no time_interval, 0.25 stddev, group_by_node for all clusters, and min jobs to 2.
    "std_025_group_full_min_jobs_2": dict(
        time_interval=None,
        nb_stddev=0.25,
        group_by_node=["raisin", "patate", "fromage", "mila"],
        min_jobs_per_group=2,
    ),
    # Check with no time_interval, 0.25 stddev, group_by_node for all clusters,
    # and min jobs set to 2 for only `raisin`.
    # No warning, since timestamp when `raisin` triggers warnings has only 2 jobs on this cluster.
    "std_025_group_full_min_jobs_raisin": dict(
        time_interval=None,
        nb_stddev=0.25,
        group_by_node=["raisin", "patate", "fromage", "mila"],
        min_jobs_per_group={"raisin": 3},
    ),
}


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", PARAMS.values(), ids=PARAMS.keys())
def test_check_prometheus_stats_for_gpu_jobs(
    params, monkeypatch, caplog, file_regression
):
    monkeypatch.setattr(
        "sarc.jobs.series.get_job_time_series", generate_fake_timeseries
    )

    for job in get_jobs():
        job.statistics(save=True)
    check_prometheus_stats_for_gpu_jobs(**params)
    file_regression.check(
        re.sub(
            r"WARNING +sarc\.alerts\.usage_alerts\.prometheus_stats_occurrences:prometheus_stats_occurrences.py:[0-9]+ +",
            "",
            caplog.text,
        )
    )
