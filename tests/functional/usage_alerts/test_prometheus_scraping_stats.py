import functools
from datetime import timedelta

import pytest

from sarc.alerts.usage_alerts.prometheus_stats_occurrences import (
    check_prometheus_stats_occurrences,
)
from sarc.client import get_jobs
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME

from ..jobs.test_func_job_statistics import generate_fake_timeseries
from .common import _get_warnings

get_warnings = functools.partial(
    _get_warnings,
    module="sarc.alerts.usage_alerts.prometheus_stats_occurrences:prometheus_stats_occurrences.py",
)


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db_with_many_cpu_jobs", "tzlocal_is_mtl")
@pytest.mark.parametrize(
    "params,expected",
    [
        # Check with default params. In last 7 days from now (mock time: 2023-11-22),
        # there is only 2 jobs from 1 cluster in 1 timestamp, both with no cpu_utilization
        # and no system_memory. So threshold will be 0 everywhere, and no warning will be printed.
        (dict(), []),
        # Check with no time_interval.
        (
            dict(time_interval=None),
            [
                "[2023-11-21 00:01:00-05:00][raisin] insufficient Prometheus data for cpu_utilization: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.368376726851694 (0.9230769230769231 - 2 * 0.2773500981126146); time unit: 1 day, 0:00:00"
            ],
        ),
        # Check with no time_interval and low amount of stddev (0.25), to get more warnings.
        (
            dict(time_interval=None, nb_stddev=0.25),
            [
                "[2023-02-14 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-15 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-16 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][fromage] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][patate] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][mila][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][patate] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-19 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-11-21 00:01:00-05:00][raisin] insufficient Prometheus data for cpu_utilization: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.8537393985487695 (0.9230769230769231 - 0.25 * 0.2773500981126146); time unit: 1 day, 0:00:00",
                "[2023-11-21 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
            ],
        ),
        # Check with no time_interval, 0.25 stddev, and 1 extra cluster.
        # Expected 1 more warning, no other changes .
        (
            dict(
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
            [
                "[2023-02-14 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-15 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-16 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][fromage] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][patate] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][mila][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][patate] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-19 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-11-21 00:01:00-05:00][raisin] insufficient Prometheus data for cpu_utilization: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.8537393985487695 (0.9230769230769231 - 0.25 * 0.2773500981126146); time unit: 1 day, 0:00:00",
                "[2023-11-21 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                # extra warning
                "[invisible-cluster] no Prometheus data available: no job found",
            ],
        ),
        # Check with no time_interval, 0.25 stddev, with only 2 clusters. Thresholds will change.
        (
            dict(time_interval=None, nb_stddev=0.25, cluster_names=["raisin", "mila"]),
            [
                "[2023-02-14 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.09459074466105402 (0.2 - 0.25 * 0.42163702135578396); time unit: 1 day, 0:00:00",
                "[2023-02-15 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.09459074466105402 (0.2 - 0.25 * 0.42163702135578396); time unit: 1 day, 0:00:00",
                "[2023-02-16 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.09459074466105402 (0.2 - 0.25 * 0.42163702135578396); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.09459074466105402 (0.2 - 0.25 * 0.42163702135578396); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][mila][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.09459074466105402 (0.2 - 0.25 * 0.42163702135578396); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.09459074466105402 (0.2 - 0.25 * 0.42163702135578396); time unit: 1 day, 0:00:00",
                "[2023-02-19 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.09459074466105402 (0.2 - 0.25 * 0.42163702135578396); time unit: 1 day, 0:00:00",
                "[2023-11-21 00:01:00-05:00][raisin] insufficient Prometheus data for cpu_utilization: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.8209430584957905 (0.9 - 0.25 * 0.31622776601683794); time unit: 1 day, 0:00:00",
                "[2023-11-21 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.09459074466105402 (0.2 - 0.25 * 0.42163702135578396); time unit: 1 day, 0:00:00",
            ],
        ),
        # Check with no time_interval, 0.25 stddev, and no group_by_node.
        (
            dict(time_interval=None, nb_stddev=0.25, group_by_node=()),
            [
                "[2023-02-14 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-15 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-16 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][fromage] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][patate] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                # Replaced ...
                # "[2023-02-18 00:01:00-05:00][mila][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                # ... By
                "[2023-02-18 00:01:00-05:00][mila] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][patate] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-02-19 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
                "[2023-11-21 00:01:00-05:00][raisin] insufficient Prometheus data for cpu_utilization: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.8537393985487695 (0.9230769230769231 - 0.25 * 0.2773500981126146); time unit: 1 day, 0:00:00",
                "[2023-11-21 00:01:00-05:00][raisin] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / cluster / time unit; minimum required: 0.059962701821302505 (0.15384615384615385 - 0.25 * 0.3755338080994054); time unit: 1 day, 0:00:00",
            ],
        ),
        # Check with no time_interval, 0.25 stddev, and group_by_node for all clusters.
        # Many changes.
        (
            dict(
                time_interval=None,
                nb_stddev=0.25,
                group_by_node=["raisin", "patate", "fromage", "mila"],
            ),
            [
                "[2023-02-14 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-15 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-16 00:01:00-05:00][raisin][bart] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-16 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][fromage][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][patate][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][raisin][bart] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][raisin][cn-c022] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][raisin][cn-d001] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][mila][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][patate][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-19 00:01:00-05:00][raisin][cn-b099] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-19 00:01:00-05:00][raisin][cn-c017] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-19 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-11-21 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for cpu_utilization: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.8900144875849911 (0.9473684210526315 - 0.25 * 0.22941573387056177); time unit: 1 day, 0:00:00",
                "[2023-11-21 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
            ],
        ),
        # Check with no time_interval, 0.25 stddev, group_by_node for all clusters, and min jobs to 2.
        (
            dict(
                time_interval=None,
                nb_stddev=0.25,
                group_by_node=["raisin", "patate", "fromage", "mila"],
                min_jobs_per_group=2,
            ),
            [
                "[2023-02-14 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-15 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-16 00:01:00-05:00][raisin][bart] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-16 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-17 00:01:00-05:00][fromage][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-17 00:01:00-05:00][patate][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-17 00:01:00-05:00][raisin][bart] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-17 00:01:00-05:00][raisin][cn-c022] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-17 00:01:00-05:00][raisin][cn-d001] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-18 00:01:00-05:00][mila][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-18 00:01:00-05:00][patate][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-19 00:01:00-05:00][raisin][cn-b099] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-19 00:01:00-05:00][raisin][cn-c017] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-19 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-11-21 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for cpu_utilization: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.8900144875849911 (0.9473684210526315 - 0.25 * 0.22941573387056177); time unit: 1 day, 0:00:00",
                "[2023-11-21 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
            ],
        ),
        # Check with no time_interval, 0.25 stddev, group_by_node for all clusters, and min jobs set for one cluster.
        (
            dict(
                time_interval=None,
                nb_stddev=0.25,
                group_by_node=["raisin", "patate", "fromage", "mila"],
                min_jobs_per_group={"raisin": 3},
            ),
            [
                "[2023-02-14 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-15 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-16 00:01:00-05:00][raisin][bart] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-16 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][fromage][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-17 00:01:00-05:00][patate][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-17 00:01:00-05:00][raisin][bart] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-17 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-17 00:01:00-05:00][raisin][cn-c022] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-17 00:01:00-05:00][raisin][cn-d001] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][mila][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][patate][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                "[2023-02-18 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-19 00:01:00-05:00][raisin][cn-b099] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-19 00:01:00-05:00][raisin][cn-c017] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-02-19 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
                # "[2023-11-21 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for cpu_utilization: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.8900144875849911 (0.9473684210526315 - 0.25 * 0.22941573387056177); time unit: 1 day, 0:00:00",
                # "[2023-11-21 00:01:00-05:00][raisin][cn-c021] insufficient Prometheus data for system_memory: 0.0 % of CPU jobs / node / cluster / time unit; minimum required: 0.026437715984160393 (0.10526315789473684 - 0.25 * 0.3153017676423058); time unit: 1 day, 0:00:00",
            ],
        ),
    ],
)
def test_check_prometheus_scraping_stats(params, expected, monkeypatch, caplog):
    monkeypatch.setattr(
        "sarc.jobs.series.get_job_time_series", generate_fake_timeseries
    )

    for job in get_jobs():
        job.statistics(save=True)
    check_prometheus_stats_occurrences(**params)
    assert get_warnings(caplog.text) == expected
