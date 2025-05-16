"""
Script to run sarc cli commands
with a version of get_job_time_series() based on Prometheus query range,
instead of default version based on Prometheus query with offset.

Prometheus query range allows to specify start and end time to find data,
while Prometheus query with offset uses an offset time relative to current time,
which we assume to be less precise.

We want to compare both implementations to check if they return (almost) same values.

QUERY WITH OFFSET VS QUERY RANGE
--------------------------------

1) clear cache:
    rm -rf <sarc-cache-dir>/prometheus
2) Call sarc cli normally, to generate cache:
    uv run sarc -v acquire jobs ...
3) Call this script with same parameters and SARC_CACHE=check:
    SARC_CACHE=check uv run python cli_with_query_range.py -v acquire jobs ...

First call with generate cache with default get_job_time_series() based on query offset.

Second call will use a get_job_time_series() based on query range, then,
thanks to `SARC_CACHE=check`, will compare returned data with the ones
already in cache, and will raise an exception if data differs.

QUERY RANGE TESTING
-------------------

The script can also be used to test query range itself:
1) clear cache:
    rm -rf <sarc-cache-dir>/prometheus
2) Run this script once:
    uv run python cli_with_query_range.py -v acquire jobs ...
3) Run this script again with SARC_CACHE=check:
    SARC_CACHE=check uv run python cli_with_query_range.py -v acquire jobs ...

This will then compare live query range results with cached query range data,
allowing to check if query range is constant.

CURRENT REMARKS
---------------

CURRENT OBSERVATIONS:
- Query with offset and query range seems to return same results in most cases.
- Query range itself is not constant neither

CURRENT CONCLUSION:
It seems not worth to use query range, at least with current implementation.
"""

import logging
from datetime import datetime, timedelta
from typing import Sequence, Union
from unittest import mock

from sarc.cli import main
from sarc.client.job import SlurmJob
from sarc.config import MTL, UTC
from sarc.jobs import get_job_time_series_metric_names
from sarc.traces import trace_decorator


@trace_decorator()
def _get_job_time_series_data_using_query_range(
    job: SlurmJob,
    metric: Union[str, Sequence[str]],
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: str = "total",
):
    """
    Implementation of get_job_time_series() using Prometheus query range.

    Arguments:
        job: The job for which to fetch metrics.
        metric: The metric or list of metrics, which must be in ``slurm_job_metric_names``.
        min_interval: The minimal reporting interval, in seconds.
        max_points: The maximal number of data points to return.
        measure: The aggregation measure to use ("avg_over_time", etc.)
            A format string can be passed, e.g. ("quantile_over_time(0.5, {})")
            to get the median.
        aggregation: Either "total", to aggregate over the whole range, or
            "interval", to aggregate over each interval.
    """
    metrics = [metric] if isinstance(metric, str) else metric
    if not metrics:
        raise ValueError("No metrics given")
    for m in metrics:
        if m not in get_job_time_series_metric_names():
            raise ValueError(f"Unknown metric name: {m}")
    if aggregation not in ("interval", "total", None):
        raise ValueError(
            f"Aggregation must be one of ['total', 'interval', None]: {aggregation}"
        )

    if job.job_state != "RUNNING" and not job.elapsed_time:
        return []

    if len(metrics) == 1:
        (prefix,) = metrics
        label_exprs = []
    else:
        prefix = ""
        label_exprs = [f'__name__=~"^({"|".join(metrics)})$"']

    label_exprs.append(f'slurmjobid="{job.job_id}"')
    selector = prefix + "{" + ", ".join(label_exprs) + "}"

    now = datetime.now(tz=UTC).astimezone(MTL)

    if job.end_time and job.end_time <= now:
        duration = job.end_time - job.start_time
        end_time = job.end_time
    else:
        # Duration should not be looking in the future
        duration = now - job.start_time
        end_time = now

    duration_seconds = int(duration.total_seconds())

    if duration_seconds <= 0:
        return []

    interval = int(max(duration_seconds / max_points, min_interval))

    if measure and aggregation:
        # NB: With current usage of get_job_time_series(),
        # this if-block is never tested.

        if aggregation == "interval":
            range_seconds = interval
        elif aggregation == "total":
            range_seconds = duration_seconds
        else:
            raise ValueError(f"Unknown aggregation: {aggregation}")

        selector_with_range = f"{selector}[{range_seconds}s]"
        if "(" in measure:
            # NB: This case is never used nor tested anywhere
            nested_query = measure.format(selector_with_range)
        else:
            nested_query = f"{measure}({selector_with_range})"
        query = f"{nested_query}[{duration_seconds}s:{range_seconds}s]"
        # Query range must cover only range_seconds from end_time.
        start_time = end_time - timedelta(seconds=range_seconds)
        step_seconds = range_seconds
    else:
        query = selector
        # Query range must cover entire job time.
        start_time = job.start_time
        step_seconds = interval

    logging.info(
        f"prometheus query range: {query} "
        f"start={start_time} end={end_time} (now? {end_time == now}) step={step_seconds}"
    )
    return job.fetch_cluster_config().prometheus.custom_query_range(
        query=query, start_time=start_time, end_time=end_time, step=f"{step_seconds}s"
    )


def patched_main():
    with mock.patch(
        "sarc.jobs.series._get_job_time_series_data",
        new=_get_job_time_series_data_using_query_range,
    ):
        check_mock()
        returncode = main()
        if returncode > 0:
            raise SystemExit(returncode)


def check_mock():
    from sarc.jobs.series import _get_job_time_series_data

    assert _get_job_time_series_data is _get_job_time_series_data_using_query_range


if __name__ == "__main__":
    patched_main()
