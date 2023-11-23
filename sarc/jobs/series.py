from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable

import numpy as np
import pandas
from pandas import DataFrame
from prometheus_api_client import MetricRangeDataFrame
from tqdm import tqdm

from sarc.config import MTL, UTC
from sarc.jobs.job import JobStatistics, Statistics, count_jobs, get_jobs

if TYPE_CHECKING:
    from sarc.jobs.sacct import SlurmJob


# pylint: disable=too-many-branches
def get_job_time_series(
    job: SlurmJob,
    metric: str,
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: str = "total",
    dataframe: bool = True,
):
    """Fetch job metrics.

    Arguments:
        cluster: The cluster on which to fetch metrics.
        job: The job for which to fetch metrics.
        metric: The metric, which must be in ``slurm_job_metric_names``.
        min_interval: The minimal reporting interval, in seconds.
        max_points: The maximal number of data points to return.
        measure: The aggregation measure to use ("avg_over_time", etc.)
            A format string can be passed, e.g. ("quantile_over_time(0.5, {})")
            to get the median.
        aggregation: Either "total", to aggregate over the whole range, or
            "interval", to aggregate over each interval.
        dataframe: If True, return a DataFrame. Otherwise, return the list of
            dicts returned by Prometheus's API.
    """

    if aggregation not in ("interval", "total", None):
        raise ValueError(
            f"Aggregation must be one of ['total', 'interval', None]: {aggregation}"
        )

    if job.job_state != "RUNNING" and not job.elapsed_time:
        return None if dataframe else []
    if metric not in slurm_job_metric_names:
        raise ValueError(f"Unknown metric name: {metric}")

    selector = f'{metric}{{slurmjobid=~"{job.job_id}"}}'

    now = datetime.now(tz=UTC).astimezone(MTL)

    ago = now - job.start_time
    duration = (job.end_time or now) - job.start_time

    offset = int((ago - duration).total_seconds())
    offset_string = f" offset {offset}s" if offset > 0 else ""

    duration_seconds = int(duration.total_seconds())

    # Duration should not be looking in the future
    if offset < 0:
        duration_seconds += offset

    if duration_seconds <= 0:
        return None if dataframe else []

    interval = int(max(duration_seconds / max_points, min_interval))

    query = selector

    if measure and aggregation:
        if aggregation == "interval":
            range_seconds = interval
        elif aggregation == "total":
            offset += duration_seconds
            range_seconds = duration_seconds

        query = f"{query}[{range_seconds}s]"
        if "(" in measure:
            query = measure.format(f"{query} {offset_string}")
        else:
            query = f"{measure}({query} {offset_string})"
        query = f"{query}[{duration_seconds}s:{range_seconds}s]"
    else:
        query = f"{query}[{duration_seconds}s:{interval}s] {offset_string}"

    results = job.fetch_cluster_config().prometheus.custom_query(query)
    if dataframe:
        return MetricRangeDataFrame(results) if results else None
    else:
        return results


def get_job_time_series_metric_names():
    """Return all the metric names that relate to slurm jobs."""
    return slurm_job_metric_names


def compute_job_statistics_from_dataframe(
    df: DataFrame,
    statistics,
    normalization=float,
    unused_threshold=0.01,
    is_time_counter=False,
):
    df = df.reset_index()

    groupby = ["instance", "core", "gpu"]
    groupby = [col for col in groupby if col in df]

    gdf = df.groupby(groupby)

    if is_time_counter:
        # This is a time-based counter like the cpu counters in /proc/stat, with
        # a resolution of 1 nanosecond.
        df["timediffs"] = gdf["timestamp"].diff().map(lambda x: x.total_seconds())
        df["value"] = gdf["value"].diff() / df["timediffs"] / 1e9
        df = df.drop(index=0)

    if unused_threshold is not None:
        means = gdf["value"].mean()
        n_unused = (means < unused_threshold).astype(bool).sum()

        df_with_means = pandas.merge(
            df, means.reset_index().rename(columns={"value": "mean"}), on=groupby
        )

        df = df_with_means[df_with_means["mean"] >= unused_threshold]
    else:
        n_unused = 0

    rval = {name: normalization(fn(df["value"])) for name, fn in statistics.items()}
    return {**rval, "unused": n_unused}


def compute_job_statistics_one_metric(
    job: SlurmJob,
    metric_name,
    statistics,
    normalization=float,
    unused_threshold=0.01,
    is_time_counter=False,
):
    df = job.series(metric=metric_name, max_points=10_000)
    if df is None:
        return None
    return compute_job_statistics_from_dataframe(
        df=df,
        statistics=statistics,
        normalization=normalization,
        unused_threshold=unused_threshold,
        is_time_counter=is_time_counter,
    )


def compute_job_statistics(job: SlurmJob):
    statistics_dict = {
        "mean": DataFrame.mean,
        "std": DataFrame.std,
        "max": DataFrame.max,
        "q25": lambda self: self.quantile(0.25),
        "median": DataFrame.median,
        "q75": lambda self: self.quantile(0.75),
        "q05": lambda self: self.quantile(0.05),
    }

    gpu_utilization = compute_job_statistics_one_metric(
        job,
        "slurm_job_utilization_gpu",
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_memory = compute_job_statistics_one_metric(
        job,
        "slurm_job_utilization_gpu_memory",
        statistics=statistics_dict,
        normalization=lambda x: float(x / 100),
        unused_threshold=None,
    )

    gpu_power = compute_job_statistics_one_metric(
        job,
        "slurm_job_power_gpu",
        statistics=statistics_dict,
        unused_threshold=None,
    )

    cpu_utilization = compute_job_statistics_one_metric(
        job,
        "slurm_job_core_usage",
        statistics=statistics_dict,
        unused_threshold=0.01,
        is_time_counter=True,
    )

    system_memory = compute_job_statistics_one_metric(
        job,
        "slurm_job_memory_usage",
        statistics=statistics_dict,
        normalization=lambda x: float(x / 1e6 / job.allocated.mem),
        unused_threshold=None,
    )

    return JobStatistics(
        gpu_utilization=gpu_utilization and Statistics(**gpu_utilization),
        gpu_memory=gpu_memory and Statistics(**gpu_memory),
        gpu_power=gpu_power and Statistics(**gpu_power),
        cpu_utilization=cpu_utilization and Statistics(**cpu_utilization),
        system_memory=system_memory and Statistics(**system_memory),
    )


DUMMY_STATS = {
    label: np.nan
    for label in [
        "gpu_utilization",
        "cpu_utilization",
        "gpu_memory",
        "gpu_power",
        "system_memory",
    ]
}


# pylint: disable=too-many-statements,fixme
def load_job_series(
    *,
    fields: None | list[str] | dict[str, str] = None,
    clip_time: bool = False,
    callback: None | Callable = None,
    **jobs_args,
) -> pandas.DataFrame:
    """
    Query jobs from the database and return them in a DataFrame, including full user info
    for each job.

    Parameters
    ----------
    fields: list or dict
        Job fields to include in the DataFrame. By default, include all fields.
        A dictionary may be passed to select fields and rename them in the DataFrame.
        In such case, the keys are the fields' names and the values are the names
        they will have in the DataFrame.
    clip_time: bool
        Whether the duration time of the jobs should be clipped within `start` and `end`.
        ValueError will be raised if `clip_time` is True and either of `start` or `end` is None. 
        Defaults to False.
    callback: Callable
        Callable taking the list of job dictionaries in the format it would be included in the DataFrame.
    **jobs_args
        Arguments to be passed to `get_jobs` to query jobs from the database.
    """

    # If fields is a list, convert it to a renaming dict with same old and new names.
    if isinstance(fields, list):
        fields = {key: key for key in fields}

    start = jobs_args.get("start", None)
    end = jobs_args.get("end", None)

    total = count_jobs(**jobs_args)

    rows = []
    now = datetime.now(tz=MTL)
    # Fetch all jobs from the clusters
    for job in tqdm(get_jobs(**jobs_args), total=total, desc="load job series"):
        if job.end_time is None:
            job.end_time = now

        # For some reason start time is not reliable, often equal to submit time,
        # so we infer it based on end_time and elapsed_time.
        job.start_time = job.end_time - timedelta(seconds=job.elapsed_time)

        unclipped_start = None
        unclipped_end = None
        if clip_time:
            if start is None:
                raise ValueError("Clip time: missing start")
            if end is None:
                raise ValueError("Clip time: missing end")
            # Clip the job to the time range we are interested in.
            # NOTE: This should perhaps be helper function for dataframes so that we don't force clipping raw data
            #       during loading.
            unclipped_start = job.start_time
            job.start_time = max(job.start_time, start)
            unclipped_end = job.end_time
            job.end_time = min(job.end_time, end)
            # Could be negative if job started after end. We don't want to filter
            # them out because they have been submitted before end, and we want to
            # compute their wait time.
            job.elapsed_time = max((job.end_time - job.start_time).total_seconds(), 0)

        if job.stored_statistics is None:
            job_series = DUMMY_STATS.copy()
        else:
            job_series = job.stored_statistics.dict()
            job_series = {k: select_stat(k, v) for k, v in job_series.items()}

        # TODO: Why is it possible to have billing smaller than gres_gpu???
        billing = job.allocated.billing or 0
        gres_gpu = job.allocated.gres_gpu or 0
        if gres_gpu:
            job_series["gpu_allocated"] = max(billing, gres_gpu)
            job_series["cpu"] = job.allocated.cpu
        else:
            job_series["gpu_allocated"] = 0
            job_series["cpu"] = (
                max(billing, job.allocated.cpu) if job.allocated.cpu else 0
            )
        job_series["gpu_requested"] = gres_gpu
        job_series["duration"] = job.elapsed_time
        job_series["mem"] = job.allocated.mem
        job_series["submit"] = job.submit_time
        job_series["start"] = job.start_time
        job_series["end"] = job.end_time
        if clip_time:
            job_series["unclipped_start"] = unclipped_start
            job_series["unclipped_end"] = unclipped_end

        job_series.update(job.dict())

        if fields is not None:
            job_series = {
                new_name: job_series[old_name] for old_name, new_name in fields.items()
            }
        rows.append(job_series)
        if callback:
            callback(rows)

    return pandas.DataFrame(rows)


def select_stat(name, dist):
    if not dist:
        return np.nan

    if name in ["system_memory", "gpu_memory"]:
        return dist["max"]

    return dist["median"]


slurm_job_metric_names = [
    "slurm_job_core_usage",
    "slurm_job_core_usage_total",
    "slurm_job_fp16_gpu",
    "slurm_job_fp32_gpu",
    "slurm_job_fp64_gpu",
    "slurm_job_memory_active_file",
    "slurm_job_memory_cache",
    "slurm_job_memory_inactive_file",
    "slurm_job_memory_limit",
    "slurm_job_memory_mapped_file",
    "slurm_job_memory_max",
    "slurm_job_memory_rss",
    "slurm_job_memory_rss_huge",
    "slurm_job_memory_unevictable",
    "slurm_job_memory_usage",
    "slurm_job_memory_usage_gpu",
    "slurm_job_nvlink_gpu",
    "slurm_job_nvlink_gpu_total",
    "slurm_job_pcie_gpu",
    "slurm_job_pcie_gpu_total",
    "slurm_job_power_gpu",
    "slurm_job_process_count",
    "slurm_job_sm_occupancy_gpu",
    "slurm_job_states",
    "slurm_job_tensor_gpu",
    "slurm_job_threads_count",
    "slurm_job_utilization_gpu",
    "slurm_job_utilization_gpu_memory",
]
