from __future__ import annotations

import logging
from datetime import datetime
from typing import Callable, Literal, Sequence, TypedDict, overload

import pandas
from pandas import DataFrame, Series
from prometheus_api_client.metric_range_df import MetricRangeDataFrame

from sarc.cache import with_cache
from sarc.client.job import JobStatistics, SlurmJob, Statistics
from sarc.config import MTL, UTC
from sarc.traces import trace_decorator


@overload
def get_job_time_series(
    job: SlurmJob,
    metric: str | Sequence[str],
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: Literal["total", "interval"] | None = "total",
    dataframe: Literal[True] = True,
) -> DataFrame | None: ...


@overload
def get_job_time_series(
    job: SlurmJob,
    metric: str | Sequence[str],
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: Literal["total", "interval"] | None = "total",
    dataframe: Literal[False] = False,
) -> list[dict]: ...


# pylint: disable=too-many-branches
@trace_decorator()
def get_job_time_series(
    job: SlurmJob,
    metric: str | Sequence[str],
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: Literal["total", "interval"] | None = "total",
    dataframe: bool = True,
) -> DataFrame | list[dict] | None:
    """Fetch job metrics.

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
        dataframe: If True, return a DataFrame. Otherwise, return the list of
            dicts returned by Prometheus's API.
    """
    results = with_cache(
        _get_job_time_series_data,
        key=_get_job_time_series_data_cache_key,
        subdirectory="prometheus",
    )(
        job=job,
        metric=metric,
        min_interval=min_interval,
        max_points=max_points,
        measure=measure,
        aggregation=aggregation,
        # cache_policy is None,
        # so that it can be set
        # with env var SARC_CACHE
        cache_policy=None,
    )
    if dataframe:
        return MetricRangeDataFrame(results) if results else None
    else:
        return results


# pylint: disable=too-many-branches
@trace_decorator()
def _get_job_time_series_data(
    job: SlurmJob,
    metric: str | Sequence[str],
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: str = "total",
) -> list:
    """Fetch job metrics.

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
        if m not in slurm_job_metric_names:
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

    if job.start_time is None:
        raise ValueError("Job hasn't started yet")

    ago = now - job.start_time
    duration = (job.end_time or now) - job.start_time

    offset = int((ago - duration).total_seconds())
    offset_string = f" offset {offset}s" if offset > 0 else ""

    duration_seconds = int(duration.total_seconds())

    # Duration should not be looking in the future
    if offset < 0:
        duration_seconds += offset

    if duration_seconds <= 0:
        return []

    interval = int(max(duration_seconds / max_points, min_interval))

    query = selector

    if measure and aggregation:
        if aggregation == "interval":
            range_seconds = interval
        elif aggregation == "total":
            range_seconds = duration_seconds
        else:
            raise ValueError(f"Unknown aggregation: {aggregation}")

        query = f"{query}[{range_seconds}s]"
        if "(" in measure:
            query = measure.format(f"{query} {offset_string}")
        else:
            query = f"{measure}({query} {offset_string})"
        query = f"{query}[{duration_seconds}s:{range_seconds}s]"
    else:
        query = f"{query}[{duration_seconds}s:{interval}s] {offset_string}"

    logging.debug(f"prometheus query with offset: {query}")
    return job.fetch_cluster_config().prometheus.custom_query(query)


def _get_job_time_series_data_cache_key(
    job: SlurmJob,
    metric: str | Sequence[str],
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: str = "total",
) -> str | None:
    metrics = [metric] if isinstance(metric, str) else sorted(metric)
    if (
        not metrics
        or any(m not in slurm_job_metric_names for m in metrics)
        or aggregation not in ("interval", "total", None)
        or (job.job_state != "RUNNING" and not job.elapsed_time)
    ):
        # We don't cache for exception cases or special cases
        # from _get_job_time_series_data()
        return None

    if job.end_time is None:
        # If job.end_time is None, then Prometheus queries
        # are based on current time (now).
        # We should not cache such results.
        return None

    job_start_time = job.start_time
    assert job_start_time is not None
    fmt = "%Y-%m-%dT%Hh%Mm%Ss"
    return (
        f"{job.cluster_name}"
        f".{job.job_id}"
        f".{job_start_time.strftime(fmt)}_to_{job.end_time.strftime(fmt)}"
        # To reduce key size, we use short metric names
        # from dictionary `slurm_job_metric_names`
        f".{'+'.join(slurm_job_metric_names[m] for m in metrics)}"
        f".min-itv-{min_interval}s"
        f".max-pts-{max_points}"
        f".{f'measure-{measure}-{aggregation}' if measure and aggregation else 'no_measure'}"
        f".json"
    )


def get_job_time_series_metric_names() -> dict[str, str]:
    """Return all the metric names that relate to slurm jobs."""
    return slurm_job_metric_names


STATS = TypedDict(
    "STATS",
    {
        "mean": float,
        "std": float,
        "max": float,
        "q25": float,
        "median": float,
        "q75": float,
        "q05": float,
        "unused": int,
    },
)


@trace_decorator()
def compute_job_statistics_from_dataframe(
    df: DataFrame | None,
    statistics: dict[str, Callable[[Series], float]],
    normalization: Callable[[float], float] = float,
    unused_threshold: float | None = 0.01,
    is_time_counter: bool = False,
) -> STATS | None:
    if df is None:
        return None

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
    return {**rval, "unused": n_unused}  # type: ignore[return-value, typeddict-item]


def compute_job_statistics_one_metric(
    job: SlurmJob,
    metric_name: str,
    statistics: dict[str, Callable[[Series], float]],
    normalization: Callable[[float], float] = float,
    unused_threshold: float | None = 0.01,
    is_time_counter: bool = False,
) -> STATS | None:
    df = job.series(metric=metric_name, max_points=10_000, dataframe=True)
    if df is None:
        return None
    return compute_job_statistics_from_dataframe(
        df=df,
        statistics=statistics,
        normalization=normalization,
        unused_threshold=unused_threshold,
        is_time_counter=is_time_counter,
    )


@trace_decorator()
def compute_job_statistics(job: SlurmJob) -> JobStatistics:
    statistics_dict = {
        "mean": lambda self: self.mean(),
        "std": lambda self: self.std(),
        "max": lambda self: self.max(),
        "q25": lambda self: self.quantile(0.25),
        "median": lambda self: self.median(),
        "q75": lambda self: self.quantile(0.75),
        "q05": lambda self: self.quantile(0.05),
    }

    # We will get all required job time series
    # with just 1 call to get_job_time_series()
    metric_names = (
        "slurm_job_utilization_gpu",
        "slurm_job_fp16_gpu",
        "slurm_job_fp32_gpu",
        "slurm_job_fp64_gpu",
        "slurm_job_sm_occupancy_gpu",
        "slurm_job_utilization_gpu_memory",
        "slurm_job_power_gpu",
        "slurm_job_core_usage",
        "slurm_job_memory_usage",
    )
    metric_to_data: dict[str, list[dict]] = {metric: [] for metric in metric_names}
    for result in get_job_time_series(
        job, metric_names, max_points=10_000, dataframe=False
    ):
        metric_to_data[result["metric"]["__name__"]].append(result)
    # Then we convert series to data frames for each metric
    metrics = {
        metric: MetricRangeDataFrame(results) if results else None
        for metric, results in metric_to_data.items()
    }
    # Now we can use data frames to compute statistics for each metric,
    # by directly using compute_job_statistics_from_dataframe().

    gpu_utilization = compute_job_statistics_from_dataframe(
        metrics["slurm_job_utilization_gpu"],
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_utilization_fp16 = compute_job_statistics_from_dataframe(
        metrics["slurm_job_fp16_gpu"],
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_utilization_fp32 = compute_job_statistics_from_dataframe(
        metrics["slurm_job_fp32_gpu"],
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_utilization_fp64 = compute_job_statistics_from_dataframe(
        metrics["slurm_job_fp64_gpu"],
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_sm_occupancy = compute_job_statistics_from_dataframe(
        metrics["slurm_job_sm_occupancy_gpu"],
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_memory = compute_job_statistics_from_dataframe(
        metrics["slurm_job_utilization_gpu_memory"],
        statistics=statistics_dict,
        normalization=lambda x: float(x / 100),
        unused_threshold=False,
    )

    gpu_power = compute_job_statistics_from_dataframe(
        metrics["slurm_job_power_gpu"],
        statistics=statistics_dict,
        unused_threshold=False,
    )

    cpu_utilization = compute_job_statistics_from_dataframe(
        metrics["slurm_job_core_usage"],
        statistics=statistics_dict,
        unused_threshold=0.01,
        is_time_counter=True,
    )

    if job.allocated.mem is None:
        logging.warning(f"job.allocated.mem is None for job {job.job_id}")
    system_memory = compute_job_statistics_from_dataframe(
        metrics["slurm_job_memory_usage"],
        statistics=statistics_dict,
        normalization=lambda x: float(x / 1e6 / job.allocated.mem),
        unused_threshold=False,
    )

    return JobStatistics(
        gpu_utilization=Statistics(**gpu_utilization) if gpu_utilization else None,
        gpu_utilization_fp16=(
            Statistics(**gpu_utilization_fp16) if gpu_utilization_fp16 else None
        ),
        gpu_utilization_fp32=(
            Statistics(**gpu_utilization_fp32) if gpu_utilization_fp32 else None
        ),
        gpu_utilization_fp64=(
            Statistics(**gpu_utilization_fp64) if gpu_utilization_fp64 else None
        ),
        gpu_sm_occupancy=Statistics(**gpu_sm_occupancy) if gpu_sm_occupancy else None,
        gpu_memory=Statistics(**gpu_memory) if gpu_memory else None,
        gpu_power=Statistics(**gpu_power) if gpu_power else None,
        cpu_utilization=Statistics(**cpu_utilization) if cpu_utilization else None,
        system_memory=Statistics(**system_memory) if system_memory else None,
    )


# Dictionary of slurm metric names:
# We both list allowed metric names as key,
# and we map each metric to a short name,
# intended to be used to generate short cache key
# for get_job_time_series().
slurm_job_metric_names = {
    "slurm_job_core_usage": "cu",
    "slurm_job_core_usage_total": "cut",
    "slurm_job_fp16_gpu": "f16g",
    "slurm_job_fp32_gpu": "f32g",
    "slurm_job_fp64_gpu": "f64g",
    "slurm_job_memory_active_file": "maf",
    "slurm_job_memory_cache": "mc",
    "slurm_job_memory_inactive_file": "mif",
    "slurm_job_memory_limit": "ml",
    "slurm_job_memory_mapped_file": "mmf",
    "slurm_job_memory_max": "mm",
    "slurm_job_memory_rss": "mr",
    "slurm_job_memory_rss_huge": "mrh",
    "slurm_job_memory_unevictable": "mun",
    "slurm_job_memory_usage": "mus",
    "slurm_job_memory_usage_gpu": "mug",
    "slurm_job_nvlink_gpu": "ng",
    "slurm_job_nvlink_gpu_total": "ngt",
    "slurm_job_pcie_gpu": "pcg",
    "slurm_job_pcie_gpu_total": "pgt",
    "slurm_job_power_gpu": "pwg",
    "slurm_job_process_count": "pc",
    "slurm_job_sm_occupancy_gpu": "sog",
    "slurm_job_states": "s",
    "slurm_job_tensor_gpu": "tg",
    "slurm_job_threads_count": "tc",
    "slurm_job_utilization_gpu": "ug",
    "slurm_job_utilization_gpu_memory": "ugm",
}
# We check that short names are unique and cover all metrics.
assert len(set(slurm_job_metric_names.values())) == len(slurm_job_metric_names)
