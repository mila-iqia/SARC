import logging
from datetime import datetime
from typing import Callable, Sequence, TypedDict, cast

from pandas import DataFrame, Series
from prometheus_api_client.metric_range_df import MetricRangeDataFrame

from sarc.config import UTC, config
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.scraping.dcgm import DCGM_FP64_BLANK
from sarc.traces import trace_decorator

logger = logging.getLogger(__name__)


# pylint: disable=too-many-branches
@trace_decorator()
def get_job_time_series_data(
    job: SlurmJobDB,
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

    now = datetime.now(tz=UTC)

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

    logger.debug(f"prometheus query with offset: {query}")
    return config.clusters[job.cluster.name].prometheus.custom_query(query)


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
    },
)


@trace_decorator()
def compute_job_statistics_from_dataframe(
    df: DataFrame | None,
    statistics: dict[str, Callable[[Series], float]],
    normalization: Callable[[float], float] = float,
    is_time_counter: bool = False,
) -> STATS | None:
    if df is None:
        return None

    df = df.reset_index()

    # Drop DCGM BLANK sentinels (2**47 and the NOT_FOUND/NOT_SUPPORTED/
    # NOT_PERMISSIONED variants) that the GPU exporter forwards untouched
    # when a metric is unavailable; otherwise they pollute mean/max/quantiles.
    df = df[df["value"] < DCGM_FP64_BLANK]
    if df.empty:
        return None

    groupby = ["instance", "core", "gpu"]
    groupby = [col for col in groupby if col in df]

    gdf = df.groupby(groupby)

    if is_time_counter:
        # This is a time-based counter like the cpu counters in /proc/stat, with
        # a resolution of 1 nanosecond.
        timediffs = gdf["timestamp"].diff().map(lambda x: x.total_seconds())
        df["value"] = gdf["value"].diff() / timediffs / 1e9
        df = df.drop(index=0)
        # Recompute groupby after modifying df
        gdf = df.groupby(groupby)

    return {name: normalization(fn(df["value"])) for name, fn in statistics.items()}  # ty:ignore[invalid-return-type]


JOB_STATISTICS_METRIC_NAMES = (
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


@trace_decorator()
def compute_job_statistics(
    job: SlurmJobDB, prom_stats: list[dict]
) -> dict[str, JobStatisticDB]:
    statistics_dict: dict[str, Callable[[Series], float]] = {
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
    metric_to_data: dict[str, list[dict]] = {
        metric: [] for metric in JOB_STATISTICS_METRIC_NAMES
    }
    for result in prom_stats:
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
        normalization=lambda x: float(x / 100),
    )

    gpu_utilization_fp16 = compute_job_statistics_from_dataframe(
        metrics["slurm_job_fp16_gpu"],
        statistics=statistics_dict,
        normalization=lambda x: float(x / 100),
    )

    gpu_utilization_fp32 = compute_job_statistics_from_dataframe(
        metrics["slurm_job_fp32_gpu"],
        statistics=statistics_dict,
        normalization=lambda x: float(x / 100),
    )

    gpu_utilization_fp64 = compute_job_statistics_from_dataframe(
        metrics["slurm_job_fp64_gpu"],
        statistics=statistics_dict,
        normalization=lambda x: float(x / 100),
    )

    gpu_sm_occupancy = compute_job_statistics_from_dataframe(
        metrics["slurm_job_sm_occupancy_gpu"],
        statistics=statistics_dict,
        normalization=lambda x: float(x / 100),
    )

    gpu_memory = compute_job_statistics_from_dataframe(
        metrics["slurm_job_utilization_gpu_memory"],
        statistics=statistics_dict,
        normalization=lambda x: float(x / 100),
    )

    gpu_power = compute_job_statistics_from_dataframe(
        metrics["slurm_job_power_gpu"], statistics=statistics_dict
    )

    cpu_utilization = compute_job_statistics_from_dataframe(
        metrics["slurm_job_core_usage"],
        statistics=statistics_dict,
        is_time_counter=True,
    )

    system_memory = None
    if job.allocated_mem is not None:
        # NB: slurm_job_memory_usage is expressed in bytes
        # job.allocated_mem is in megabytes (multiple of 2**20 bytes)
        system_memory = compute_job_statistics_from_dataframe(
            metrics["slurm_job_memory_usage"],
            statistics=statistics_dict,
            normalization=lambda x: float(x / (2**20) / cast(int, job.allocated_mem)),
        )
    elif metrics["slurm_job_memory_usage"] is not None:
        logger.warning(
            f"job.allocated.mem is None for job {job.job_id} (job status: {job.job_state.value})"
        )

    res = dict()
    if gpu_utilization:
        res["gpu_utilization"] = JobStatisticDB(
            name="gpu_utilization", **gpu_utilization
        )
    if gpu_utilization_fp16:
        res["gpu_utilization_fp16"] = JobStatisticDB(
            name="gpu_utilization_fp16", **gpu_utilization_fp16
        )
    if gpu_utilization_fp32:
        res["gpu_utilization_fp32"] = JobStatisticDB(
            name="gpu_utilization_fp32", **gpu_utilization_fp32
        )
    if gpu_utilization_fp64:
        res["gpu_utilization_fp64"] = JobStatisticDB(
            name="gpu_utilization_fp64", **gpu_utilization_fp64
        )
    if gpu_sm_occupancy:
        res["gpu_sm_occupancy"] = JobStatisticDB(
            name="gpu_sm_occupancy", **gpu_sm_occupancy
        )
    if gpu_memory:
        res["gpu_memory"] = JobStatisticDB(name="gpu_memory", **gpu_memory)
    if gpu_power:
        res["gpu_power"] = JobStatisticDB(name="gpu_power", **gpu_power)
    if cpu_utilization:
        res["cpu_utilization"] = JobStatisticDB(
            name="cpu_utilization", **cpu_utilization
        )
    if system_memory:
        res["system_memory"] = JobStatisticDB(name="system_memory", **system_memory)
    return res


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
