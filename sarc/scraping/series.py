import logging
import math
from datetime import datetime
from typing import Callable, Sequence, TypedDict, cast

import numpy as np

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


# Labels that identify one physical source (node, CPU core, GPU) within a
# job's series; time counters are differenced per source.
_COUNTER_GROUP_LABELS = ("instance", "core", "gpu")


def _filtered_points(series: dict) -> tuple[np.ndarray, np.ndarray]:
    """Timestamps and values of one raw Prometheus series, real samples only.

    Drops the DCGM BLANK sentinels (2**47 and the NOT_FOUND/NOT_SUPPORTED/
    NOT_PERMISSIONED variants) that the GPU exporter forwards untouched when a
    metric is unavailable, as well as NaN samples: any comparison with NaN is
    False, so `value < DCGM_FP64_BLANK` discards both.
    """
    points = series["values"]
    timestamps = np.fromiter((p[0] for p in points), dtype=float, count=len(points))
    values = np.array([p[1] for p in points], dtype=float)
    keep = values < DCGM_FP64_BLANK
    return timestamps[keep], values[keep]


def _counter_rates(results: Sequence[dict]) -> np.ndarray | None:
    """Pooled per-second rates of a nanosecond time counter (e.g. core usage).

    Series are grouped by their (instance, core, gpu) labels — series sharing
    the same labels are concatenated in input order — and each group is
    differenced separately. Returns None when no real sample survives the
    sentinel filter, and an empty array when the groups are all too short to
    difference (statistics then come out NaN).
    """
    used = [k for k in _COUNTER_GROUP_LABELS if any(k in s["metric"] for s in results)]
    groups: dict[tuple, list[tuple[np.ndarray, np.ndarray]]] = {}
    n_samples = 0
    for series in results:
        labels = series["metric"]
        try:
            key = tuple(labels[k] for k in used)
        except KeyError:
            continue  # a group label is missing: these samples are not attributable
        timestamps, values = _filtered_points(series)
        n_samples += values.size
        if values.size:
            groups.setdefault(key, []).append((timestamps, values))
    if n_samples == 0:
        return None
    rates = []
    for chunks in groups.values():
        timestamps = np.concatenate([c[0] for c in chunks])
        values = np.concatenate([c[1] for c in chunks])
        if values.size >= 2:
            # 1-nanosecond resolution, like the cpu counters in /proc/stat.
            rates.append(np.diff(values) / np.diff(timestamps) / 1e9)
    return np.concatenate(rates) if rates else np.empty(0)


@trace_decorator()
def compute_metric_statistics(
    results: Sequence[dict],
    normalization: Callable[[float], float] = float,
    is_time_counter: bool = False,
) -> STATS | None:
    """Compute the stored statistics of one metric's raw Prometheus series.

    Arguments:
        results: The raw `custom_query` result list for a single metric:
            dicts with a "metric" labels mapping and a "values" list of
            [timestamp, value] samples.
        normalization: Applied to each computed statistic.
        is_time_counter: The metric is a monotonic nanosecond counter:
            statistics are computed on its per-second rate instead of its
            raw values.

    Values are pooled across all the metric's series. Returns None when no
    usable sample remains. std uses ddof=1 (NaN for a single sample) and
    quantiles interpolate linearly, matching the former pandas reductions.
    """
    if not results:
        return None
    if is_time_counter:
        values = _counter_rates(results)
        if values is None:
            return None
        if values.size == 0:
            nan = normalization(math.nan)
            return {
                "mean": nan,
                "std": nan,
                "max": nan,
                "q25": nan,
                "median": nan,
                "q75": nan,
                "q05": nan,
            }
    else:
        values = np.concatenate([_filtered_points(s)[1] for s in results])
        if values.size == 0:
            return None
    std = float(np.std(values, ddof=1)) if values.size >= 2 else math.nan
    q05, q25, median, q75 = map(float, np.quantile(values, (0.05, 0.25, 0.5, 0.75)))
    return {
        "mean": normalization(float(np.mean(values))),
        "std": normalization(std),
        "max": normalization(float(np.max(values))),
        "q25": normalization(q25),
        "median": normalization(median),
        "q75": normalization(q75),
        "q05": normalization(q05),
    }


def _percent(x: float) -> float:
    return float(x / 100)


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
    # We get all required job time series with just 1 call to
    # get_job_time_series(), then split them by metric.
    metric_to_data: dict[str, list[dict]] = {
        metric: [] for metric in JOB_STATISTICS_METRIC_NAMES
    }
    for result in prom_stats:
        metric_to_data[result["metric"]["__name__"]].append(result)

    gpu_utilization = compute_metric_statistics(
        metric_to_data["slurm_job_utilization_gpu"], normalization=_percent
    )

    gpu_utilization_fp16 = compute_metric_statistics(
        metric_to_data["slurm_job_fp16_gpu"], normalization=_percent
    )

    gpu_utilization_fp32 = compute_metric_statistics(
        metric_to_data["slurm_job_fp32_gpu"], normalization=_percent
    )

    gpu_utilization_fp64 = compute_metric_statistics(
        metric_to_data["slurm_job_fp64_gpu"], normalization=_percent
    )

    gpu_sm_occupancy = compute_metric_statistics(
        metric_to_data["slurm_job_sm_occupancy_gpu"], normalization=_percent
    )

    gpu_memory = compute_metric_statistics(
        metric_to_data["slurm_job_utilization_gpu_memory"], normalization=_percent
    )

    gpu_power = compute_metric_statistics(metric_to_data["slurm_job_power_gpu"])

    cpu_utilization = compute_metric_statistics(
        metric_to_data["slurm_job_core_usage"], is_time_counter=True
    )

    system_memory = None
    if job.allocated_mem:
        # NB: slurm_job_memory_usage is expressed in bytes
        # job.allocated_mem is in megabytes (multiple of 2**20 bytes)
        system_memory = compute_metric_statistics(
            metric_to_data["slurm_job_memory_usage"],
            normalization=lambda x: float(x / (2**20) / cast(int, job.allocated_mem)),
        )
    elif metric_to_data["slurm_job_memory_usage"]:
        # A zero allocation cannot normalize anything: skip system_memory
        # instead of dividing by zero.
        logger.warning(
            f"job.allocated.mem is None or 0 for job {job.job_id} (job status: {job.job_state.value})"
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
