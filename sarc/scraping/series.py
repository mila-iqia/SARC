import logging
from collections import defaultdict
from datetime import UTC, datetime
from typing import Callable, Sequence, TypedDict, cast

import numpy as np

from sarc.config import config
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.scraping.dcgm import DCGM_FP64_BLANK, dcgm_prof_blackout
from sarc.traces import trace_decorator

logger = logging.getLogger(__name__)


@trace_decorator()
def get_job_time_series_batched(
    jobs: Sequence[SlurmJobDB],
    metric: str | Sequence[str],
    max_points: int = 100,
    min_interval: int = 30,
    bucket_ratio: float = 4.0,
) -> list[list]:
    """Fetch job metrics for a sequence of jobs in batched Prometheus queries.

    One batched query imposes one subquery step on all its jobs, and the
    step that caps a long job at ``max_points`` points would starve the
    batch's short jobs. Jobs are instead grouped by *runtime*: each group
    is fetched with a subquery step of its shortest runtime divided by
    ``max_points``, bounding every job of the group near ``max_points``
    points per series (within ``bucket_ratio`` of it). A group's time
    window only has to cover its jobs, so jobs that ran far apart still
    batch together. Jobs whose runtime cannot exceed ``max_points``
    points at the raw ``min_interval`` sampling are not downsampled and
    share one raw query.

    Arguments:
        jobs: The sequence of jobs for which to fetch metrics.
        metric: The metric or list of metrics, which must be in
            ``SLURM_JOB_METRIC_NAMES``.
        max_points: Target number of points per series for the longest
            job of a group.
        min_interval: The minimal reporting interval, in seconds: a step
            of no more than the raw sampling interval would only repeat
            samples and skew the statistics.
        bucket_ratio: Maximal max/min runtime ratio within a downsampled
            group.

    Returns:
        A list of result lists corresponding positionally to each job in `jobs`.
    """
    if not jobs:
        return []

    metrics = [metric] if isinstance(metric, str) else metric
    if not metrics:
        raise ValueError("No metrics given")
    for m in metrics:
        if m not in SLURM_JOB_METRIC_NAMES:
            raise ValueError(f"Unknown metric name: {m}")
    if max_points < 1 or min_interval < 1:
        raise ValueError("max_points and min_interval must be positive")

    now = datetime.now(tz=UTC)
    results: list[list] = [[] for _ in jobs]

    # Runtime and time bounds of every fetchable job, keyed by position in `jobs`.
    durations: dict[int, int] = {}
    spans: dict[int, tuple[datetime, datetime]] = {}
    for idx, job in enumerate(jobs):
        if job.job_state != "RUNNING" and not job.elapsed_time:
            continue
        if job.start_time is None or job.end_time is None:
            continue

        durations[idx] = max(1, int((job.end_time - job.start_time).total_seconds()))
        spans[idx] = (job.start_time, job.end_time)

    if not durations:
        return results

    # A job id resuming after preemption appears several times with possibly
    # different runtimes, and can thus land in several groups: a series for
    # that id is only attributable to one job run without time filtering.
    run_counts: dict[str, int] = defaultdict(int)
    for idx in durations:
        run_counts[str(jobs[idx].job_id)] += 1

    raw_cut = max_points * min_interval
    groups: list[tuple[list[int], int | None]] = [
        ([idx for idx in durations if durations[idx] <= raw_cut], None)
    ]
    # Group the longer jobs by ascending runtime so the step imposed by the
    # shortest job of a group leaves at most bucket_ratio * max_points
    # points to its longest jobs.
    long_idxs = sorted(
        (idx for idx in durations if durations[idx] > raw_cut),
        key=durations.__getitem__,
    )
    start = 0
    while start < len(long_idxs):
        min_duration = durations[long_idxs[start]]
        end = start + 1
        while (
            end < len(long_idxs)
            and durations[long_idxs[end]] <= min_duration * bucket_ratio
        ):
            end += 1
        # Ceil division: min_duration > raw_cut, so the step stays above
        # the raw sampling interval and samples are never repeated.
        groups.append((long_idxs[start:end], -(-min_duration // max_points)))
        start = end

    if len(metrics) == 1:
        prefix = metrics[0]
        metric_label_exprs: list[str] = []
    else:
        prefix = ""
        metric_label_exprs = [f'__name__=~"^({"|".join(metrics)})$"']

    cluster_name = jobs[0].cluster.name

    for idxs, step in groups:
        if not idxs:
            continue

        job_map: dict[str, list[int]] = defaultdict(list)
        min_start: datetime | None = None
        max_end: datetime | None = None
        for idx in idxs:
            job = jobs[idx]
            job_map[str(job.job_id)].append(idx)
            start_time, end_time = spans[idx]
            min_start = start_time if min_start is None else min(min_start, start_time)
            max_end = end_time if max_end is None else max(max_end, end_time)

        if min_start is None or max_end is None:  # keep ty happy: idxs is not empty
            continue

        # The group's bounding time window
        group_duration = max(1, int((max_end - min_start).total_seconds()))
        group_offset = max(0, int((now - max_end).total_seconds()))
        offset_string = f" offset {group_offset}s" if group_offset > 0 else ""
        step_string = f":{step}s" if step is not None else ""

        str_job_ids = "|".join(job_map.keys())
        label_exprs = metric_label_exprs + [f'slurmjobid=~"^{str_job_ids}$"']
        selector = prefix + "{" + ", ".join(label_exprs) + "}"
        combined_query = f"({selector}[{group_duration}s{step_string}]{offset_string})"

        logger.debug(f"batched prometheus query: {combined_query}")

        raw_results = config.clusters[cluster_name].prometheus.custom_query(
            combined_query
        )

        # Map returned series back to jobs & filter values to exact job time windows
        for series_data in raw_results:
            job_id = series_data.get("metric", {}).get("slurmjobid")
            if not job_id or job_id not in job_map:
                continue

            if len(job_map[job_id]) == 1 and run_counts[job_id] == 1:
                # The only run of this id: every point is its job's.
                idx = job_map[job_id][0]
                results[idx].append(series_data)
            else:
                series_values = series_data.get("values", [])
                for idx in job_map[job_id]:
                    job = jobs[idx]
                    start_ts = job.start_time.timestamp()  # ty:ignore[unresolved-attribute]
                    end_ts = job.end_time.timestamp()  # ty:ignore[unresolved-attribute]

                    # Filter points belonging strictly to this job run
                    filtered_values = [
                        pt for pt in series_values if start_ts <= pt[0] <= end_ts
                    ]
                    if filtered_values:
                        series_copy = dict(series_data)
                        series_copy["values"] = filtered_values
                        results[idx].append(series_copy)

    return results


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


def _counter_rates(results: Sequence[dict]) -> np.ndarray:
    """Pooled per-second rates of a nanosecond time counter (e.g. core usage).

    Series are grouped by their (instance, core, gpu) labels — series sharing
    the same labels are concatenated in input order — and each group is
    differenced separately. The result is empty when no group has at least
    two attributable real samples.
    """
    used = [k for k in _COUNTER_GROUP_LABELS if any(k in s["metric"] for s in results)]
    groups: dict[tuple, list[tuple[np.ndarray, np.ndarray]]] = {}
    for series in results:
        labels = series["metric"]
        try:
            key = tuple(labels[k] for k in used)
        except KeyError:
            continue  # a group label is missing: these samples are not attributable
        timestamps, values = _filtered_points(series)
        if values.size:
            groups.setdefault(key, []).append((timestamps, values))
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
    usable sample remains. std uses ddof=0 (population std, 0.0 for a single
    sample): jobs are only fetched once finished, so their samples form the
    complete population. Quantiles interpolate linearly.
    """
    if not results:
        return None
    if is_time_counter:
        values = _counter_rates(results)
    else:
        values = np.concatenate([_filtered_points(s)[1] for s in results])
    if values.size == 0:
        return None
    std = float(np.std(values))
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
    "slurm_job_sm_active_gpu",
    "slurm_job_sm_occupancy_gpu",
    "slurm_job_utilization_gpu_memory",
    "slurm_job_power_gpu",
    "slurm_job_core_usage",
    "slurm_job_memory_usage",
)

# Stored statistics of the DCGM PROF family, dropped together when
# compute_job_statistics detects a blackout (dcgm.dcgm_prof_blackout):
# they share one counter access path, so their zeros are not independent.
# `gpu_utilization` is absent: it is now sourced from DEV GPU_UTIL
# (slurm-job-exporter #72), which keeps measuring even through a PROF blackout
# (and the former SM_ACTIVE samples it carried are now under `gpu_sm_active`).
_PROF_BLACKOUT_STATS = (
    "gpu_sm_active",
    "gpu_sm_occupancy",
    "gpu_utilization_fp16",
    "gpu_utilization_fp32",
    "gpu_utilization_fp64",
    "gpu_memory",
)


def gpu_activity_series(
    metric_to_data: dict[str, list[dict]],
) -> tuple[list[dict], list[dict]]:
    """Split the job's GPU series into (SM_ACTIVE, true utilization) series.

    What slurm_job_utilization_gpu measures depends on the slurm-job-exporter
    version and mode, and the exporter exposes neither:
    - < 0.5, DCGM mode: SM_ACTIVE, a DCGM profiling (PROF) field.
    - >= 0.5, DCGM mode: the true utilization (DEV GPU_UTIL); SM_ACTIVE
      moved to slurm_job_sm_active_gpu.
    - NVML mode, any version: the true utilization; no PROF field at all.
    """
    utilization = metric_to_data["slurm_job_utilization_gpu"]
    sm_active = metric_to_data["slurm_job_sm_active_gpu"]
    # DCGM reads SM_OCCUPANCY and SM_ACTIVE together (one PROF counter
    # subgroup in `dcgmi profile -l`): slurm_job_sm_occupancy_gpu is present
    # exactly when SM_ACTIVE was readable.
    sm_active_readable = bool(metric_to_data["slurm_job_sm_occupancy_gpu"])

    if sm_active:
        # Only exporter >= 0.5 publishes this name.
        return sm_active, utilization
    if sm_active_readable:
        # SM_ACTIVE was readable but not published under its new name:
        # exporter < 0.5, whose utilization series is SM_ACTIVE.
        return utilization, []
    # No SM_ACTIVE: NVML mode, or DCGM without readable PROF. In DCGM mode,
    # exporter < 0.5 publishes no utilization without SM_ACTIVE, so any
    # utilization here is the true one.
    return [], utilization


@trace_decorator()
def compute_job_statistics(
    job: SlurmJobDB, prom_stats: list[dict]
) -> dict[str, JobStatisticDB]:
    """Compute the stored statistics of a job from its raw Prometheus series.

    A detected DCGM PROF blackout (sarc.scraping.dcgm.dcgm_prof_blackout)
    drops the whole PROF family from the result, keeping DEV power/utilization
    and the cgroup metrics: their false zeros must not reach the database.
    """
    # We get all required job time series with a few batched calls to
    # get_job_time_series_batched(), then split them by metric.
    metric_to_data: dict[str, list[dict]] = {
        metric: [] for metric in JOB_STATISTICS_METRIC_NAMES
    }
    for result in prom_stats:
        metric_to_data[result["metric"]["__name__"]].append(result)

    sm_active_series, utilization_series = gpu_activity_series(metric_to_data)
    gpu_sm_active = compute_metric_statistics(sm_active_series, normalization=_percent)
    gpu_utilization = compute_metric_statistics(
        utilization_series, normalization=_percent
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
            f"job.allocated_mem is None or 0 for job {job.job_id} (job status: {job.job_state.value})"
        )

    res = dict()
    if gpu_utilization:
        res["gpu_utilization"] = JobStatisticDB(
            name="gpu_utilization", **gpu_utilization
        )
    if gpu_sm_active:
        res["gpu_sm_active"] = JobStatisticDB(name="gpu_sm_active", **gpu_sm_active)
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

    # DCGM PROF blackout guard: exact-zero occupancy *and* DRAM activity on a
    # job that drew real power means the whole PROF family is lying (false
    # zeros from blocked hardware counters) -- store none of it, so the
    # metrics read NULL ("no measurement") instead of asserting a false 0.
    if dcgm_prof_blackout(
        res["gpu_sm_occupancy"].max if "gpu_sm_occupancy" in res else None,
        res["gpu_memory"].max if "gpu_memory" in res else None,
        res["gpu_power"].max if "gpu_power" in res else None,
    ):
        dropped = [name for name in _PROF_BLACKOUT_STATS if res.pop(name, None)]
        logger.warning(
            f"job {job.job_id} (cluster {job.cluster.name}): DCGM PROF blackout "
            f"(occupancy and DRAM activity maxed at 0 while gpu_power reached "
            f"{res['gpu_power'].max:g} mW), dropping stats: {', '.join(dropped)}"
        )
    return res


# The slurm job metric names that get_job_time_series_batched() accepts.
SLURM_JOB_METRIC_NAMES = frozenset(
    [
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
        "slurm_job_sm_active_gpu",
        "slurm_job_sm_occupancy_gpu",
        "slurm_job_states",
        "slurm_job_tensor_gpu",
        "slurm_job_threads_count",
        "slurm_job_utilization_gpu",
        "slurm_job_utilization_gpu_memory",
    ]
)
