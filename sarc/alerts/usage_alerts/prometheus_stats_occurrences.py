import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Sequence, cast

from sarc.alerts.common import HealthCheck, CheckResult

logger = logging.getLogger(__name__)


class PrometheusStatInfo:
    """Prometheus stat context, used in checking below."""

    def __init__(self, name: str):
        self.name = name
        self.col_has = f"has_{name}"
        self.col_ratio = f"ratio_{name}"
        self.avg: float | None = None
        self.stddev: float | None = None
        self.threshold: float | None = None


# pylint: disable=too-many-branches
def check_prometheus_stats_occurrences(
    time_interval: timedelta | None = timedelta(days=7),
    time_unit: timedelta = timedelta(days=1),
    minimum_runtime: timedelta | None = timedelta(minutes=5),
    cluster_names: list[str] | None = None,
    group_by_node: bool | Sequence[str] = ("mila",),
    min_jobs_per_group: int | dict[str, int] | None = None,
    nb_stddev: float = 2.0,
    with_gres_gpu: bool = False,
    prometheus_stats: Iterable[str] = ("cpu_utilization", "system_memory"),
) -> bool:
    """
    Check if we have scrapped Prometheus stats for enough jobs per node per cluster per time unit.
    Log an alert for each node / cluster where ratio of jobs with Prometheus stats is lower than
    a threshold computed using mean and standard deviation statistics from all clusters.

    Parameters
    ----------
    time_interval: timedelta
        If given, only jobs which ran in [now - time_interval, now] will be used for checking.
        Default is last 7 days.
        If None, all jobs are used.
    time_unit: timedelta
        Time unit in which we must check cluster usage through time_interval. Default is 1 day.
    minimum_runtime: timedelta
        If given, only jobs which ran at least for this minimum runtime will be used for checking.
        Default is 5 minutes.
        If None, set to 0.
    cluster_names: list
        Optional list of clusters to check.

        There may have clusters we don't want to check among retrieved jobs (eg. clusters in maintenance).
        On the opposite, we may expect to see jobs in a cluster, but there are actually no jobs in this cluster.
        To cover such cases, one can specify the complete list of expected clusters with `cluster_names`.
        Jobs from clusters not in this list will be ignored both to compute statistics and in checking phase.
        If a cluster in this list does not appear in jobs, an alert will be logged.

        If empty (or not specified), use all clusters available among jobs retrieved with time_interval.
    group_by_node: Sequence | bool
        Either a sequence of clusters to group by node,
        or False to indicate no cluster to group by node (equivalent to empty sequence),
        or True to indicate that all clusters must be grouped by node.
        For clusters in this list, we will check each node separately (ie. a "group" is a cluster node).
        By default, we check the entire cluster (i.e. the "group" is the cluster itself).
    min_jobs_per_group: int | dict
        Minimum number of jobs required for checking in each group.
        Either an integer, as minimum number for any group,
        or a dictionary mapping a cluster name to minimum number in each group of this cluster
        A group is either a cluster node, if cluster name is in `group_by_node`,
        or the entire cluster otherwise.
        Default is 1 job per group.
    nb_stddev: float
        Amount of standard deviation to remove from average statistics to compute checking threshold.
        Threshold is computed as:
        max(0, average - nb_stddev * stddev)
    with_gres_gpu: bool
        If True, check only jobs which have allocated.gres_gpu > 0  (GPU jobs)
        If False (default), check only jobs which have allocated.gres_gpu == 0 (CPU jobs).
    prometheus_stats: Sequence[str]
        Prometheus stats to check. Default: "cpu_utilization", "system_memory"

    Returns
    -------
    bool
        True if check succeeds, False otherwise.
    """
    from sarc.config import UTC
    from sarc.client.series import compute_time_frames, load_job_series

    # Parse time_interval and get data frame
    start: datetime | None = None
    end: datetime | None = None
    clip_time = False
    if time_interval is not None:
        end = datetime.now(tz=UTC)
        start = end - time_interval
        clip_time = True
    df = load_job_series(start=start, end=end, clip_time=clip_time)

    # Parse minimum_runtime
    if minimum_runtime is None:
        minimum_runtime = timedelta(seconds=0)
    # Select only jobs where elapsed time >= minimum runtime and
    # jobs are GPU or CPU jobs, depending on `with_gres_gpu`
    selection_elapsed_time = df["elapsed_time"] >= minimum_runtime.total_seconds()
    selection_gres_gpu = (
        (df["allocated.gres_gpu"] > 0)
        if with_gres_gpu
        else (df["allocated.gres_gpu"] == 0)
    )
    df = df[selection_elapsed_time & selection_gres_gpu]

    # List clusters
    cluster_names = cluster_names or sorted(df["cluster_name"].unique())

    # If df is empty, alert for each cluster that we can't check Prometheus stats.
    if df.empty:
        for cluster_name in cluster_names:
            logger.error(f"[{cluster_name}] no Prometheus data available: no job found")
        # As there's nothing to check, we return immediately.
        return False

    # Split data frame into time frames using `time_unit`
    df = compute_time_frames(df, frame_size=time_unit)

    # Duplicates lines per node to count each job for each node where it runs
    df = df.explode("nodes")

    # parse group_by_node
    if isinstance(group_by_node, bool):
        group_by_node = list(df["cluster_name"].unique()) if group_by_node else ()

    # If cluster not in group_by_node,
    # then we must count jobs for the entire cluster, not per node.
    # To simplify the code, let's just define 1 common node for all cluster jobs
    cluster_node_name = "(all)"
    df.loc[~df["cluster_name"].isin(group_by_node), "nodes"] = cluster_node_name

    # Add a column to ease job count
    df.loc[:, "task_"] = 1

    # Generate Prometheus context for each Prometheus stat we want to check.
    prom_contexts = [PrometheusStatInfo(name=prom_col) for prom_col in prometheus_stats]

    # Add columns to check if job has prometheus stats
    for prom in prom_contexts:
        # NB: Use DataFrame.reindex() to add column with NaN values if missing:
        # (2024/09/26) https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.reindex.html
        df.loc[:, prom.col_has] = ~(df.reindex(columns=[prom.name])[prom.name].isnull())

    # Group per timestamp per cluster per node, and count jobs and prometheus stats.
    # If "cluster_names" are given, use only jobs in these clusters.
    f_stats = (
        df[df["cluster_name"].isin(cluster_names)]
        .groupby(["timestamp", "cluster_name", "nodes"])[
            [prom_info.col_has for prom_info in prom_contexts] + ["task_"]
        ]
        .sum()
    )

    # Compute ratio of job with Prometheus stat for each group,
    # then compute threshold for each Prometheus stat.
    for prom in prom_contexts:
        f_stats[prom.col_ratio] = f_stats[prom.col_has] / f_stats["task_"]
        prom.avg = f_stats[prom.col_ratio].mean()
        prom.stddev = f_stats[prom.col_ratio].std()
        prom.threshold = max(0.0, prom.avg - nb_stddev * prom.stddev)

    # Parse min_jobs_per_group
    default_min_jobs = 1
    if min_jobs_per_group is None:
        min_jobs_per_group = {}
    elif isinstance(min_jobs_per_group, int):
        default_min_jobs = min_jobs_per_group
        min_jobs_per_group = {}
    assert isinstance(min_jobs_per_group, dict)

    ok = True
    job_type = "GPU" if with_gres_gpu else "CPU"

    # Now we can check
    clusters_seen: set[str] = set()
    for row in f_stats.itertuples():
        timestamp, cluster_name, node = row.Index  # type: ignore[misc, assignment]
        clusters_seen.add(cluster_name)
        nb_jobs = cast(int, row.task_)
        if nb_jobs >= min_jobs_per_group.get(cluster_name, default_min_jobs):
            grouping_type = "cluster" if node == cluster_node_name else "node / cluster"
            grouping_name = (
                f"[{cluster_name}]"
                if node == cluster_node_name
                else f"[{cluster_name}][{node}]"
            )
            for prom in prom_contexts:
                local_stat = getattr(row, prom.col_has) / nb_jobs
                if local_stat < prom.threshold:
                    logger.error(
                        f"[{timestamp}]{grouping_name} insufficient Prometheus data for {prom.name}: "
                        f"{round(local_stat * 100, 2)} % of {job_type} jobs / {grouping_type} / time unit; "
                        f"minimum required: {prom.threshold} ({prom.avg} - {nb_stddev} * {prom.stddev}); "
                        f"time unit: {time_unit}"
                    )
                    ok = False

    # Check clusters listed in `cluster_names` but not found in jobs.
    for cluster_name in cluster_names:
        if cluster_name not in clusters_seen:
            # No stats found for this cluster. Warning
            logger.error(f"[{cluster_name}] no Prometheus data available: no job found")
            ok = False

    return ok


@dataclass
class PrometheusCpuStatCheck(HealthCheck):
    """Health check for Prometheus CPU stats."""

    time_interval: timedelta | None = timedelta(days=7)
    time_unit: timedelta = timedelta(days=1)
    minimum_runtime: timedelta | None = timedelta(minutes=5)
    cluster_names: list[str] | None = None
    group_by_node: list[str] | bool = field(default_factory=lambda: ["mila"])
    min_jobs_per_group: dict[str, int] | int | None = None
    nb_stddev: float = 2.0
    with_gres_gpu: bool = False
    prometheus_stats: list[str] = field(
        default_factory=lambda: ["cpu_utilization", "system_memory"]
    )

    def check(self) -> CheckResult:
        if check_prometheus_stats_occurrences(
            time_interval=self.time_interval,
            time_unit=self.time_unit,
            minimum_runtime=self.minimum_runtime,
            cluster_names=self.cluster_names,
            group_by_node=self.group_by_node,
            min_jobs_per_group=self.min_jobs_per_group,
            nb_stddev=self.nb_stddev,
            with_gres_gpu=self.with_gres_gpu,
            prometheus_stats=self.prometheus_stats,
        ):
            return self.ok()
        else:
            return self.fail()


def check_prometheus_stats_for_gpu_jobs(
    time_interval: timedelta | None = timedelta(days=7),
    time_unit: timedelta = timedelta(days=1),
    minimum_runtime: timedelta | None = timedelta(minutes=5),
    cluster_names: list[str] | None = None,
    # For GPU jobs, default behavior is to group each cluster by nodes for checking.
    group_by_node: bool | Sequence[str] = True,
    min_jobs_per_group: int | dict[str, int] | None = None,
    nb_stddev: float = 2.0,
) -> bool:
    """
    Check if we have scrapped Prometheus stats for enough GPU jobs per node per cluster per time unit.
    Log an alert for each node / cluster where ratio of GPU jobs with Prometheus stats is lower than
    a threshold computed using mean and standard deviation statistics from all clusters.

    To get more info about parameters, see documentation for `check_prometheus_stats_occurrences`.
    """
    return check_prometheus_stats_occurrences(
        time_interval=time_interval,
        time_unit=time_unit,
        minimum_runtime=minimum_runtime,
        cluster_names=cluster_names,
        group_by_node=group_by_node,
        min_jobs_per_group=min_jobs_per_group,
        nb_stddev=nb_stddev,
        # We are looking for GPU jobs
        with_gres_gpu=True,
        # We are looking for GPU-related Prometheus stats
        prometheus_stats=(
            "gpu_utilization",
            "gpu_utilization_fp16",
            "gpu_utilization_fp32",
            "gpu_utilization_fp64",
            "gpu_sm_occupancy",
            "gpu_memory",
            "gpu_power",
        ),
    )


@dataclass
class PrometheusGpuStatCheck(HealthCheck):
    """Health check for Prometheus GPU stats."""

    time_interval: timedelta | None = timedelta(days=7)
    time_unit: timedelta = timedelta(days=1)
    minimum_runtime: timedelta | None = timedelta(minutes=5)
    cluster_names: list[str] | None = None
    group_by_node: list[str] | bool = True
    min_jobs_per_group: dict[str, int] | int | None = None
    nb_stddev: float = 2.0

    def check(self) -> CheckResult:
        if check_prometheus_stats_for_gpu_jobs(
            time_interval=self.time_interval,
            time_unit=self.time_unit,
            minimum_runtime=self.minimum_runtime,
            cluster_names=self.cluster_names,
            group_by_node=self.group_by_node,
            min_jobs_per_group=self.min_jobs_per_group,
            nb_stddev=self.nb_stddev,
        ):
            return self.ok()
        else:
            return self.fail()
