import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Union

from sarc.config import MTL
from sarc.jobs.series import compute_time_frames, load_job_series

logger = logging.getLogger(__name__)


class PrometheusStatInfo:
    """Prometheus stat context, used in checking below."""

    def __init__(self, name):
        self.name = name
        self.col_has = f"has_{name}"
        self.col_ratio = f"ratio_{name}"
        self.avg = None
        self.stddev = None
        self.threshold = None


def check_prometheus_stats_occurrences(
    time_interval: Optional[timedelta] = timedelta(days=7),
    time_unit=timedelta(days=1),
    minimum_runtime: Optional[timedelta] = timedelta(minutes=5),
    cluster_names: Optional[List[str]] = None,
    group_by_node: Optional[Sequence[str]] = ("mila",),
    min_jobs_per_group: Optional[Union[int, Dict[str, int]]] = None,
    nb_stddev=2,
):
    """
    Check if we have scrapped Prometheus stats for enough jobs per node per cluster per time unit.
    Log a warning for each node / cluster where ratio of jobs with Prometheus stats is lower than
    a threshold computed using mean and standard deviation statistics from all clusters.

    Parameters
    ----------
    time_interval: timedelta
        If given, only jobs which ran in [now - time_interval, time_interval] will be used for checking.
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
        If a cluster in this list does not appear in jobs, a warning will be logged.

        If empty (or not specified), use all clusters available among jobs retrieved with time_interval.
    group_by_node: Sequence
        Optional sequence of clusters to group by node.
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
    """

    # Parse time_interval and get data frame
    start, end, clip_time = None, None, False
    if time_interval is not None:
        end = datetime.now(tz=MTL)
        start = end - time_interval
        clip_time = True
    df = load_job_series(start=start, end=end, clip_time=clip_time)

    # Parse minimum_runtime, and select only jobs where
    # elapsed time >= minimum runtime and allocated.gres_gpu == 0
    if minimum_runtime is None:
        minimum_runtime = timedelta(seconds=0)
    df = df[
        (df["elapsed_time"] >= minimum_runtime.total_seconds())
        & (df["allocated.gres_gpu"] == 0)
    ]

    # List clusters
    cluster_names = cluster_names or sorted(df["cluster_name"].unique())

    # Split data frame into time frames using `time_unit`
    df = compute_time_frames(df, frame_size=time_unit)

    # Duplicates lines per node to count each job for each node where it runs
    df = df.explode("nodes")

    # If cluster not in group_by_node,
    # then we must count jobs for the entire cluster, not per node.
    # To simplify the code, let's just define 1 common node for all cluster jobs
    cluster_node_name = "(all)"
    df.loc[~df["cluster_name"].isin(group_by_node), "nodes"] = cluster_node_name

    # Add a column to ease job count
    df.loc[:, "task_"] = 1

    # Generate Prometheus context for each Prometheus stat we want to check.
    prom_contexts = [
        PrometheusStatInfo(name=prom_col)
        for prom_col in ["cpu_utilization", "system_memory"]
    ]

    # Add columns to check if job has prometheus stats
    for prom in prom_contexts:
        df.loc[:, prom.col_has] = ~df[prom.name].isnull()

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
        prom.threshold = max(0, prom.avg - nb_stddev * prom.stddev)

    # Parse min_jobs_per_group
    default_min_jobs = 1
    if min_jobs_per_group is None:
        min_jobs_per_group = {}
    elif isinstance(min_jobs_per_group, int):
        default_min_jobs = min_jobs_per_group
        min_jobs_per_group = {}
    assert isinstance(min_jobs_per_group, dict)

    # Now we can check
    clusters_seen = set()
    for row in f_stats.itertuples():
        timestamp, cluster_name, node = row.Index
        clusters_seen.add(cluster_name)
        nb_jobs = row.task_
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
                    logger.warning(
                        f"[{timestamp}]{grouping_name} insufficient Prometheus data for {prom.name}: "
                        f"{round(local_stat * 100, 2)} % of CPU jobs / {grouping_type} / time unit; "
                        f"minimum required: {prom.threshold} ({prom.avg} - {nb_stddev} * {prom.stddev}); "
                        f"time unit: {time_unit}"
                    )

    # Check clusters listed in `cluster_names` but not found in jobs.
    for cluster_name in cluster_names:
        if cluster_name not in clusters_seen:
            # No stats found for this cluster. Warning
            logger.warning(
                f"[{cluster_name}] no Prometheus data available: no job found"
            )
