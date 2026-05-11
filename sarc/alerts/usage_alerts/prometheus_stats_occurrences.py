import logging
import math
import statistics
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import sqlalchemy
from sqlmodel import case, func, select

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.alerts.usage_alerts.alert_sql_utils import SqlSymbols
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB

logger = logging.getLogger(__name__)


class PrometheusStatInfo:
    """Prometheus stat context, used in checking below."""

    def __init__(self, name: str):
        self.name = name
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
    from sarc.config import config

    if cluster_names is None:
        cluster_names = []

    if minimum_runtime is None:
        minimum_runtime = timedelta(seconds=0)

    with config().db.session() as sess:
        if not sess.exec(select(func.count(SlurmJobDB.id))).one():
            logger.error("No Prometheus data available: no job found")
            return False

        # Determine [start, end] and clipped elapsed time for frame iteration and comparison.
        start, end, clipped_elapsed_time = (
            SqlSymbols.convert_job_time_interval_to_sql_bounds(sess, time_interval)
        )
        eff_start = SqlSymbols.eff_start
        eff_end = SqlSymbols.eff_end

        # Use a Postgresql query: time frange + join
        # NB: Since we iterate over frames, we want to capture point jobs located at frame bounds,
        # e.g. PENDING jobs which run in [submit_time, submit_time]. So, we grab everything in
        # [frame_start included, frame_end excluded), instead of (frame_start excluded, frame_end excluded)
        time_unit_sql = sqlalchemy.literal(time_unit, type_=sqlalchemy.Interval)
        frames = select(
            func.generate_series(start, end, time_unit_sql).label("frame_start")
        ).cte("frames")
        frame_start = frames.c.frame_start
        frame_end = frame_start + time_unit_sql

        exploded_query = (
            select(
                SlurmJobDB.id.label("job_id"),
                SlurmClusterDB.name.label("cluster_name"),
                # Explode jobs per node and join with matching frames.
                func.jsonb_array_elements_text(SlurmJobDB.nodes).label("node"),
                frame_start.label("frame_start"),
            )
            .select_from(SlurmJobDB)
            .join(SlurmClusterDB, SlurmJobDB.cluster_id == SlurmClusterDB.id)
            .join(frames, (eff_start < frame_end) & (eff_end >= frame_start))
            .where(
                # Select only jobs where elapsed time >= minimum runtime,
                # and jobs are GPU or CPU jobs, depending on `with_gres_gpu`
                (
                    (SlurmJobDB.allocated_gres_gpu > 0)
                    if with_gres_gpu
                    else (SlurmJobDB.allocated_gres_gpu == 0)
                ),
                clipped_elapsed_time
                >= sqlalchemy.literal(minimum_runtime, type_=sqlalchemy.Interval),
            )
        )
        if cluster_names:
            exploded_query = exploded_query.where(
                SlurmClusterDB.name.in_(cluster_names)
            )
        # Materializing as a CTE turns `exploded.c.node` into a plain column ref,
        # so the case() below references a column rather than an SRF.
        exploded = exploded_query.cte("exploded")

        # Resolve `group_by_node`: drives whether each cluster's jobs are grouped per
        # node or collapsed under "(all)".
        if isinstance(group_by_node, bool):
            group_all = group_by_node
            clusters_to_group: list[str] = []
        else:
            group_all = False
            clusters_to_group = list(group_by_node)
        # `group_node`: real node for grouped clusters, "(all)" otherwise.
        if group_all:
            group_node_expr = exploded.c.node
        elif clusters_to_group:
            group_node_expr = case(
                (exploded.c.cluster_name.in_(clusters_to_group), exploded.c.node),
                else_=sqlalchemy.literal("(all)"),
            )
        else:
            group_node_expr = sqlalchemy.literal("(all)")

        # Pre-aggregate per-job stat presence as a CTE: one row per job_id with
        # a boolean column per requested stat. Lets the main query do a single
        # LEFT JOIN instead of N correlated EXISTS subqueries (one per stat per
        # exploded row).
        job_stats = (
            select(
                JobStatisticDB.job_id,
                *(
                    # Inside group (by job_id),
                    # bool_or aggregates job stat entries and tell if at least
                    # one row matches given stat name
                    func.bool_or(JobStatisticDB.name == name).label(f"has_{name}")
                    for name in prometheus_stats
                ),
            )
            .where(JobStatisticDB.name.in_(list(prometheus_stats)))
            .group_by(JobStatisticDB.job_id)
            .cte("job_stats")
        )

        agg_query = (
            select(
                exploded.c.frame_start.label("timestamp"),
                exploded.c.cluster_name,
                group_node_expr.label("group_node"),
                func.count(exploded.c.job_id).label("nb_jobs"),
                *[
                    func.sum(case((job_stats.c[f"has_{name}"], 1), else_=0)).label(
                        f"nb_jobs_with_{name}"
                    )
                    for name in prometheus_stats
                ],
            )
            .select_from(exploded)
            .outerjoin(job_stats, exploded.c.job_id == job_stats.c.job_id)
            .group_by(exploded.c.frame_start, exploded.c.cluster_name, group_node_expr)
            .order_by(exploded.c.frame_start, exploded.c.cluster_name, group_node_expr)
        )
        results = sess.exec(agg_query).all()

    if not results:
        if cluster_names:
            for cluster_name in cluster_names:
                logger.error(
                    f"[{cluster_name}] no Prometheus data available: no job found"
                )
        else:
            logger.error("No Prometheus data available: no job found")
        # As there's nothing to check, we return immediately.
        return False

    prom_contexts = [PrometheusStatInfo(name=name) for name in prometheus_stats]
    rows: list[tuple[datetime, str, str, int, dict[str, float]]] = []
    clusters_seen: set[str] = set()
    for row in results:
        timestamp, cluster_name, group_node, nb_jobs = row[:4]
        clusters_seen.add(cluster_name)
        ratios = {
            prom.name: row[4 + i] / nb_jobs for i, prom in enumerate(prom_contexts)
        }
        rows.append((timestamp, cluster_name, group_node, nb_jobs, ratios))

    # Compute mean/stddev/threshold for each stat across all groups. Mirror the
    # Pandas behavior: `Series.std()` returns NaN on a single sample, which makes
    # `max(0.0, NaN)` collapse to 0.0 — no row triggers an alert for that stat.
    for prom in prom_contexts:
        stat_ratios = [r[4][prom.name] for r in rows]
        prom.avg = statistics.mean(stat_ratios)
        prom.stddev = (
            statistics.stdev(stat_ratios) if len(stat_ratios) > 1 else math.nan
        )
        prom.threshold = max(0.0, prom.avg - nb_stddev * prom.stddev)

    # Parse min_jobs_per_group
    default_min_jobs = 1
    if min_jobs_per_group is None:
        min_jobs_per_group = {}
    elif isinstance(min_jobs_per_group, int):
        default_min_jobs = min_jobs_per_group
        min_jobs_per_group = {}
    assert isinstance(min_jobs_per_group, dict)

    cluster_node_name = "(all)"
    job_type = "GPU" if with_gres_gpu else "CPU"
    ok = True
    for timestamp, cluster_name, node, nb_jobs, ratios in rows:
        if nb_jobs >= min_jobs_per_group.get(cluster_name, default_min_jobs):
            grouping_type = "cluster" if node == cluster_node_name else "node / cluster"
            grouping_name = (
                f"[{cluster_name}]"
                if node == cluster_node_name
                else f"[{cluster_name}][{node}]"
            )
            for prom in prom_contexts:
                local_stat = ratios[prom.name]
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
