import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import sqlalchemy
from sqlmodel import case, col, func, select

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB

logger = logging.getLogger(__name__)


def check_gpu_type_usage_per_node(
    gpu_type: str,
    time_interval: timedelta | None = timedelta(hours=24),
    minimum_runtime: timedelta | None = timedelta(minutes=5),
    threshold: float = 1.0,
    min_tasks: int = 0,
    ignore_min_tasks_for_clusters: Iterable[str] | None = ("mila",),
) -> bool:
    """
    Check if a GPU type is sufficiently used on each node.
    Log an alert for each node where ratio of jobs using GPU type is lesser than given threshold.

    Parameters
    ----------
    gpu_type: str
        GPU type to check.
    time_interval: timedelta
        If given, only jobs which ran in [now - time_interval, now] will be used for checking.
        Default is last 24 hours.
        If None, all jobs are used.
    minimum_runtime: timedelta
        If given, only jobs which ran at least for this minimum runtime will be used for checking.
        Default is 5 minutes.
        If None, set to 0.
    threshold: float
        A value between 0 and 1 to represent the minimum expected ratio of jobs that use given GPU type
        wr/t running jobs on each node. Log an alert if computed ratio is lesser than this threshold.
    min_tasks: int
        Minimum number of jobs required on a cluster node to make checking.
        Checking is performed on a node only if, either it contains at least `min_tasks` jobs,
        or node cluster is in `ignore_min_tasks_for_clusters`.
    ignore_min_tasks_for_clusters: Sequence
        Clusters to check even if nodes from those clusters don't have `min_tasks` jobs.

    Returns
    -------
    bool
        True if check succeeds, False otherwise.
    """
    from sarc.config import config

    if not gpu_type:
        logger.error("No GPU type specified.")
        return False

    # Parse minimum_runtime
    if minimum_runtime is None:
        minimum_runtime = timedelta(seconds=0)

    # Effective start_time: fall back to submit_time when the job never started.
    eff_start = func.coalesce(SlurmJobDB.start_time, SlurmJobDB.submit_time)
    # Effective end_time: end_time if recorded; else `start_time + elapsed_time` for jobs that started
    # (still running or transitioning); else submit_time, so jobs that never ran
    # (e.g. PENDING) collapse to a point at submission.
    eff_end = case(
        (col(SlurmJobDB.end_time).is_not(None), SlurmJobDB.end_time),
        (
            col(SlurmJobDB.start_time).is_not(None),
            SlurmJobDB.start_time
            + SlurmJobDB.elapsed_time
            * sqlalchemy.literal(timedelta(seconds=1), type_=sqlalchemy.Interval),
        ),
        else_=SlurmJobDB.submit_time,
    )

    ok = True
    with config().db.session() as sess:
        if not sess.exec(select(func.count(SlurmJobDB.id))).one():
            logger.warning("No jobs in database.")
            return False

        # Determine [start, end] bounds for frame iteration.
        # We compute clip elapsed time if start and end are available,
        # so that minimum_runtime is compared to job running time in given interval.
        if time_interval is None:
            start, end = sess.exec(
                select(func.min(eff_start), func.max(eff_end))
            ).one_or_none()
            clipped_elapsed_time = SlurmJobDB.elapsed_time * sqlalchemy.literal(
                timedelta(seconds=1), type_=sqlalchemy.Interval
            )
        else:
            end = datetime.now(tz=UTC)
            start = end - time_interval
            clipped_elapsed_time = func.least(eff_end, end) - func.least(
                eff_start, start
            )

        ignore_min_tasks_for_clusters = set(ignore_min_tasks_for_clusters or ())
        # Select only jobs in (start, end) where elapsed time >= minimum runtime and gres_gpu > 0.
        # `nodes` is a list of nodes. We explode this column to count each job for each of its node.
        for cluster_name, node, nb_gpu_tasks, nb_tasks in sess.exec(
            select(
                SlurmClusterDB.name,
                func.jsonb_array_elements_text(SlurmJobDB.nodes).label("node"),
                func.sum(
                    case((SlurmJobDB.allocated_gpu_type == gpu_type, 1), else_=0)
                ).label("nb_gpu_tasks"),
                func.count(SlurmJobDB.id).label("nb_tasks"),
            )
            .select_from(SlurmJobDB)
            .join(SlurmClusterDB, SlurmJobDB.cluster_id == SlurmClusterDB.id)
            .where(
                eff_start < end,
                eff_end > start,
                clipped_elapsed_time
                >= sqlalchemy.literal(minimum_runtime, type_=sqlalchemy.Interval),
                SlurmJobDB.allocated_gres_gpu > 0,
            )
            .group_by(SlurmClusterDB.name, sqlalchemy.text("node"))
        ):
            gpu_usage = nb_gpu_tasks / nb_tasks
            if gpu_usage < threshold and (
                cluster_name in ignore_min_tasks_for_clusters or nb_tasks >= min_tasks
            ):
                # We alert if gpu usage < threshold and if
                # either we are on a cluster listed in `ignore_min_tasks_for_clusters`,
                # or there are enough jobs in node.
                logger.error(
                    f"[{cluster_name}][{node}] insufficient usage for GPU {gpu_type}: "
                    f"{round(gpu_usage * 100, 2)} % ({nb_gpu_tasks}/{nb_tasks}), "
                    f"minimum required: {round(threshold * 100, 2)} %"
                )
                ok = False

    return ok


@dataclass
class NodeGpuUsageCheck(HealthCheck):
    """Health check for GPU usage per node"""

    gpu_type: str = ""  # ** required **
    time_interval: timedelta | None = timedelta(hours=24)
    minimum_runtime: timedelta | None = timedelta(minutes=5)
    threshold: float = 1.0
    min_tasks: int = 0
    ignore_min_tasks_for_clusters: list[str] | None = field(
        default_factory=lambda: ["mila"]
    )

    def check(self) -> CheckResult:
        if check_gpu_type_usage_per_node(
            gpu_type=self.gpu_type,
            time_interval=self.time_interval,
            minimum_runtime=self.minimum_runtime,
            threshold=self.threshold,
            min_tasks=self.min_tasks,
            ignore_min_tasks_for_clusters=self.ignore_min_tasks_for_clusters,
        ):
            return self.ok()
        else:
            return self.fail()
