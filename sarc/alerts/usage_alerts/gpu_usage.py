import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import cast

from sarc.alerts.common import HealthCheck, CheckResult
from sarc.client.series import load_job_series
from sarc.config import UTC

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
    if not gpu_type:
        logger.error("No GPU type specified.")
        return False

    # Parse time_interval
    start, end, clip_time = None, None, False
    if time_interval is not None:
        end = datetime.now(tz=UTC)
        start = end - time_interval
        clip_time = True

    # Parse minimum_runtime
    if minimum_runtime is None:
        minimum_runtime = timedelta(seconds=0)

    # Get data frame. We clip time if start and end are available,
    # so that minimum_runtime is compared to job running time in given interval.
    df = load_job_series(start=start, end=end, clip_time=clip_time)

    # Add a column `gpu_task_` with value 1 for each job running on given GPU type.
    df.loc[:, "gpu_task_"] = df["allocated.gpu_type"] == gpu_type
    # Add a column `task_` with value 1 for each job. Used later to count jobs in a groupby().
    df.loc[:, "task_"] = 1

    # Group jobs.
    ff = (
        # Select only jobs where elapsed time >= minimum runtime and gres_gpu > 0
        df[
            (df["elapsed_time"] >= minimum_runtime.total_seconds())
            & (df["allocated.gres_gpu"] > 0)
        ]
        # `nodes` is a list of nodes. We explode this column to count each job for each node where it is running
        .explode("nodes")
        # Then we group by cluster name and nodes,
        .groupby(["cluster_name", "nodes"])[["gpu_task_", "task_"]]
        # and we sum on gpu_task_ and task_
        .sum()
    )
    # Finally, we compute GPU usage.
    ff["gpu_usage_"] = ff["gpu_task_"] / ff["task_"]

    # We can now check GPU usage.
    ok = True
    ignore_min_tasks_for_clusters = set(ignore_min_tasks_for_clusters or ())
    for row in ff.itertuples():
        cluster_name, node = row.Index  # type: ignore[misc]
        nb_gpu_tasks = row.gpu_task_
        nb_tasks = cast(int, row.task_)
        gpu_usage = cast(float, row.gpu_usage_)
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
