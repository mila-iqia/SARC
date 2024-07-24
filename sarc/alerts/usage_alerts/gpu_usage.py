import logging
from datetime import datetime, timedelta
from typing import Optional

from sarc.config import MTL
from sarc.jobs.series import load_job_series

logger = logging.getLogger(__name__)


def check_gpu_type_usage_per_node(
    gpu_type: str,
    time_interval: Optional[timedelta] = timedelta(hours=24),
    minimum_runtime: Optional[timedelta] = timedelta(minutes=5),
    threshold=1.0,
    min_drac_tasks=0,
):
    """
    Check if a GPU type is sufficiently used on each node.
    Log a warning for each node where ratio of jobs using GPU type is lesser than given threshold.

    Parameters
    ----------
    gpu_type: str
        GPU type to check.
    time_interval: timedelta
        If given, only jobs which ran in [now - time_interval, time_interval] will be used for checking.
        Default is last 24 hours.
        If None, all jobs are used.
    minimum_runtime: timedelta
        If given, only jobs which ran at least for this minimum runtime will be used for checking.
        Default is 5 minutes.
        If None, set to 0.
    threshold: float
        A value between 0 and 1 to represent the minimum expected ratio of jobs that use given GPU type
        wr/t running jobs on each node. Log a warning if computed ratio is lesser than this threshold.
    min_drac_tasks: int
        Minimum number of jobs required on a node from a DRAC cluster to make checking.
        Checking won't be performed on DRAC nodes where available jobs are lesser than this number.
    """
    # Parse time_interval
    start, end, clip_time = None, None, False
    if time_interval is not None:
        end = datetime.now(tz=MTL)
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
    for row in ff.itertuples():
        cluster_name, node = row.Index
        nb_gpu_tasks = row.gpu_task_
        nb_tasks = row.task_
        gpu_usage = row.gpu_usage_
        if gpu_usage < threshold and (
            cluster_name == "mila" or nb_tasks >= min_drac_tasks
        ):
            # We warn if gpu usage < threshold and if
            # either we are on MILA cluster,
            # or we are on a DRAC cluster with enough jobs.
            logger.warning(
                f"[{cluster_name}][{node}] insufficient usage for GPU {gpu_type}: "
                f"{round(gpu_usage * 100, 2)} % ({nb_gpu_tasks}/{nb_tasks}), "
                f"minimum required: {round(threshold * 100, 2)} %"
            )
