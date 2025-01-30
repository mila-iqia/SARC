import logging
from datetime import datetime, timedelta
from typing import Optional

from sarc.client.series import compute_cost_and_waste, load_job_series
from sarc.config import MTL

logger = logging.getLogger(__name__)


def check_gpu_util_per_user(
    threshold: timedelta,
    time_interval: Optional[timedelta] = timedelta(days=7),
    minimum_runtime: Optional[timedelta] = timedelta(minutes=5),
):
    """
    Check if users have enough utilization of GPUs.
    Log a warning for each user if average GPU-util of user jobs
    in time interval is lower than a given threshold.

    For a given user job, GPU-util is computed as
    gpu_utilization * gpu_equivalent_cost
    (with gpu_equivalent_cost as elapsed_time * allocated.gres_gpu).

    Parameters
    ----------
    threshold: timedelta
        Minimum value for average GPU-util expected per user.
        We assume GPU-util is expressed in GPU-seconds,
        thus threshold can be expressed with a timedelta.
    time_interval
        If given, only jobs which ran in [now - time_interval, time_interval] will be used for checking.
        Default is last 7 days.
        If None, all jobs are used.
    minimum_runtime
        If given, only jobs which ran at least for this minimum runtime will be used for checking.
        Default is 5 minutes.
        If None, set to 0.
    """
    # Parse time_interval
    start, end, clip_time = None, None, False
    if time_interval is not None:
        end = datetime.now(tz=MTL)
        start = end - time_interval
        clip_time = True

    # Get data frame. We clip time if start and end are available,
    # so that minimum_runtime is compared to job running time in given interval.
    df = load_job_series(start=start, end=end, clip_time=clip_time)

    # Parse minimum_runtime, and select only jobs where
    # elapsed time >= minimum runtime and allocated.gres_gpu > 0
    if minimum_runtime is None:
        minimum_runtime = timedelta(seconds=0)
    df = df[
        (df["elapsed_time"] >= minimum_runtime.total_seconds())
        & (df["allocated.gres_gpu"] > 0)
    ]

    # Compute cost
    df = compute_cost_and_waste(df)

    # Compute GPU-util for each job
    df["gpu_util"] = df["gpu_utilization"] * df["gpu_equivalent_cost"]

    # Compute average GPU-util per user
    f_stats = df.groupby(["user"])[["gpu_util"]].mean()

    # Now we can check
    for row in f_stats.itertuples():
        user = row.Index
        gpu_util = row.gpu_util
        if gpu_util < threshold.total_seconds():
            logger.warning(
                f"[{user}] insufficient average gpu_util: {gpu_util} GPU-seconds; "
                f"minimum required: {threshold} ({threshold.total_seconds()} GPU-seconds)"
            )
