import logging
import sys
from datetime import datetime, timedelta

import pandas

from sarc.client.series import compute_time_frames, load_job_series
from sarc.config import MTL

logger = logging.getLogger(__name__)


def check_nb_jobs_per_cluster_per_time(
    time_interval: timedelta | None = timedelta(days=7),
    time_unit: timedelta = timedelta(days=1),
    cluster_names: list[str] | None = None,
    nb_stddev: int = 2,
    verbose: bool = False,
) -> None:
    """
    Check if we have scraped enough jobs per time unit per cluster on given time interval.
    Log a warning for each cluster where number of jobs per time unit is lower than a limit
    computed using mean and standard deviation statistics from this cluster.

    Parameters
    ----------
    time_interval: timedelta
        If given, only jobs which ran in [now - time_interval, now] will be used for checking.
        Default is last 7 days.
        If None, all jobs are used.
    time_unit: timedelta
        Time unit in which we must check cluster usage through time_interval. Default is 1 day.
    cluster_names: list
        Optional list of clusters to check.
        If empty (or not specified), use all clusters available among jobs retrieved with time_interval.
    nb_stddev: int
        Amount of standard deviation to remove from average statistics to compute checking threshold.
        For each cluster, threshold is computed as:
        max(0, average - nb_stddev * stddev)
    verbose: bool
        If True, print supplementary info about clusters statistics.
    """

    # Parse time_interval
    start, end, clip_time = None, None, False
    if time_interval is not None:
        end = datetime.now(tz=MTL)
        start = end - time_interval
        clip_time = True

    # Get data frame
    df = load_job_series(start=start, end=end, clip_time=clip_time)

    # Split data frame into time frames using `time_unit`
    tf = compute_time_frames(df, frame_size=time_unit)

    # List all available timestamps.
    # We will check each timestamp for each cluster.
    timestamps = sorted(tf["timestamp"].unique())

    # List clusters
    if cluster_names:
        cluster_names = sorted(cluster_names)
    else:
        cluster_names = sorted(df["cluster_name"].unique())

    # Iter for each cluster.
    for cluster_name in cluster_names:
        # Select only jobs for current cluster,
        # group jobs by timestamp, and count jobs for each timestamp.
        f_stats = (
            tf[tf["cluster_name"] == cluster_name]
            .groupby(["timestamp"])[["job_id"]]
            .count()
        )

        # Create a dataframe with all available timestamps
        # and associate each timestamp to 0 jobs by default.
        c = (
            pandas.DataFrame({"timestamp": timestamps, "count": [0] * len(timestamps)})
            .groupby(["timestamp"])[["count"]]
            .sum()
        )
        # Set each timestamp valid for this cluster with real number of jobs scraped in this timestamp.
        c.loc[f_stats.index, "count"] = f_stats["job_id"]

        # We now have number of jobs for each timestamp for this cluster,
        # with count 0 for timestamps where no jobs run on cluster,

        # Compute average number of jobs per timestamp for this cluster
        avg = c["count"].mean()
        # Compute standard deviation of job count per timestamp for this cluster
        stddev = c["count"].std()
        # Compute threshold to use for warnings: <average> - nb_stddev * <standard deviation>
        threshold = max(0, avg - nb_stddev * stddev)

        if verbose:
            print(f"[{cluster_name}]", file=sys.stderr)  # noqa: T201
            print(c, file=sys.stderr)  # noqa: T201
            print(f"avg {avg}, stddev {stddev}, threshold {threshold}", file=sys.stderr)  # noqa: T201
            print(file=sys.stderr)  # noqa: T201

        if threshold == 0:
            # If threshold is zero, no check can be done, as jobs count will be always >= 0.
            # Instead, we log a general warning.
            msg = f"[{cluster_name}] threshold 0 ({avg} - {nb_stddev} * {stddev})."
            if len(timestamps) == 1:
                msg += (
                    f" Only 1 timestamp found. Either time_interval ({time_interval}) is too short, "
                    f"or this cluster should not be currently checked"
                )
            else:
                msg += (
                    f" Either nb_stddev is too high, time_interval ({time_interval}) is too short, "
                    f"or this cluster should not be currently checked"
                )
            logger.warning(msg)
        else:
            # With a non-null threshold, we can check each timestamp.
            for timestamp in timestamps:
                nb_jobs = c.loc[timestamp]["count"]
                if nb_jobs < threshold:
                    logger.warning(
                        f"[{cluster_name}][{timestamp}] "
                        f"insufficient cluster scraping: {nb_jobs} jobs / cluster / time unit; "
                        f"minimum required for this cluster: {threshold} ({avg} - {nb_stddev} * {stddev}); "
                        f"time unit: {time_unit}"
                    )
