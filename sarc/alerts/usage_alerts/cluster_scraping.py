import logging
from datetime import datetime, timedelta
from typing import List, Optional

from sarc.config import MTL
from sarc.jobs.series import compute_time_frames, load_job_series

logger = logging.getLogger(__name__)


def check_nb_jobs_per_cluster_per_time(
    time_interval: Optional[timedelta] = timedelta(days=7),
    time_unit=timedelta(days=1),
    cluster_names: Optional[List[str]] = None,
):
    """
    Check if we have scraped enough jobs per cluster per time unit on given time interval.
    Log a warning for each cluster where number of jobs is lower than a required limit
    computed using mean and standard deviation statistics from clusters usage.

    Parameters
    ----------
    time_interval: timedelta
        If given, only jobs which ran in [now - time_interval, time_interval] will be used for checking.
        Default is last 7 days.
        If None, all jobs are used.
    time_unit: timedelta
        Time unit in which we must check cluster usage through time_interval. Default is 1 day.
    cluster_names: list
        Optional list of clusters to check.
        If empty (or not specified), use all clusters available among jobs retrieved with time_interval.
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

    # List clusters
    if not cluster_names:
        cluster_names = sorted(df["cluster_name"].unique())

    # Generate a dataframe for stats.
    f_stats = (
        # Use only lines associated with given clusters
        tf[tf["cluster_name"].isin(cluster_names)]
        # Group by timestamp
        .groupby(["timestamp"])[["job_id"]]
        # And count jobs by counting column `job_id`
        .count()
    )
    # Compute cluster usage: number of jobs per cluster per timestamp
    f_stats["jobs_per_cluster"] = f_stats["job_id"] / len(cluster_names)
    # Compute average cluster usage
    avg = f_stats["jobs_per_cluster"].mean()
    # Compute standard deviation for cluster usage
    stddev = f_stats["jobs_per_cluster"].std()
    # Compute threshold to use for warnings: <average> - 2 * <standard deviation>
    threshold = max(0, avg - 2 * stddev)

    # List to collect warnings:
    reports = []
    # Set of cluster-timestamp associations found while checking warnings:
    founds = set()

    # Check cluster usage from data frame
    ff = (
        tf[tf["cluster_name"].isin(cluster_names)]
        .groupby(["cluster_name", "timestamp"])[["job_id"]]
        .count()
    )
    for row in ff.itertuples():
        cluster_name, timestamp = row.Index
        founds.add((cluster_name, timestamp))
        nb_jobs = row.job_id
        if nb_jobs < threshold:
            reports.append((cluster_name, timestamp, nb_jobs))

    # Check cluster usage for cluster-timestamp associations not yet found in dataframe
    # NB: For these cases, number of jobs is always 0
    for cluster_name in cluster_names:
        # Iter for each timestamp available in data frame
        for timestamp in sorted(tf["timestamp"].unique()):
            key = (cluster_name, timestamp)
            nb_jobs = 0
            if key not in founds and nb_jobs < threshold:
                reports.append((cluster_name, timestamp, nb_jobs))

    # Finally log warnings
    if reports:
        for cluster_name, timestamp, nb_jobs in reports:
            logger.warning(
                f"[{cluster_name}][{timestamp}] "
                f"insufficient cluster scraping: {nb_jobs} jobs / cluster / time unit; "
                f"minimum required: {threshold} ({avg} - 2 * {stddev}); time unit: {time_unit}"
            )
