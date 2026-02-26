import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict

from tqdm import tqdm

from sarc.alerts.common import HealthCheck, CheckResult
from sarc.core.models.validators import datetime_utc

logger = logging.getLogger(__name__)


def check_same_job_id(
    time_interval: timedelta | None = timedelta(days=7),
    since: datetime_utc | None = None,
) -> bool:
    """
    Check if there are many jobs with same job ID in given time interval.
    Log an alert for each duplicated job ID.

    Parameters
    ----------
    time_interval: timedelta
        If given, only jobs which ran in a certain interval of time will be used for checking.
        If None, all jobs are used.
        If `since` is given, interval of time is [since, since + time_interval]
        If `since` not given, interval of time is [now - time_interval, now]
        Default is 7 days.
    since: datetime
        If given, only jobs which ran after `since` will be checked.

    Returns
    -------
    bool
        True if there are no duplicated job IDs (check succeeds), False otherwise.
    """
    from sarc.client import count_jobs, get_jobs
    from sarc.config import UTC

    # Compute parameters `start` and `end` for function `get_jobs()`
    if since is None:
        if time_interval is None:
            start: datetime | None = None
            end: datetime | None = None
        else:
            end = datetime.now(tz=UTC)
            start = end - time_interval
    elif time_interval is None:
        start = since
        end = None
    else:
        start = since
        end = start + time_interval

    nb_jobs = count_jobs(start=start, end=end)

    # Collect job indices, and count occurrences of clusters
    # among jobs which have same job ID.
    job_id_to_cluster_to_count: Dict[int, Counter] = {}
    for job in tqdm(get_jobs(start=start, end=end), total=nb_jobs, desc="get jobs"):
        job_id_to_cluster_to_count.setdefault(job.job_id, Counter()).update(
            [job.cluster_name]
        )

    # Find duplicates.
    duplicates = {
        job_id: cluster_to_count
        for job_id, cluster_to_count in job_id_to_cluster_to_count.items()
        if sum(cluster_to_count.values()) > 1
    }
    if duplicates:
        # Log alerts
        if start:
            time_message = str(start)
            if end:
                time_message += f" until {end}"
        else:
            time_message = "always"
        # General alert
        logger.error(
            f"[duplicated job indices] found {len(duplicates)} duplicated job_id since {time_message}"
        )
        # Alert for each duplicated job ID
        # Display job ID, and number of jobs having this ID on each cluster
        for job_id, cluster_to_count in duplicates.items():
            message = (
                f"[duplicated job ID: {job_id}] on "
                + ", ".join(
                    f"{cluster_name} ({count} job{'s' if count > 1 else ''})"
                    for cluster_name, count in cluster_to_count.most_common()
                )
                + f", since {time_message}"
            )
            logger.error(message)

    return not duplicates


@dataclass
class SameJobIdCheck(HealthCheck):
    time_interval: timedelta | None = timedelta(days=7)
    since: datetime_utc | None = None

    def check(self) -> CheckResult:
        if check_same_job_id(time_interval=self.time_interval, since=self.since):
            return self.ok()
        else:
            return self.fail()
