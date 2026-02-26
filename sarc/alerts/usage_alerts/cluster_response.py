import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from sarc.alerts.common import HealthCheck, CheckResult

logger = logging.getLogger(__name__)


def check_cluster_response(time_interval: timedelta = timedelta(days=7)) -> bool:
    """
    Check if we scraped clusters recently.
    Log an alert for each cluster not scraped since `time_interval` from now.

    Parameters
    ----------
    time_interval: timedelta
        Interval of time (until current time) in which we want to see cluster scrapings.
        For each cluster, if the latest scraping occurred before this period, an alert will be logged.
        Default is 7 days.
    Returns
    -------
    bool
        True if we scraped all clusters recently, False otherwise.
    """
    from sarc.config import UTC
    from sarc.client.job import get_available_clusters

    # Get current date
    current_date = datetime.now(tz=UTC)
    # Get the oldest date allowed from now
    oldest_allowed_date = current_date - time_interval
    # Check each available cluster
    ok = True
    for cluster in get_available_clusters():
        if cluster.end_time_sacct is None:
            logger.error(
                f"[{cluster.cluster_name}] no end_time_sacct available, cannot check last scraping"
            )
            ok = False
        else:
            # Cluster's latest scraping date should be in `cluster.end_time_sacct`.
            # NB: We assume cluster's `end_time_sacct` is stored as a date string,
            # so we must first convert it to a datetime object.
            cluster_end_date = datetime.strptime(
                cluster.end_time_sacct, "%Y-%m-%dT%H:%M"
            ).replace(tzinfo=UTC)
            # Now we can check.
            if cluster_end_date < oldest_allowed_date:
                logger.error(
                    f"[{cluster.cluster_name}] no scraping since {cluster_end_date}, "
                    f"oldest required: {oldest_allowed_date}, "
                    f"current time: {current_date}"
                )
                ok = False
    return ok


@dataclass
class ClusterResponseCheck(HealthCheck):
    """Health check for cluster response"""

    time_interval: timedelta = timedelta(days=7)

    def check(self) -> CheckResult:
        if check_cluster_response(time_interval=self.time_interval):
            return self.ok()
        else:
            return self.fail()
