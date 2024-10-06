import logging
from datetime import datetime, time, timedelta

from sarc.client.job import get_available_clusters
from sarc.config import MTL

logger = logging.getLogger(__name__)


def check_cluster_response(time_interval: timedelta = timedelta(days=7)):
    """
    Check if we scraped clusters recently.
    Log a warning for each cluster not scraped since `time_interval` from now.

    Parameters
    ----------
    time_interval: timedelta
        Interval of time (until current time) in which we want to see cluster scrapings.
        For each cluster, if the latest scraping occurred before this period, a warning will be logged.
        Default is 7 days.
    """
    # Get current date
    current_date = datetime.now(tz=MTL)
    # Get the oldest date allowed from now
    oldest_allowed_date = current_date - time_interval
    # Check each available cluster
    for cluster in get_available_clusters():
        if cluster.end_date is None:
            logger.warning(
                f"[{cluster.cluster_name}] no end_date available, cannot check last scraping"
            )
        else:
            # Cluster's latest scraping date should be in `cluster.end_date`.
            # NB: We assume cluster's `end_date` is stored as a date string,
            # so we must first convert it to a datetime object.
            # `en_date` is parsed the same way as start/end parameters in `get_jobs()`.
            cluster_end_date = datetime.combine(
                datetime.strptime(cluster.end_date, "%Y-%m-%d"), time.min
            ).replace(tzinfo=MTL)
            # Now we can check.
            if cluster_end_date < oldest_allowed_date:
                logger.warning(
                    f"[{cluster.cluster_name}] no scraping since {cluster_end_date}, "
                    f"oldest required: {oldest_allowed_date}, "
                    f"current time: {current_date}"
                )
