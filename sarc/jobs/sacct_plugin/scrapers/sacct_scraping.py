from datetime import datetime
import logging

from sarc.config import ClusterConfig
from sarc.jobs.sacct_plugin.scrapers.sacctjobscraper import SacctJobScraper
from sarc.traces import trace_decorator

logger = logging.getLogger(__name__)


@trace_decorator()
def sacct_acquire(
    cluster: ClusterConfig, day: datetime, no_prometheus: bool
    # TODO: determine if the Prometheus exchanges are part of scraping, parsing or both (cf no_prometheus arg)
) -> None:
    """
    Fetch sacct data and store it in the cache.

    Parameters: 
        cluster: ClusterConfig
            The configuration of the cluster on which to fetch the data.
        day: datetime
            The day for which to fetch the data. The time does not matter.
        no_prometheus: bool
            If True, avoid any scraping requiring prometheus connection.
    """
    scraper = SacctJobScraper(cluster)
    logger.info(f"Getting the sacct data for cluster {cluster.name}...")
    scraper.get_raw(day)
    logger.info("Sacct acquiring done.")
