import logging

from sarc.jobs.job import get_jobs

from .cache import Timespan, spancache

logger = logging.getLogger(__name__)


@spancache
def latest_jobs(timespan: Timespan):
    logger.info(f"Querying jobs from the last {timespan}...")
    start, end = timespan.calculate_bounds()
    jobs = get_jobs(start=start, end=end)
    return list(jobs)
