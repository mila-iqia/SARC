import logging

from sarc.jobs.job import get_jobs
from sarc.jobs.series import load_job_series

from .cache import Timespan, spancache

logger = logging.getLogger(__name__)


@spancache
def latest_jobs(timespan: Timespan):
    logger.info(f"Querying jobs from the last {timespan}...")
    start, end = timespan.calculate_bounds()
    jobs = get_jobs(start=start, end=end)
    return list(jobs)


@spancache
def latest_job_series(timespan: Timespan):
    logger.info(f"Querying job series from the last {timespan}...")
    start, end = timespan.calculate_bounds()
    return load_job_series(start=start, end=end)
