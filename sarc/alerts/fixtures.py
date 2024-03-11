import logging
from datetime import timedelta

from gifnoc.std import time

from sarc.jobs.job import get_jobs

from .cache import cache

logger = logging.getLogger(__name__)


@cache(hours=1)
def jobs_last_hour():
    logger.info("Querying jobs from the last hour...")
    jobs = get_jobs(
        start=time.now() - timedelta(hours=1),
        end=time.now(),
    )
    return list(jobs)


@cache(days=1)
def jobs_last_week():
    logger.info("Querying jobs from the last week...")
    jobs = get_jobs(
        start=time.now() - timedelta(days=7),
        end=time.now(),
    )
    return list(jobs)
