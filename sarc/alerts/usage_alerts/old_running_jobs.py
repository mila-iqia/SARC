import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlmodel import col, select
from tqdm import tqdm

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.validators import datetime_utc

logger = logging.getLogger(__name__)


def check_old_running_jobs(since: datetime_utc | None = None) -> bool:
    """
    Check if database contains RUNNING jobs which should have already finished.

    Parameters
    ----------
    since: datetime_utc | None
        Date since when we must check jobs. Only jobs with a submit time >= `since` will be checked.
        If None, all jobs are checked.

    Return
    ------
    bool
        True if check succeeds (no old RUNNING job in database), False otherwise.
    """
    from sarc.config import config
    from sarc.db.job import SlurmJobDB
    from sarc.models.job import SlurmState

    now = datetime.now(tz=UTC)
    jobs_over_limit: list[SlurmJobDB] = []
    with config().db.session() as sess:
        query = select(SlurmJobDB).where(
            SlurmJobDB.job_state == SlurmState.RUNNING,
            col(SlurmJobDB.time_limit).is_not(None),
        )
        if since is not None:
            query = query.where(SlurmJobDB.submit_time >= since)
        for job in sess.exec(query):
            assert job.job_state == SlurmState.RUNNING, job
            assert job.start_time is not None, job
            assert job.end_time is None, job
            assert job.time_limit is not None, job
            # A running job should have already finished
            # if maximum allowed end time is before current time.
            max_end_time = job.start_time + timedelta(seconds=job.time_limit)
            if max_end_time < now:
                jobs_over_limit.append(job)

    if jobs_over_limit:
        # We have old RUNNING jobs
        # Check if this job was re-submitted with a more recent status

        # First, get job keys :cluster name + job ID
        # NB: Database may contain many job entries with same cluster name,
        # same job ID, AND same job state `RUNNING`
        index_jobs: set[tuple[int, int]] = {
            (job.cluster_id, job.job_id) for job in jobs_over_limit
        }
        job_story: dict[tuple[int, int], list[SlurmJobDB]] = {}
        for cluster_id, job_id in tqdm(
            index_jobs, total=len(index_jobs), desc="running job states"
        ):
            local_query = select(SlurmJobDB).where(
                SlurmJobDB.cluster_id == cluster_id, SlurmJobDB.job_id == job_id
            )
            if since is not None:
                local_query = local_query.where(SlurmJobDB.submit_time >= since)
            local_jobs = sorted(
                sess.exec(local_query).all(), key=lambda jdb: jdb.submit_time
            )
            assert local_jobs
            job_story[(cluster_id, job_id)] = local_jobs

        # Now we get some stats

        # nb. initial entries
        nb_entries = len(jobs_over_limit)

        # nb. initial jobs
        assert len(index_jobs) == len(job_story)
        nb_jobs = len(index_jobs)

        # nb. jobs not re-submitted
        nb_uniques = 0

        # nb. latest found states for re-submitted jobs
        nb_latest_state: Counter[SlurmState] = Counter()

        for jobs in job_story.values():
            if len(jobs) == 1:
                nb_uniques += 1
            else:
                # Jobs are already sorted by submit time
                # Get and count latest job state
                latest_job = jobs[-1]
                nb_latest_state.update([latest_job.job_state])
        # Now log detailed error
        message = f"Found {nb_entries} RUNNING job entries"
        if since is not None:
            message += f", submitted since {since},"
        message += " which should have already finished"
        if nb_entries != nb_jobs:
            message += f", distributed in {nb_jobs} jobs (cluster name + job ID)"
        message += f", from which {nb_uniques} not re-submitted"
        for latest_state, latest_state_count in nb_latest_state.most_common():
            message += f", {latest_state_count} with a latest entry {latest_state.name}"
        logger.error(message)

    return not jobs_over_limit


@dataclass
class OldRunningJobCheck(HealthCheck):
    since: datetime_utc | None = None

    def check(self) -> CheckResult:
        if check_old_running_jobs(since=self.since):
            return self.ok()
        else:
            return self.fail()
