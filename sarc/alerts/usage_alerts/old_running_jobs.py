import logging
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta, datetime, UTC
from typing import Any

from tqdm import tqdm

from sarc.alerts.common import HealthCheck, CheckResult
from sarc.client.job import SlurmJob
from sarc.core.models.validators import datetime_utc

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
    from sarc.client.job import _jobs_collection, SlurmState

    now = datetime.now(tz=UTC)
    coll_jobs = _jobs_collection()
    db_jobs = coll_jobs.get_collection()
    # Search RUNNING jobs
    base_query: dict[str, Any] = {"job_state": SlurmState.RUNNING}
    if since is not None:
        base_query["submit_time"] = {"$gte": since}
    expected = db_jobs.count_documents(base_query)
    jobs_over_limit: list[SlurmJob] = []
    for job in tqdm(
        coll_jobs.find_by(base_query), total=expected, desc="running job(s)"
    ):
        assert job.job_state == SlurmState.RUNNING, job
        assert job.start_time is not None, job
        assert job.end_time is None, job
        # A running job should have already finished
        # if maximum allowed end time is before current time.
        max_end_time = job.start_time + timedelta(seconds=job.time_limit)
        if max_end_time < now:
            jobs_over_limit.append(job)

    if jobs_over_limit:
        # We have old RUNNING jobs
        # Check if this job was re-submitted with a more recent status

        # First, classify job entries by key :cluster name + job ID
        # NB: Database may contain many job entries with same cluster name,
        # same job ID, AND same job state `RUNNING`
        index_jobs = {}
        for job in jobs_over_limit:
            key = (job.cluster_name, job.job_id)
            index_jobs.setdefault(key, []).append(job)

        # Now search potential re-submitted entries.
        # Look for all collected job indices,
        # and only non-RUNNING jobs
        resubmitted_query: dict[str, Any] = {
            "job_id": {"$in": [job.job_id for job in jobs_over_limit]},
            "job_state": {"$ne": SlurmState.RUNNING},
        }
        if since is not None:
            resubmitted_query["submit_time"] = {"$gte": since}
        resubmitted_expected = db_jobs.count_documents(resubmitted_query)
        for job in tqdm(
            coll_jobs.find_by(resubmitted_query),
            total=resubmitted_expected,
            desc="requeued job(s)",
        ):
            # Keep only resubmitted jobs with an already found key: cluster name + job ID
            key = (job.cluster_name, job.job_id)
            if key in index_jobs:
                index_jobs[key].append(job)

        # Now we get some stats
        nb_entries = len(jobs_over_limit)  # nb. initial entries
        nb_jobs = len(index_jobs)  # nb. initial jobs
        nb_uniques = 0  # nb. jobs not re-submitted
        nb_latest_state = Counter()  # nb. latest found states for re-submitted jobs
        for jobs in index_jobs.values():
            if len(jobs) == 1:
                nb_uniques += 1
            else:
                # Sort jobs by submit time
                jobs = sorted(jobs, key=lambda job: job.submit_time)
                # Get and count latest job state
                latest_job = jobs[-1]
                nb_latest_state.update([latest_job.job_state])
        # Now log detailed error
        message = (
            f"Found {nb_entries} RUNNING job entries which should have already finished"
        )
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
