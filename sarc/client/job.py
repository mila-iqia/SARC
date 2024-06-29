from __future__ import annotations

from datetime import datetime, time
from typing import Iterable, Optional

from pydantic_mongo import AbstractRepository, ObjectIdField

from sarc.config import TZLOCAL, UTC, BaseModel, ClusterConfig, config
from sarc.jobs.job import SlurmJob, SlurmState, jobs_collection


# pylint: disable=too-many-branches,dangerous-default-value
def _compute_jobs_query(
    *,
    cluster: str | ClusterConfig | None = None,
    job_id: int | list[int] | None = None,
    job_state: str | SlurmState | None = None,
    user: str | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
) -> dict:
    """Compute the MongoDB query dict to be used to match given arguments.

    Arguments:
        cluster: The cluster on which to search for jobs.
        job_id: The id or a list of ids to select.
        start: Get all jobs that have a status after that time.
        end: Get all jobs that have a status before that time.
        query_options: Additional options to pass to MongoDB (limit, etc.)
    """
    cluster_name = cluster
    if isinstance(cluster, ClusterConfig):
        cluster_name = cluster.name

    if isinstance(start, str):
        start = datetime.combine(
            datetime.strptime(start, "%Y-%m-%d"), time.min
        ).replace(tzinfo=TZLOCAL)
    if isinstance(end, str):
        end = (datetime.combine(datetime.strptime(end, "%Y-%m-%d"), time.min)).replace(
            tzinfo=TZLOCAL
        )

    if start is not None:
        start = start.astimezone(UTC)
    if end is not None:
        end = end.astimezone(UTC)

    query = {}
    if cluster_name:
        query["cluster_name"] = cluster_name

    if isinstance(job_id, int):
        query["job_id"] = job_id
    elif isinstance(job_id, list):
        query["job_id"] = {"$in": job_id}
    elif job_id is not None:
        raise TypeError(f"job_id must be an int or a list of ints: {job_id}")

    if end:
        # Select any job that had a status before the given end time.
        query["submit_time"] = {"$lt": end}

    if user:
        query["user"] = user

    if job_state:
        query["job_state"] = job_state

    if start:
        # Select jobs that had a status after the given time. This is a bit special
        # since we need to get both jobs that did not finish, and any job that ended after
        # the given time. This appears to require an $or, so we handle it after the others.
        query = {
            "$or": [
                {**query, "end_time": None},
                {**query, "end_time": {"$gt": start}},
            ]
        }

    return query


def count_jobs(
    *,
    cluster: str | ClusterConfig | None = None,
    job_id: int | list[int] | None = None,
    job_state: str | SlurmState | None = None,
    user: str | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    query_options: dict | None = None,
) -> int:
    """Count jobs that match the query.

    Arguments:
        cluster: The cluster on which to search for jobs.
        job_id: The id or a list of ids to select.
        start: Get all jobs that have a status after that time.
        end: Get all jobs that have a status before that time.
        query_options: Additional options to pass to MongoDB (limit, etc.)
    """
    query = _compute_jobs_query(
        cluster=cluster,
        job_id=job_id,
        job_state=job_state,
        user=user,
        start=start,
        end=end,
    )
    if query_options is None:
        query_options = {}
    return config().mongo.database_instance.jobs.count_documents(query, **query_options)


def get_jobs(
    *,
    cluster: str | ClusterConfig | None = None,
    job_id: int | list[int] | None = None,
    job_state: str | SlurmState | None = None,
    user: str | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    query_options: dict | None = None,
) -> Iterable[SlurmJob]:
    """Get jobs that match the query.

    Arguments:
        cluster: The cluster on which to search for jobs.
        job_id: The id or a list of ids to select.
        start: Get all jobs that have a status after that time.
        end: Get all jobs that have a status before that time.
        query_options: Additional options to pass to MongoDB (limit, etc.)
    """
    if query_options is None:
        query_options = {}

    query = _compute_jobs_query(
        cluster=cluster,
        job_id=job_id,
        job_state=job_state,
        user=user,
        start=start,
        end=end,
    )

    coll = jobs_collection()

    return coll.find_by(query, **query_options)


# pylint: disable=dangerous-default-value
def get_job(*, query_options={}, **kwargs):
    """Get a single job that matches the query, or None if nothing is found.

    Same signature as `get_jobs`.
    """
    # Sort by submit_time descending, which ensures we get the most recent version
    # of the job.
    jobs = get_jobs(
        **kwargs,
        query_options={**query_options, "sort": [("submit_time", -1)], "limit": 1},
    )
    for job in jobs:
        return job
    return None


class SlurmCLuster(BaseModel):
    """Hold data for a Slurm cluster."""

    # Database ID
    id: ObjectIdField = None

    cluster_name: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class SlurmClusterRepository(AbstractRepository[SlurmCLuster]):
    class Meta:
        collection_name = "clusters"


def get_available_clusters() -> Iterable[SlurmCLuster]:
    """Get clusters available in database."""
    db = config().mongo.database_instance
    return SlurmClusterRepository(database=db).find_by({})
