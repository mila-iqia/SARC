# pylint: disable=dangerous-default-value

from datetime import datetime, time, timedelta
from enum import Enum
from typing import Optional, Union

from pydantic import validator
from pydantic_mongo import AbstractRepository, ObjectIdField

from ..config import MTL, TZLOCAL, UTC, BaseModel, ClusterConfig, config


class SlurmState(str, Enum):
    """Possible Slurm job states.

    Reference: https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES
    """

    BOOT_FAIL = "BOOT_FAIL"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    CONFIGURING = "CONFIGURING"
    COMPLETING = "COMPLETING"
    DEADLINE = "DEADLINE"
    FAILED = "FAILED"
    NODE_FAIL = "NODE_FAIL"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    PENDING = "PENDING"
    PREEMPTED = "PREEMPTED"
    RUNNING = "RUNNING"
    RESV_DEL_HOLD = "RESV_DEL_HOLD"
    REQUEUE_FED = "REQUEUE_FED"
    REQUEUE_HOLD = "REQUEUE_HOLD"
    REQUEUED = "REQUEUED"
    RESIZING = "RESIZING"
    REVOKED = "REVOKED"
    SIGNALING = "SIGNALING"
    SPECIAL_EXIT = "SPECIAL_EXIT"
    STAGE_OUT = "STAGE_OUT"
    STOPPED = "STOPPED"
    SUSPENDED = "SUSPENDED"
    TIMEOUT = "TIMEOUT"


class SlurmResources(BaseModel):
    """Counts for various resources."""

    cpu: Optional[int]
    mem: Optional[int]
    node: Optional[int]
    billing: Optional[int]
    gres_gpu: Optional[int]
    gpu_type: Optional[str]


class SlurmJob(BaseModel):
    """Holds data for a Slurm job."""

    # Database ID
    id: ObjectIdField = None

    # job identification
    cluster_name: str
    account: str
    job_id: int
    array_job_id: Optional[int]
    task_id: Optional[int]
    name: str
    user: str
    group: str

    # status
    job_state: SlurmState
    exit_code: Optional[int]
    signal: Optional[int]

    # allocation information
    partition: str
    nodes: list[str]
    work_dir: str

    # Miscellaneous
    constraints: Optional[str]
    priority: Optional[int]
    qos: Optional[str]

    # Flags
    CLEAR_SCHEDULING: bool = False
    STARTED_ON_SUBMIT: bool = False
    STARTED_ON_SCHEDULE: bool = False
    STARTED_ON_BACKFILL: bool = False

    # temporal fields
    time_limit: Optional[int]
    submit_time: datetime
    start_time: datetime
    end_time: Optional[datetime]
    elapsed_time: int

    # tres
    requested: SlurmResources
    allocated: SlurmResources

    @validator("submit_time", "start_time", "end_time")
    def _ensure_timezone(cls, v):
        # We'll store in MTL timezone because why not
        return v and v.replace(tzinfo=UTC).astimezone(MTL)

    @property
    def cluster(self):
        return config().clusters[self.cluster_name]

    @property
    def duration(self):
        if self.end_time:
            return self.end_time - self.start_time

        return timedelta(seconds=0)

    def series(self, **kwargs):
        from .series import get_job_time_series  # pylint: disable=cyclic-import

        return get_job_time_series(job=self, **kwargs)


class SlurmJobRepository(AbstractRepository[SlurmJob]):
    class Meta:
        collection_name = "jobs"

    def save_job(self, model: SlurmJob):
        """Save a SlurmJob into the database.

        Note: This overrides AbstractRepository's save function to do an upsert when
        the id is provided.
        """
        document = self.to_document(model)
        return self.get_collection().update_one(
            {"job_id": model.job_id, "cluster_name": model.cluster_name},
            {"$set": document},
            upsert=True,
        )


def jobs_collection():
    """Return the jobs collection in the current MongoDB."""
    db = config().mongo.instance
    return SlurmJobRepository(database=db)


def get_jobs(
    *,
    cluster: Union[str, ClusterConfig, None] = None,
    job_id: Union[int, list[int], None] = None,
    job_state: Union[str, SlurmState, None] = None,
    username: Union[str, None] = None,
    start: Union[str, datetime, None] = None,
    end: Union[str, datetime, None] = None,
    query_options: dict = {},
) -> list[SlurmJob]:
    """Get jobs that match the query.

    Arguments:
        cluster: The cluster on which to search for jobs.
        job_id: The id or a list of ids to select.
        start: Get all jobs that have a status after that time.
        end: Get all jobs that have a status before that time.
        query_options: Additional options to pass to MongoDB (limit, etc.)
    """
    if isinstance(cluster, str):
        cluster = config().clusters[cluster]

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
    if isinstance(cluster, ClusterConfig):
        query["cluster_name"] = cluster.name

    if isinstance(job_id, int):
        query["job_id"] = job_id
    elif isinstance(job_id, list) and job_id:
        query["job_id"] = {"$in": job_id}
    else:
        raise TypeError(f"job_id must be an int or a list of ints: {job_id}")

    if end:
        # Select any job that had a status before the given end time.
        query["submit_time"] = {"$lt": end}

    if username:
        query["user"] = username

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

    coll = jobs_collection()

    return coll.find_by(query, **query_options)


def get_job(*, query_options={}, **kwargs):
    """Get a single job that matches the query, or None if nothing is found.

    Same signature as `get_jobs`.
    """
    jobs = get_jobs(**kwargs, query_options={**query_options, "limit": 1})
    for job in jobs:
        return job
    return None
