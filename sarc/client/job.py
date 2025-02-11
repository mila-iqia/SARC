from __future__ import annotations

from datetime import datetime, time, timedelta
from enum import Enum
from typing import Iterable, Optional

from pydantic import validator
from pydantic_mongo import AbstractRepository, ObjectIdField

from sarc.traces import trace_decorator

from ..config import (
    MTL,
    TZLOCAL,
    UTC,
    BaseModel,
    ClusterConfig,
    config,
    scraping_mode_required,
)


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


class Statistics(BaseModel):
    """Statistics for a timeseries."""

    mean: float
    std: float
    q05: float
    q25: float
    median: float
    q75: float
    max: float
    unused: int


class JobStatistics(BaseModel):
    """Statistics for a job."""

    gpu_utilization: Optional[Statistics]
    gpu_utilization_fp16: Optional[Statistics]
    gpu_utilization_fp32: Optional[Statistics]
    gpu_utilization_fp64: Optional[Statistics]
    gpu_sm_occupancy: Optional[Statistics]
    gpu_memory: Optional[Statistics]
    gpu_power: Optional[Statistics]

    cpu_utilization: Optional[Statistics]
    system_memory: Optional[Statistics]

    def empty(self):
        return (
            self.gpu_utilization is None
            and self.gpu_utilization_fp16 is None
            and self.gpu_utilization_fp32 is None
            and self.gpu_utilization_fp64 is None
            and self.gpu_sm_occupancy is None
            and self.gpu_memory is None
            and self.gpu_power is None
            and self.cpu_utilization is None
            and self.system_memory is None
        )


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
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    elapsed_time: int

    # tres
    requested: SlurmResources
    allocated: SlurmResources

    # statistics
    stored_statistics: Optional[JobStatistics] = None

    @validator("submit_time", "start_time", "end_time")
    def _ensure_timezone(cls, v):
        # We'll store in MTL timezone because why not
        return v and v.replace(tzinfo=UTC).astimezone(MTL)

    @property
    def duration(self):
        if self.end_time:
            return self.end_time - self.start_time

        return timedelta(seconds=0)

    @scraping_mode_required
    def series(self, **kwargs):
        from sarc.jobs.series import (  # pylint: disable=cyclic-import
            get_job_time_series,
        )

        return get_job_time_series(job=self, **kwargs)

    @trace_decorator()
    @scraping_mode_required
    def statistics(self, recompute=False, save=True, overwrite_when_empty=False):
        from sarc.jobs.series import (  # pylint: disable=cyclic-import
            compute_job_statistics,
        )

        if self.stored_statistics and not recompute:
            return self.stored_statistics
        elif self.end_time and self.fetch_cluster_config().prometheus_url:
            statistics = compute_job_statistics(self)
            if save and (
                overwrite_when_empty
                or not self.stored_statistics
                or not statistics.empty()
            ):
                self.stored_statistics = statistics
                self.save()
            return statistics

        return None

    @scraping_mode_required
    def save(self):
        _jobs_collection().save_job(self)

    @scraping_mode_required
    def fetch_cluster_config(self):
        """This function is only available on the admin side"""
        return config().clusters[self.cluster_name]


class SlurmJobRepository(AbstractRepository[SlurmJob]):
    class Meta:
        collection_name = "jobs"

    @scraping_mode_required
    def save_job(self, model: SlurmJob):
        """Save a SlurmJob into the database.

        Note: This overrides AbstractRepository's save function to do an upsert when
        the id is provided.
        """
        document = self.to_document(model)
        # Resubmitted jobs have the same job ID can be distinguished by their submit time,
        # as per sacct's documentation.
        return self.get_collection().update_one(
            {
                "job_id": model.job_id,
                "cluster_name": model.cluster_name,
                "submit_time": model.submit_time,
            },
            {"$set": document},
            upsert=True,
        )


def _jobs_collection():
    """Return the jobs collection in the current MongoDB."""
    db = config().mongo.database_instance
    return SlurmJobRepository(database=db)


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

    coll = _jobs_collection()

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
    billing_is_gpu: bool = False


class SlurmClusterRepository(AbstractRepository[SlurmCLuster]):
    class Meta:
        collection_name = "clusters"


def get_available_clusters() -> Iterable[SlurmCLuster]:
    """Get clusters available in database."""
    db = config().mongo.database_instance
    return SlurmClusterRepository(database=db).find_by({})
