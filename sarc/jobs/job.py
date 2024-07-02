from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum
from functools import cache
from typing import Optional

from pydantic import validator
from pydantic_mongo import AbstractRepository, ObjectIdField

from sarc.traces import trace_decorator

from ..config import MTL, UTC, BaseModel, config, scraping_mode_required


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
        from .series import get_job_time_series  # pylint: disable=cyclic-import

        return get_job_time_series(job=self, **kwargs)

    @trace_decorator()
    @scraping_mode_required
    def statistics(self, recompute=False, save=True, overwrite_when_empty=False):
        from .series import compute_job_statistics  # pylint: disable=cyclic-import

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
        jobs_collection().save_job(self)

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


def jobs_collection():
    """Return the jobs collection in the current MongoDB."""
    db = config().mongo.database_instance
    return SlurmJobRepository(database=db)


@cache
def get_clusters():
    """Fetch all possible clusters"""
    # TODO Is this function still useful ? Currently used only in sarc.cli.utils
    jobs = jobs_collection().get_collection()
    return jobs.distinct("cluster_name", {})
