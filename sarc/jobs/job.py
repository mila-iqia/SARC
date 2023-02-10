from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import validator
from pydantic_mongo import AbstractRepository, ObjectIdField

from ..config import MTL, UTC, BaseModel, config


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
    priority: int
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
