import math
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Annotated, Any

from pydantic import UUID4, BaseModel, BeforeValidator, field_validator


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


def float_nan_fallback(v: Any) -> Any:
    """Convert None to NaN"""
    if v is None:
        return math.nan
    return v


# Annotated float to accept None as value and convert it to NaN
SmartFloat = Annotated[float, BeforeValidator(float_nan_fallback)]


class Statistics(BaseModel):
    """Statistics for a timeseries."""

    mean: SmartFloat
    std: SmartFloat
    q05: SmartFloat
    q25: SmartFloat
    median: SmartFloat
    q75: SmartFloat
    max: SmartFloat
    unused: SmartFloat


class JobStatistics(BaseModel):
    """Statistics for a job."""

    gpu_utilization: Statistics | None = None
    gpu_utilization_fp16: Statistics | None = None
    gpu_utilization_fp32: Statistics | None = None
    gpu_utilization_fp64: Statistics | None = None
    gpu_sm_occupancy: Statistics | None = None
    gpu_memory: Statistics | None = None
    gpu_power: Statistics | None = None

    cpu_utilization: Statistics | None = None
    system_memory: Statistics | None = None

    def empty(self) -> bool:
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

    cpu: int | None = None
    mem: int | None = None
    node: int | None = None
    billing: int | None = None
    gres_gpu: int | None = None
    gpu_type: str | None = None


class SlurmJob(BaseModel):
    """Holds data for a Slurm job."""

    # job identification
    cluster_name: str
    account: str
    job_id: int
    array_job_id: int | None = None
    task_id: int | None = None
    name: str
    user: str
    group: str

    # status
    job_state: SlurmState
    exit_code: int | None = None
    signal: int | None = None

    # allocation information
    partition: str
    nodes: list[str]
    work_dir: str

    # Miscellaneous
    constraints: str | None = None
    priority: int | None = None
    qos: str | None = None

    # Flags
    CLEAR_SCHEDULING: bool = False
    STARTED_ON_SUBMIT: bool = False
    STARTED_ON_SCHEDULE: bool = False
    STARTED_ON_BACKFILL: bool = False

    # temporal fields
    time_limit: int | None = None
    submit_time: datetime
    start_time: datetime | None = None
    end_time: datetime | None = None
    elapsed_time: float
    # Latest period the job was scraped with sacct
    latest_scraped_start: datetime | None = None
    latest_scraped_end: datetime | None = None

    # tres
    requested: SlurmResources
    allocated: SlurmResources

    # statistics
    stored_statistics: JobStatistics | None = None

    # User ID
    user_uuid: UUID4 | None = None

    @field_validator(
        "submit_time",
        "start_time",
        "end_time",
        "latest_scraped_start",
        "latest_scraped_end",
    )
    @classmethod
    def _ensure_timezone(cls, v: datetime | None) -> datetime | None:
        """
        Store datetime in UTC

        **NB**: Naive dates are interpreted as in UTC.
        """
        if v is None:
            return None
        if v.tzinfo is None:
            v = v.replace(tzinfo=UTC)
        return v.astimezone(UTC)

    @property
    def duration(self) -> timedelta:
        if self.end_time is not None:
            assert self.start_time is not None
            return self.end_time - self.start_time

        return timedelta(seconds=0)

    # TODO: just rename stored_statistics to statistics
    # We keep it that way for backward compat for now
    def statistics(self) -> JobStatistics | None:
        return self.stored_statistics
