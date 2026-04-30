import math
from datetime import UTC, datetime
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, Field, field_validator

from sarc.db.job import SlurmState


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
    requested_cpu: int | None = None
    requested_mem: int | None = None
    requested_node: int | None = None
    requested_billing: int | None = None
    requested_gres_gpu: int | None = None
    requested_gpu_type: str | None = None

    allocated_cpu: int | None = None
    allocated_mem: int | None = None
    allocated_node: int | None = None
    allocated_billing: int | None = None
    allocated_gres_gpu: int | None = None
    allocated_gpu_type: str | None = None

    # statistics
    statistics: dict[str, Statistics] = Field(default_factory=dict)

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
