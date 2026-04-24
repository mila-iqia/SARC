from datetime import datetime

from pydantic import UUID4
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import attribute_keyed_dict, relationship
from sqlmodel import Field, SQLModel, UniqueConstraint
from sqlmodel.main import Relationship

from sarc.core.models.job import SlurmState

from .users import UserDB


class JobStatisticDB(SQLModel, table=True):
    """Statistics for a timeseries."""

    id: int | None = Field(default=None, primary_key=True)
    name: str
    mean: float | None
    std: float | None
    q05: float | None
    q25: float | None
    median: float | None
    q75: float | None
    max: float | None
    unused: float | None


class SlurmJobDB(SQLModel, table=True):
    __tablename__ = "slurm_jobs"
    __table_args__ = (UniqueConstraint("cluster_id", "job_id", "submit_time"),)

    id: int | None = Field(default=None, primary_key=True)
    # job identification
    cluster_id: int = Field(foreign_key="clusters.id")
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
    nodes: list[str] = Field(sa_type=JSONB)

    work_dir: str
    submit_line: str  # new

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

    statistics: dict[str, JobStatisticDB] = Relationship(
        sa_relationship=relationship(
            JobStatisticDB, collection_class=attribute_keyed_dict("name")
        )
    )

    # User ID
    user_uuid: UUID4 = Field(foreign_key="users.uuid")
    sarc_user: UserDB = Relationship()
