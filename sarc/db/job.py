from datetime import datetime

from pydantic import UUID4, PrivateAttr
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import attribute_keyed_dict, relationship
from sqlmodel import Field, SQLModel, UniqueConstraint
from sqlmodel.main import Relationship

from sarc.core.models.job import SlurmState

from .users import UserDB


class JobNodesDB(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    node_name: str
    job_id: int = Field(foreign_key="slurm_jobs.id", index=True)


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
    __table_args__ = (UniqueConstraint("cluster_name", "job_id", "submit_time"),)

    id: int | None = Field(default=None, primary_key=True)
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
    _nodesdb: list[JobNodesDB] = Relationship()
    _node_list: AssociationProxy[list[str]] = PrivateAttr(
        association_proxy(
            "_nodesdb", "node_name", creator=lambda n: JobNodesDB(node_name=n)
        )
    )

    @property
    def nodes(self) -> list[str]:
        return self._node_list

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

    statistics: dict[str, JobStatisticDB] = Relationship(
        sa_relationship=relationship(
            JobStatisticDB, collection_class=attribute_keyed_dict("name")
        )
    )

    # User ID
    user_uuid: UUID4 = Field(foreign_key="users.uuid")
    sarc_user: UserDB = Relationship()
