from datetime import datetime
from types import SimpleNamespace
from typing import Self

from iguane.fom import RAWDATA, fom_ugr
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import attribute_keyed_dict, relationship
from sqlmodel import CheckConstraint, Field, Session, UniqueConstraint, select
from sqlmodel.main import Relationship

from sarc.db.cluster import SlurmClusterDB
from sarc.db.sqlmodel import SQLModel, datetime_utc_field
from sarc.db.users import UserDB
from sarc.models.job import SlurmState
from sarc.validators import datetime_utc


class JobStatisticDB(SQLModel, table=True):
    """Statistics for a timeseries."""

    id: int | None = Field(default=None, primary_key=True)
    job_id: int | None = Field(
        default=None, foreign_key="slurm_jobs.id", nullable=False, ondelete="CASCADE"
    )
    name: str | None = Field(default=None, nullable=False)
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
    __table_args__ = (
        UniqueConstraint("cluster_id", "job_id", "submit_time"),
        CheckConstraint("submit_time <= start_time"),
        CheckConstraint("start_time <= end_time"),
    )

    id: int | None = Field(default=None, primary_key=True)
    # job identification
    cluster_id: int = Field(foreign_key="clusters.id", ondelete="RESTRICT")
    cluster: SlurmClusterDB = Relationship()
    account: str
    job_id: int
    array_job_id: int | None = None
    task_id: int | None = None
    name: str
    cluster_user: str
    group: str

    # status
    job_state: SlurmState
    exit_code: int | None = None
    signal: int | None = None

    # allocation information
    partition: str
    nodes: list[str] = Field(sa_type=JSONB)

    work_dir: str
    submit_line: str | None  # new

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
    submit_time: datetime_utc = datetime_utc_field()
    start_time: datetime_utc | None = datetime_utc_field(default=None)
    end_time: datetime_utc | None = datetime_utc_field(default=None)
    elapsed_time: float
    # Latest period the job was scraped with sacct
    latest_scraped_start: datetime_utc | None = datetime_utc_field(default=None)
    latest_scraped_end: datetime_utc | None = datetime_utc_field(default=None)

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
    # Harmonized version or allocated_gpu_type. If not None, should exist in GpuRguDB.
    harmonized_gpu_type: str | None = Field(
        foreign_key="gpurgudb.name", default=None, ondelete="SET NULL"
    )

    statistics: dict[str, JobStatisticDB] = Relationship(
        sa_relationship=relationship(
            JobStatisticDB, collection_class=attribute_keyed_dict("name")
        )
    )

    # User ID
    sarc_user_id: int = Field(foreign_key="users.id", ondelete="RESTRICT")
    sarc_user: UserDB = Relationship()

    @classmethod
    def get_or_create(cls, sess: Session, **kwargs) -> Self:
        res = cls.model_validate(kwargs)
        res.id = sess.exec(
            select(SlurmJobDB.id).where(
                SlurmJobDB.cluster_id == res.cluster_id,
                SlurmJobDB.job_id == res.job_id,
                SlurmJobDB.submit_time == res.submit_time,
            )
        ).one_or_none()
        return sess.merge(res)

    @classmethod
    def by_ref(
        cls, sess: Session, cluster_id: int, job_id: int, submit_time: datetime
    ) -> Self | None:
        return sess.exec(
            select(cls).where(
                cls.cluster_id == cluster_id,
                cls.job_id == job_id,
                cls.submit_time == submit_time,
            )
        ).one_or_none()


def get_rgus(rgu_version: str = "1.0") -> dict[str, float]:
    """
    Return GPU->RGU mapping for given RGU version.

    Get mapping from package IGUANE.
    """
    args = SimpleNamespace(fom_version=rgu_version, custom_weights=None, norm=False)
    gpus = sorted(RAWDATA.keys())
    return {gpu: fom_ugr(gpu, args=args) for gpu in gpus}
