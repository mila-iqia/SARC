from types import SimpleNamespace
from typing import Self

from iguane.fom import RAWDATA, fom_ugr
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import attribute_keyed_dict, relationship
from sqlmodel import BIGINT, Field, Index, Session, UniqueConstraint, select
from sqlmodel.main import Relationship

from sarc.db.cluster import SlurmClusterDB
from sarc.db.sqlmodel import SQLModel, datetime_utc_field
from sarc.db.users import UserDB
from sarc.models.job import SlurmState
from sarc.validators import datetime_utc


class JobStatisticDB(SQLModel, table=True):
    """Statistics for a timeseries."""

    __table_args__ = (
        # /dash joins filter by name then read mean/max: name-first so `name = X`
        # scans only that name's rows, INCLUDE (mean, max) makes the join index-only.
        Index(
            "ix_jobstatisticdb_name_job_covering",
            "name",
            "job_id",
            postgresql_include=["mean", "max"],
            unique=True,
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    job_id: int | None = Field(
        default=None,
        foreign_key="slurm_jobs.id",
        nullable=False,
        ondelete="CASCADE",
        index=True,
    )
    # `name` is already indexed in covering index ix_jobstatisticdb_name_job_covering above.
    # job_id already has its own index above, for the join itself.
    name: str | None = Field(default=None, nullable=False)
    mean: float | None
    std: float | None
    q05: float | None
    q25: float | None
    median: float | None
    q75: float | None
    max: float | None


class JobStatisticsFetchDateDB(SQLModel, table=True):
    """Tracks when we last attempted to fetch Prometheus stats for a job."""

    __tablename__ = "jobstatistics_fetchdate"
    __table_args__ = (UniqueConstraint("job_id"),)

    id: int | None = Field(default=None, primary_key=True)
    job_id: int = Field(foreign_key="slurm_jobs.id", nullable=False, ondelete="CASCADE")
    fetch_date: datetime_utc = datetime_utc_field()
    jobstatistic_id: int | None = Field(
        default=None,
        foreign_key="jobstatisticdb.id",
        nullable=True,
        ondelete="SET NULL",
    )


# The instant a job's run stopped, for the /dash "was it running then?" queries:
# a run meets [lo, hi) exactly when this is above lo and start_time below hi.
#
# A function only to be IMMUTABLE, which an index requires: `timestamptz +
# interval` is merely STABLE (days/months shift with the session TimeZone),
# while make_interval(secs => ...) is absolute. NULL is "meets no window": STRICT
# gives that to a job that never started, and the CASE to a run of no length,
# which occupies no instant (the incoherent rows -- end before start, start
# before submit -- are all zero-elapsed). Both then drop out of the comparison
# above, with no second test to forget.
#
# Registered with alembic-utils in alembic/env.py. Alembic replaces the function
# in place, leaving the indexes below on the old result: a body change that
# alters the result must REINDEX in the same migration.
SLURM_JOB_END_SIGNATURE = (
    "slurm_job_end(job_start timestamptz, job_elapsed double precision)"
)
SLURM_JOB_END_DEFINITION = """
returns timestamptz
language sql
immutable
strict
parallel safe
as $$ select case when job_elapsed > 0 then job_start + make_interval(secs => job_elapsed) end $$
"""


class SlurmJobDB(SQLModel, table=True):
    __tablename__ = "slurm_jobs"
    __table_args__ = (
        Index(
            "ix_job_unique",
            "cluster_id",
            "submit_time",
            "job_id",
            unique=True,
            postgresql_include=["id"],
        ),
        # Covering index for any query based on submit_time
        # (dashboard, Prometheus scraping, etc.). Covering so that a scan
        # over a submit range gets what it filters and groups on without
        # reading the table; displaying a job's other columns still does.
        Index(
            "ix_slurm_jobs_submit",
            "submit_time",
            postgresql_include=[
                "id",
                "harmonized_gpu_type",
                "allocated_gpu_type",
                "allocated_gres_gpu",
                "elapsed_time",
                "cluster_id",
                "cluster_user",
                "sarc_user_id",  # used by view when joining users and member_type
                "start_time",  # with time_limit and job_state, what the
                "time_limit",  # time-limit heatmaps read beyond the above
                "job_state",
            ],
        ),
        # "Which jobs were running during a period" is `end > lo AND start < hi`,
        # two scalar comparisons over one indexed expression. Covering, so the
        # window never reaches the heap; and a btree scan parallelizes, which no
        # GiST scan does.
        #
        # Partial because every /dash plot is about GPU jobs (_gpu_only in
        # sarc/api/metrics.py). Its predicate must stay the one the
        # endpoints write, or the planner cannot prove the index applies.
        Index(
            "ix_slurm_jobs_end_gpu",
            text("slurm_job_end(start_time, elapsed_time)"),
            postgresql_include=[
                "start_time",  # with elapsed_time: the expression's own inputs,
                "elapsed_time",  # which an index-only scan must be able to return
                "id",
                "harmonized_gpu_type",
                "allocated_gres_gpu",
                "cluster_id",
                "cluster_user",
                "sarc_user_id",  # used by view when joining users and member_type
                "job_state",
            ],
            postgresql_where=text(
                "allocated_gres_gpu > 0 AND harmonized_gpu_type IS NOT NULL"
            ),
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    # job identification
    cluster_id: int = Field(foreign_key="clusters.id", ondelete="RESTRICT")
    cluster: SlurmClusterDB = Relationship(passive_deletes="all")
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
    # Indexed via ix_slurm_jobs_submit_id in __table_args__ (covering, INCLUDE id).
    submit_time: datetime_utc = datetime_utc_field()
    start_time: datetime_utc | None = datetime_utc_field(default=None)
    end_time: datetime_utc | None = datetime_utc_field(default=None)
    elapsed_time: float
    # Latest period the job was scraped with sacct
    latest_scraped_start: datetime_utc | None = datetime_utc_field(default=None)
    latest_scraped_end: datetime_utc | None = datetime_utc_field(default=None)

    # tres
    requested_cpu: int | None = Field(default=None, sa_type=BIGINT)
    requested_mem: int | None = Field(default=None, sa_type=BIGINT)
    requested_node: int | None = Field(default=None, sa_type=BIGINT)
    requested_billing: int | None = Field(default=None, sa_type=BIGINT)
    requested_gres_gpu: int | None = Field(default=None, sa_type=BIGINT)
    requested_gpu_type: str | None = None

    allocated_cpu: int | None = Field(default=None, sa_type=BIGINT)
    allocated_mem: int | None = Field(default=None, sa_type=BIGINT)
    allocated_node: int | None = Field(default=None, sa_type=BIGINT)
    allocated_billing: int | None = Field(default=None, sa_type=BIGINT)
    allocated_gres_gpu: int | None = Field(default=None, sa_type=BIGINT)
    allocated_gpu_type: str | None = None
    # Harmonized version or allocated_gpu_type. If not None, should exist in GpuRguDB.
    harmonized_gpu_type: str | None = Field(
        foreign_key="gpurgudb.name", default=None, ondelete="SET NULL"
    )

    statistics: dict[str, JobStatisticDB] = Relationship(
        sa_relationship=relationship(
            JobStatisticDB,
            collection_class=attribute_keyed_dict("name"),
            passive_deletes="all",
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


def get_rgus(rgu_version: str = "1.0") -> dict[str, float]:
    """
    Return GPU->RGU mapping for given RGU version.

    Get mapping from package IGUANE.
    """
    args = SimpleNamespace(fom_version=rgu_version, custom_weights=None, norm=False)
    gpus = sorted(RAWDATA.keys())
    return {gpu: fom_ugr(gpu, args=args) for gpu in gpus}
