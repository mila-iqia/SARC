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


# A job's run as a time range, for the /dash "was it running then?" queries.
#
# It exists as a function only to be IMMUTABLE. `timestamptz + interval` is
# merely STABLE -- an interval carrying days or months lands on a different
# instant depending on the session TimeZone (DST) -- and Postgres refuses a
# STABLE expression in an index. make_interval(secs => ...) yields seconds only,
# so the addition really is absolute and immutable.
#
# STRICT is necessary: tstzrange(NULL, NULL) is `(,)`, the *unbounded*
# range, which overlaps every period. A job that never started would
# then land in every bucket of every window. Returning NULL instead keeps them out,
# since NULL && anything is NULL.
#
# The `[)` bounds are the default, passed explicitly for better understanding;
# buckets tile the window bound to bound, so half-open is what
# puts each instant in exactly one of them, and it is what makes a zero-elapsed
# run the *empty* range -- which overlaps nothing -- rather than a point, which
# would overlap. `[]` would double-count every job sitting on a bucket edge.
#
# Registered with alembic-utils in alembic/env.py, like the job_series view.
# Careful when editing this body: alembic replaces the function in place and
# leaves ix_slurm_jobs_run holding whatever the old one computed.
# A change that does alter the result has to REINDEX in the same migration.
SLURM_JOB_RUN_SIGNATURE = (
    "slurm_job_run(job_start timestamptz, job_elapsed double precision)"
)
SLURM_JOB_RUN_DEFINITION = """
returns tstzrange
language sql
immutable
strict
parallel safe
as $$ select tstzrange(job_start, job_start + make_interval(secs => job_elapsed), '[)') $$
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
        # Partial covering index for the /dash GPU queries (count, page, rgu_by_*):
        # they read every column they need from the index, without opening the table
        # -- but only while autovacuum stays current, else Postgres opens the rows
        # anyway to check they are still live.
        Index(
            "ix_slurm_jobs_submit_gpu_type",
            "submit_time",
            "allocated_gpu_type",
            postgresql_include=[
                "id",
                "harmonized_gpu_type",
                "allocated_gres_gpu",
                "elapsed_time",
                "cluster_id",
                "cluster_user",
                "sarc_user_id",  # used by view when joining users and member_type
            ],
        ),
        # The /dash window filter asks which jobs were running during a period,
        # which is an overlap between two time ranges. A btree cannot answer it:
        # it orders one scalar, while the answer depends on start_time *and*
        # elapsed_time together -- two jobs starting the same second can end six
        # months apart. GiST indexes the range itself, each node bounding the
        # ranges below it, so a period that misses a node prunes its whole
        # subtree. Nothing here is tuned to how long jobs happen to run: the tree
        # learns that from the rows and keeps up on its own.
        Index(
            "ix_slurm_jobs_run",
            text("slurm_job_run(start_time, elapsed_time)"),
            postgresql_using="gist",
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
