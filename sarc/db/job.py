import math
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from enum import Enum
from types import SimpleNamespace
from typing import Self

from iguane.fom import RAWDATA, fom_ugr
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import attribute_keyed_dict, relationship
from sqlmodel import (
    CheckConstraint,
    Field,
    Session,
    UniqueConstraint,
    col,
    func,
    or_,
    select,
)
from sqlmodel.main import Relationship
from sqlmodel.sql.expression import SelectOfScalar

from sarc.config import ClusterConfig
from sarc.db.cluster import SlurmClusterDB

from .sqlmodel import SQLModel
from .users import UserDB


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
    user_id: int = Field(foreign_key="users.id", ondelete="RESTRICT")
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

    @property
    def gpu_type_rgu(self) -> float:
        """Get RGU value for the GPU type of this job, or NaN if not applicable."""
        gpu_type = self.allocated_gpu_type
        if gpu_type is None:
            return math.nan
        else:
            gpu_to_rgu = get_rgus()
            # NB: If GPU type is a MIG
            # (e.g: "A100-SXM4-40GB : a100_1g.5gb"),
            # we currently return RGU for the main GPU type
            # (in this example: "A100-SXM4-40GB")
            return gpu_to_rgu.get(gpu_type.split(":")[0].rstrip(), math.nan)

    @property
    def rgu(self) -> float:
        """
        Get RGU billing for this job, or NaN if not applicable.
        Same algorithm as in series functions
        load_job_series() and update_job_series_rgu().

        RGU billing for a job is equivalent to:
        Number of GPUs used by this job
        x
        RGU value for a single GPU (self.gpu_type_rgu)
        """

        end_time = self.end_time
        if end_time is None:
            end_time = datetime.now(tz=UTC)
        start_time = end_time - timedelta(seconds=self.elapsed_time)
        gpu_type = self.allocated_gpu_type
        if start_time is None or gpu_type is None:
            return math.nan

        billing = self.allocated_billing or 0
        gres_gpu = self.requested_gres_gpu or 0
        if gres_gpu:
            gres_gpu = max(billing, gres_gpu)

        gpu_type_rgu = self.gpu_type_rgu
        if self.cluster.billing_is_gpu:
            # Compute RGU from gpu count
            gpu_count = gres_gpu
            gres_rgu = gpu_count * gpu_type_rgu
        else:
            # Job billing is in its own unit.
            # We must first infer gpu count
            # before computing RGU
            cluster_billing = self.cluster.get_gpu_billing(start_time)
            if cluster_billing is None:
                # Before the oldest gpu->billing mapping available
                # We assume gres_gpu is gpu count
                gpu_count = gres_gpu
                gres_rgu = gpu_count * gpu_type_rgu
            else:
                # Then find billing for this job GPU type
                gpu_billing = cluster_billing.gpu_to_billing.get(gpu_type, math.nan)
                # gres_gpu is job billing
                job_billing = gres_gpu
                # So, gpu count == job billing / gpu billing
                gres_rgu = (job_billing / gpu_billing) * gpu_type_rgu
        return gres_rgu


def get_rgus(rgu_version: str = "1.0") -> dict[str, float]:
    """
    Return GPU->RGU mapping for given RGU version.

    Get mapping from package IGUANE.
    """
    args = SimpleNamespace(fom_version=rgu_version, custom_weights=None, norm=False)
    gpus = sorted(RAWDATA.keys())
    return {gpu: fom_ugr(gpu, args=args) for gpu in gpus}


def _compute_jobs_query(
    query: SelectOfScalar,
    sess: Session,
    *,
    cluster: str | ClusterConfig | None = None,
    job_id: int | list[int] | None = None,
    job_state: str | SlurmState | None = None,
    user: int | UserDB | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
) -> SelectOfScalar:
    """Compute the MongoDB query dict to be used to match given arguments.

    Arguments:
        cluster: The cluster on which to search for jobs.
        job_id: The id or a list of ids to select.
        job_state: Job state to filter on.
        user: User to filter on.
        start: Get all jobs that have a status after that time.
            If str, parsed as a day (YYYY-MM-DD) at 00:00 local timezone.
        end: Get all jobs that have a status before that time.
            If str, parsed as a day (YYYY-MM-DD) at 00:00 local timezone.
    """
    if isinstance(cluster, ClusterConfig):
        cluster_name = cluster.name
    else:
        cluster_name = cluster

    if isinstance(start, str):
        start = datetime.strptime(start, "%Y-%m-%d").astimezone()
    if isinstance(end, str):
        end = datetime.strptime(end, "%Y-%m-%d").astimezone()

    if start is not None:
        start = start.astimezone(UTC)
    if end is not None:
        end = end.astimezone(UTC)

    if isinstance(user, UserDB):
        user = user.id

    if cluster_name:
        query = query.where(
            SlurmJobDB.cluster_id == SlurmClusterDB.id_by_name(sess, cluster_name)
        )

    if isinstance(job_id, int):
        query = query.where(SlurmJobDB.job_id == job_id)
    elif isinstance(job_id, list):
        query = query.where(col(SlurmJobDB.job_id).in_(job_id))

    if user is not None:
        query = query.where(SlurmJobDB.user_id == user)

    if job_state:
        query = query.where(SlurmJobDB.job_state == job_state)

    if start:
        # Select jobs that had a status after the given time. This is a bit special
        # since we need to get both jobs that did not finish, and any job that ended after
        # the given time.
        query = query.where(
            or_(SlurmJobDB.end_time == None, col(SlurmJobDB.end_time) > start)  # noqa: E711
        )
    if end:
        # Select any job that had a status before the given end time.
        query = query.where(SlurmJobDB.submit_time < end)

    return query


def count_jobs(
    sess: Session,
    *,
    cluster: str | ClusterConfig | None = None,
    job_id: int | list[int] | None = None,
    job_state: str | SlurmState | None = None,
    user: int | UserDB | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
) -> int:
    """Count jobs that match the query.

    Arguments:
        cluster: The cluster on which to search for jobs.
        job_id: The id or a list of ids to select.
        job_state: Job state to filter on.
        user: User to filter on.
        start: Get all jobs that have a status after that time.
            If str, parsed as a day (YYYY-MM-DD) at 00:00 local timezone.
        end: Get all jobs that have a status before that time.
            If str, parsed as a day (YYYY-MM-DD) at 00:00 local timezone.
        query_options: Additional options to pass to MongoDB (limit, etc.)
    """

    query = _compute_jobs_query(
        select(func.count(col(SlurmJobDB.id))),
        sess,
        cluster=cluster,
        job_id=job_id,
        job_state=job_state,
        user=user,
        start=start,
        end=end,
    )
    return sess.exec(query).one()


def get_jobs(
    sess: Session,
    *,
    cluster: str | ClusterConfig | None = None,
    job_id: int | list[int] | None = None,
    job_state: str | SlurmState | None = None,
    user: int | UserDB | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
) -> Iterable[SlurmJobDB]:
    """Get jobs that match the query.

    Arguments:
        cluster: The cluster on which to search for jobs.
        job_id: The id or a list of ids to select.
        job_state: Job state to filter on.
        user: User to filter on.
        start: Get all jobs that have a status after that time.
            If str, parsed as a day (YYYY-MM-DD) at 00:00 local timezone.
        end: Get all jobs that have a status before that time.
            If str, parsed as a day (YYYY-MM-DD) at 00:00 local timezone.
        query_options: Additional options to pass to MongoDB (limit, etc.)
    """

    query = _compute_jobs_query(
        select(SlurmJobDB),
        sess,
        cluster=cluster,
        job_id=job_id,
        job_state=job_state,
        user=user,
        start=start,
        end=end,
    )

    return sess.exec(query)


def get_job(sess: Session, **kwargs) -> SlurmJobDB | None:
    """Get a single job that matches the query, or None if nothing is found.

    Same signature as `get_jobs`.
    """
    # Sort by submit_time descending, which ensures we get the most recent version
    # of the job.
    query = (
        _compute_jobs_query(select(SlurmJobDB), sess, **kwargs)
        .order_by(col(SlurmJobDB.submit_time).desc())
        .limit(1)
    )
    return sess.exec(query).one_or_none()
