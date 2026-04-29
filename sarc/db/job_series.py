from datetime import datetime

from sqlalchemy.dialects.postgresql import JSONB, aggregate_order_by
from sqlmodel import FLOAT, JSON, Field, and_, case, col, desc, func, select

from sarc.models.user import MemberType

from .cluster import GPUBillingDB, SlurmClusterDB
from .job import JobStatisticDB, SlurmJobDB, SlurmState
from .sqlmodel import SQLModel
from .support import GpuRguDB
from .users import MemberTypeDB, SupervisorsDB, SupervisorsHelper, UserDB

#### statistics
inner_dict = func.json_build_object(
    "mean",
    JobStatisticDB.mean,
    "std",
    JobStatisticDB.std,
    "q05",
    JobStatisticDB.q05,
    "q25",
    JobStatisticDB.q25,
    "median",
    JobStatisticDB.median,
    "q75",
    JobStatisticDB.q75,
    "max",
    JobStatisticDB.max,
    "unused",
    JobStatisticDB.unused,
)

stats_subq = (
    select(func.json_object_agg(JobStatisticDB.name, inner_dict))
    .where(JobStatisticDB.job_id == SlurmJobDB.id)
    .scalar_subquery()
).label("statistics")

#### supervisors
supervisors_subq = (
    select(
        func.json_agg(
            aggregate_order_by(SupervisorsHelper.supervisor, SupervisorsHelper.pos)
        )
    )
    .select_from(SupervisorsDB)
    .join(SupervisorsHelper, SupervisorsDB.id == SupervisorsHelper.list_id)
    .where(
        SupervisorsDB.user_id == SlurmJobDB.user_id,
        SupervisorsDB.valid.contains(SlurmJobDB.submit_time),
    )
    .scalar_subquery()
).label("supervisors")

#### RGU
base_gres_gpu = func.coalesce(SlurmJobDB.requested_gres_gpu, 0)
base_billing = func.coalesce(SlurmJobDB.allocated_billing, 0)
gpu_count_raw = case(
    (base_gres_gpu > 0, func.greatest(base_billing, base_gres_gpu)), else_=base_gres_gpu
)
billing_subq = (
    select(GPUBillingDB.gpu_to_billing)
    .where(GPUBillingDB.cluster_id == SlurmJobDB.cluster_id)
    .where(GPUBillingDB.since <= SlurmJobDB.submit_time)
    .order_by(desc(GPUBillingDB.since))
    .limit(1)
    .correlate(SlurmJobDB)
    .lateral()
)
gpu_unit_billing = func.cast(
    billing_subq.c.gpu_to_billing.op("->>")(SlurmJobDB.allocated_gpu_type), FLOAT
)
rgu_expr = case(
    # Case A: Billing IS GPU (Simple multiply)
    (col(SlurmClusterDB.billing_is_gpu) == True, gpu_count_raw * GpuRguDB.rgu),  # noqa: E712
    # Case B: No billing record found (Fallback)
    (gpu_unit_billing == None, gpu_count_raw * GpuRguDB.rgu),  # noqa: E711
    # Case C: Unit billing exists (job_billing / gpu_billing) * type_rgu
    else_=(gpu_count_raw / gpu_unit_billing) * GpuRguDB.rgu,
).label("rgu_per_time")


class JobSeries(SQLModel, table=True):
    __tablename__ = "job_series_view"  # This is filtered out in table creation
    __sql_view__ = (
        select(
            SlurmJobDB.id.label("job_db_id"),
            UserDB.id.label("user_db_id"),
            *[
                c
                for c in SlurmJobDB.__table__.columns
                if c.name in ("id", "latest_scraped_start", "latest_scraped_end")
            ],
            *[c for c in UserDB.__table__.columns if c.name != "id"],
            SlurmClusterDB.name.label("cluster_name"),
            MemberTypeDB.member_type,
            stats_subq,
            supervisors_subq,
            GpuRguDB.rgu.label("gpu_type_rgu"),
            rgu_expr,
        )
        .join(UserDB, SlurmJobDB.user_id == UserDB.id)
        .join(SlurmClusterDB, SlurmJobDB.cluster_id == SlurmClusterDB.id)
        .join(
            MemberTypeDB,
            and_(
                MemberTypeDB.user_id == SlurmJobDB.user_id,
                MemberTypeDB.valid.contains(SlurmJobDB.submit_time),
            ),
            isouter=True,
        )
        .join(GpuRguDB, GpuRguDB.name == SlurmJobDB.allocated_gpu_type, isouter=True)
    )
    job_db_id: int = Field(primary_key=True)
    # job identification
    cluster_id: int
    account: str
    job_id: int
    array_job_id: int | None
    task_id: int | None
    name: str
    user: str
    group: str

    # status
    job_state: SlurmState
    exit_code: int | None
    signal: int | None

    # allocation information
    partition: str
    nodes: list[str] = Field(sa_type=JSONB)

    work_dir: str
    submit_line: str | None

    # Miscellaneous
    constraints: str | None
    priority: int | None
    qos: str | None

    # Flags
    CLEAR_SCHEDULING: bool
    STARTED_ON_SUBMIT: bool
    STARTED_ON_SCHEDULE: bool
    STARTED_ON_BACKFILL: bool

    # temporal fields
    time_limit: int | None
    submit_time: datetime
    start_time: datetime | None
    end_time: datetime | None
    elapsed_time: float

    # tres
    requested_cpu: int | None
    requested_mem: int | None
    requested_node: int | None
    requested_billing: int | None
    requested_gres_gpu: int | None
    requested_gpu_type: str | None

    allocated_cpu: int | None
    allocated_mem: int | None
    allocated_node: int | None
    allocated_billing: int | None
    allocated_gres_gpu: int | None
    allocated_gpu_type: str | None

    statistics: dict[str, dict[str, float]] | None = Field(sa_type=JSON)

    gpu_type_rgu: float | None
    rgu: float | None

    # User ID
    user_db_id: int
    display_name: str
    email: str
    member_type: MemberType
    supervisors: list[int] | None = Field(sa_type=JSON)
