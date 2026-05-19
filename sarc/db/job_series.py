from datetime import datetime

from sqlalchemy import true
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
            aggregate_order_by(
                col(SupervisorsHelper.supervisor), col(SupervisorsHelper.pos)
            )
        )
    )
    .select_from(SupervisorsDB)
    .join(SupervisorsHelper, col(SupervisorsDB.id) == col(SupervisorsHelper.list_id))
    .where(
        SupervisorsDB.user_id == SlurmJobDB.sarc_user_id,
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
cluster_billing_count = (
    select(func.count(col(GPUBillingDB.id)))
    .where(GPUBillingDB.cluster_id == SlurmClusterDB.id)
    .scalar_subquery()
)
rgu_expr = case(
    # A: billing_is_gpu -> multiply.
    (col(SlurmClusterDB.billing_is_gpu) == True, gpu_count_raw * GpuRguDB.rgu),  # noqa: E712
    # B: pre-billing era (cluster has billings but none applicable yet) -> multiply.
    (
        and_(col(billing_subq.c.gpu_to_billing).is_(None), cluster_billing_count > 0),
        gpu_count_raw * GpuRguDB.rgu,
    ),
    # C: scale by per-type billing. NULL division yields NULL (NaN) when
    # gpu_unit_billing is missing (cluster has no billing record at all, or
    # gpu_type missing from the mapping).
    else_=(gpu_count_raw / gpu_unit_billing) * GpuRguDB.rgu,
).label("rgu")

# Cost and waste
cpu_cost = col(SlurmJobDB.elapsed_time) * col(SlurmJobDB.requested_cpu)
cpu_utilization = (
    select(JobStatisticDB.mean)
    .where(JobStatisticDB.job_id == SlurmJobDB.id)
    .where(JobStatisticDB.name == "cpu_utilization")
    .scalar_subquery()
)
cpu_equivalent_cost = col(SlurmJobDB.elapsed_time) * col(SlurmJobDB.allocated_cpu)
cpu_overbilling_cost = (
    SlurmJobDB.elapsed_time
    * (col(SlurmJobDB.allocated_cpu) - col(SlurmJobDB.requested_cpu))
).label("cpu_overbilling_cost")

gpu_cost = col(SlurmJobDB.elapsed_time) * col(SlurmJobDB.requested_gres_gpu)
gpu_utilization = (
    select(JobStatisticDB.mean)
    .where(JobStatisticDB.job_id == SlurmJobDB.id)
    .where(JobStatisticDB.name == "gpu_utilization")
    .scalar_subquery()
)
gpu_equivalent_cost = col(SlurmJobDB.elapsed_time) * col(SlurmJobDB.allocated_gres_gpu)
gpu_overbilling_cost = (
    SlurmJobDB.elapsed_time
    * (col(SlurmJobDB.allocated_gres_gpu) - col(SlurmJobDB.requested_gres_gpu))
).label("gpu_overbilling_cost")

JOB_SERIES_EXCLUDED_JOB_COLS = frozenset(
    {"id", "sarc_user_id", "latest_scraped_start", "latest_scraped_end"}
)


class JobSeriesDB(SQLModel, table=True):
    __tablename__ = "job_series_view"  # This is filtered out in table creation
    __sql_view__ = (
        select(
            col(SlurmJobDB.id).label("job_db_id"),
            col(UserDB.id).label("sarc_user_id"),
            *[
                c
                for c in SlurmJobDB.__table__.columns  # ty:ignore[unresolved-attribute]
                if c.name not in JOB_SERIES_EXCLUDED_JOB_COLS
            ],
            *[c for c in UserDB.__table__.columns if c.name != "id"],  # ty:ignore[unresolved-attribute]
            col(SlurmClusterDB.name).label("cluster_name"),
            col(MemberTypeDB.member_type),
            stats_subq,
            supervisors_subq,
            col(GpuRguDB.rgu).label("gpu_type_rgu"),
            rgu_expr,
            cpu_cost.label("cpu_cost"),
            ((1 - cpu_utilization) * cpu_cost).label("cpu_waste"),
            cpu_equivalent_cost.label("cpu_equivalent_cost"),
            ((1 - cpu_utilization) * cpu_equivalent_cost).label("cpu_equivalent_waste"),
            cpu_overbilling_cost,
            gpu_cost.label("gpu_cost"),
            ((1 - gpu_utilization) * gpu_cost).label("gpu_waste"),
            gpu_equivalent_cost.label("gpu_equivalent_cost"),
            ((1 - gpu_utilization) * gpu_equivalent_cost).label("gpu_equivalent_waste"),
            gpu_overbilling_cost,
        )  # ty:ignore[no-matching-overload]
        .join(UserDB, SlurmJobDB.sarc_user_id == UserDB.id)
        .join(SlurmClusterDB, SlurmJobDB.cluster_id == SlurmClusterDB.id)
        .join(
            MemberTypeDB,
            and_(
                MemberTypeDB.user_id == SlurmJobDB.sarc_user_id,
                MemberTypeDB.valid.contains(SlurmJobDB.submit_time),
            ),
            isouter=True,
        )
        .join(GpuRguDB, GpuRguDB.name == SlurmJobDB.allocated_gpu_type, isouter=True)
        .outerjoin(billing_subq, true())
    )
    job_db_id: int = Field(primary_key=True)
    # job identification
    cluster_id: int
    account: str
    job_id: int
    array_job_id: int | None
    task_id: int | None
    name: str
    cluster_user: str
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

    cluster_name: str | None = None
    statistics: dict[str, dict[str, float]] | None = Field(sa_type=JSON)

    gpu_type_rgu: float | None
    rgu: float | None

    cpu_cost: float | None
    cpu_waste: float | None
    cpu_equivalent_cost: float | None
    cpu_equivalent_waste: float | None
    cpu_overbilling_cost: float | None
    gpu_cost: float | None
    gpu_waste: float | None
    gpu_equivalent_cost: float | None
    gpu_equivalent_waste: float | None
    gpu_overbilling_cost: float | None

    # User ID
    sarc_user_id: int
    display_name: str
    email: str
    member_type: MemberType | None = None
    supervisors: list[int] | None = Field(sa_type=JSON)
