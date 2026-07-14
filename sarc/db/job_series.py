from datetime import datetime

from sqlalchemy.dialects.postgresql import JSONB, aggregate_order_by
from sqlmodel import BIGINT, JSON, Field, and_, col, func, select

from sarc.models.user import MemberType

from .cluster import SlurmClusterDB
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
# requested_rgu/requested_rgu_drac and allocated_rgu/allocated_rgu_drac are
# per-job RGU-count metrics (GPU count x RGU weight, not time-integrated):
# requested_rgu/requested_rgu_drac from the requested GPU count,
# allocated_rgu/allocated_rgu_drac from the allocated GPU count. Both coalesce a
# missing GPU count to 0 (non-GPU jobs get RGU 0).
requested_gres_gpu = func.coalesce(SlurmJobDB.requested_gres_gpu, 0)
requested_rgu_expr = (requested_gres_gpu * GpuRguDB.rgu).label("requested_rgu")
requested_rgu_drac_expr = (requested_gres_gpu * GpuRguDB.drac_rgu).label(
    "requested_rgu_drac"
)

allocated_gres_gpu = func.coalesce(SlurmJobDB.allocated_gres_gpu, 0)
allocated_rgu_expr = (allocated_gres_gpu * GpuRguDB.rgu).label("allocated_rgu")
allocated_rgu_drac_expr = (allocated_gres_gpu * GpuRguDB.drac_rgu).label(
    "allocated_rgu_drac"
)

# Cost and waste. CPU costs are in CPU-seconds; GPU cost/waste/overbilling are
# in RGU-seconds, using the raw (not coalesced) requested/allocated GPU counts —
# requested_gpu_cost and requested_gpu_waste are count-based cost metrics, not
# billing cost. requested_gres_gpu for requested_gpu_cost, allocated_gres_gpu
# for allocated_gpu_cost and gpu_overbilling_cost — all NULL when the job's RGU
# is not computable, and also NULL (not 0) when the underlying GPU count itself
# is NULL (unlike requested_rgu/allocated_rgu above, which coalesce to 0).
cpu_utilization = (
    select(JobStatisticDB.mean)
    .where(JobStatisticDB.job_id == SlurmJobDB.id)
    .where(JobStatisticDB.name == "cpu_utilization")
    .scalar_subquery()
)
requested_cpu_cost = col(SlurmJobDB.elapsed_time) * col(SlurmJobDB.requested_cpu)
allocated_cpu_cost = col(SlurmJobDB.elapsed_time) * col(SlurmJobDB.allocated_cpu)
cpu_overbilling_cost = (
    SlurmJobDB.elapsed_time
    * (col(SlurmJobDB.allocated_cpu) - col(SlurmJobDB.requested_cpu))
).label("cpu_overbilling_cost")

gpu_sm_occupancy = (
    select(JobStatisticDB.mean)
    .where(JobStatisticDB.job_id == SlurmJobDB.id)
    .where(JobStatisticDB.name == "gpu_sm_occupancy")
    .scalar_subquery()
)
requested_gpu_cost = (
    col(SlurmJobDB.elapsed_time)
    * col(SlurmJobDB.requested_gres_gpu)
    * GpuRguDB.drac_rgu
)
allocated_gpu_cost = (
    col(SlurmJobDB.elapsed_time)
    * col(SlurmJobDB.allocated_gres_gpu)
    * GpuRguDB.drac_rgu
)
gpu_overbilling_cost = (
    SlurmJobDB.elapsed_time
    * (col(SlurmJobDB.allocated_gres_gpu) - col(SlurmJobDB.requested_gres_gpu))
    * GpuRguDB.drac_rgu
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
            col(GpuRguDB.drac_rgu).label("gpu_type_rgu_drac"),
            requested_rgu_expr,
            requested_rgu_drac_expr,
            allocated_rgu_expr,
            allocated_rgu_drac_expr,
            requested_cpu_cost.label("requested_cpu_cost"),
            ((1 - cpu_utilization) * requested_cpu_cost).label("requested_cpu_waste"),
            allocated_cpu_cost.label("allocated_cpu_cost"),
            ((1 - cpu_utilization) * allocated_cpu_cost).label("allocated_cpu_waste"),
            cpu_overbilling_cost,
            requested_gpu_cost.label("requested_gpu_cost"),
            ((1 - gpu_sm_occupancy) * requested_gpu_cost).label("requested_gpu_waste"),
            allocated_gpu_cost.label("allocated_gpu_cost"),
            ((1 - gpu_sm_occupancy) * allocated_gpu_cost).label("allocated_gpu_waste"),
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
        .join(GpuRguDB, GpuRguDB.name == SlurmJobDB.harmonized_gpu_type, isouter=True)
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
    requested_cpu: int | None = Field(default=None, sa_type=BIGINT)
    requested_mem: int | None = Field(default=None, sa_type=BIGINT)
    requested_node: int | None = Field(default=None, sa_type=BIGINT)
    requested_billing: int | None = Field(default=None, sa_type=BIGINT)
    requested_gres_gpu: int | None = Field(default=None, sa_type=BIGINT)
    requested_gpu_type: str | None

    allocated_cpu: int | None = Field(default=None, sa_type=BIGINT)
    allocated_mem: int | None = Field(default=None, sa_type=BIGINT)
    allocated_node: int | None = Field(default=None, sa_type=BIGINT)
    allocated_billing: int | None = Field(default=None, sa_type=BIGINT)
    allocated_gres_gpu: int | None = Field(default=None, sa_type=BIGINT)
    allocated_gpu_type: str | None
    harmonized_gpu_type: str | None

    cluster_name: str | None = None
    statistics: dict[str, dict[str, float]] | None = Field(sa_type=JSON)

    gpu_type_rgu: float | None
    gpu_type_rgu_drac: float | None
    requested_rgu: float | None
    requested_rgu_drac: float | None
    allocated_rgu: float | None  # RGU computed using gres_gpu
    allocated_rgu_drac: float | None  # RGU computed using gres_gpu and DRAC RGU values

    requested_cpu_cost: float | None
    requested_cpu_waste: float | None
    allocated_cpu_cost: float | None
    allocated_cpu_waste: float | None
    cpu_overbilling_cost: float | None
    requested_gpu_cost: float | None
    requested_gpu_waste: float | None
    allocated_gpu_cost: float | None
    allocated_gpu_waste: float | None
    gpu_overbilling_cost: float | None

    # User ID
    sarc_user_id: int
    display_name: str
    email: str
    member_type: MemberType | None = None
    supervisors: list[int] | None = Field(sa_type=JSON)
