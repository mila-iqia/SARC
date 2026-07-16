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
# is NULL (unlike requested_rgu/allocated_rgu above, which coalesce to 0). Mean
# of the per-job "cpu_utilization" statistic (fraction in [0, 1] of the
# allocated CPU capacity that was actually used); used below to derive CPU
# waste.
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

# Mean of the per-job "gpu_sm_occupancy" statistic. Two distinct GPU stats are
# scraped from DCGM/Prometheus, both normalized to [0, 1]:
# - gpu_utilization (slurm_job_utilization_gpu): the GPU "busy" fraction, i.e.
#   the fraction of time the GPU was active on any work.
# - gpu_sm_occupancy (slurm_job_sm_occupancy_gpu): the fraction of streaming
#   multiprocessors (SMs) occupied, a finer-grained measure of how much of the
#   GPU's compute is actually used.
# GPU waste below is computed from SM-occupancy mean (not GPU utilization).
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
    """Slurm accounting account the job was charged to (e.g. "rrg-..."); an
    allocation/billing account, not a person."""
    job_id: int
    """Individual Slurm job id (unique per array task)."""
    array_job_id: int | None
    """Shared parent id of the job array; None for non-array jobs."""
    task_id: int | None
    """Task index within the job array; None for non-array jobs. (DB uniqueness
    is on (cluster_id, job_id, submit_time), not on these array fields.)"""
    name: str
    cluster_user: str
    """Cluster login username; resolves to the SARC user in sarc_user_id."""
    group: str
    """Unix group of the submitting user."""

    # status
    job_state: SlurmState
    """Slurm job-state code (see the SlurmState enum), e.g. COMPLETED, FAILED,
    TIMEOUT, CANCELLED."""
    exit_code: int | None
    """Process return code of the job."""
    signal: int | None
    """Number of the signal that terminated the job, if any."""

    # allocation information
    partition: str
    nodes: list[str] = Field(sa_type=JSONB)
    """Expanded list of node hostnames the job ran on; empty when none assigned."""

    work_dir: str
    submit_line: str | None
    """The command line used to submit the job. Added later, so old records may
    lack it."""

    # Miscellaneous
    constraints: str | None
    """Job constraint/feature expression requested at submit time."""
    priority: int | None
    """Dimensionless Slurm scheduling priority value."""
    qos: str | None
    """Quality-of-Service (QoS) name."""

    # Flags
    # Slurm's own job flags (booleans, default False). Names come straight from
    # Slurm; no in-repo source elaborates beyond the name.
    CLEAR_SCHEDULING: bool
    """Slurm flag: the job's scheduling information was cleared."""
    STARTED_ON_SUBMIT: bool
    """Slurm flag: the job started immediately on submission."""
    STARTED_ON_SCHEDULE: bool
    """Slurm flag: the job started via the main scheduler."""
    STARTED_ON_BACKFILL: bool
    """Slurm flag: the job started via the backfill scheduler."""

    # temporal fields
    time_limit: int | None
    """Wall-clock time limit in SECONDS (sacct reports minutes; multiplied by 60
    on ingest). None if unset."""
    submit_time: datetime
    start_time: datetime | None
    end_time: datetime | None
    elapsed_time: float
    """Elapsed wall-clock time in SECONDS. Used as the time factor in all
    cost/waste columns below."""

    # tres
    # TRES columns hold the raw Slurm TRES count for each resource, copied
    # verbatim with no unit conversion on ingest.
    requested_cpu: int | None = Field(default=None, sa_type=BIGINT)
    """Requested CPU core COUNT (not core-seconds)."""
    requested_mem: int | None = Field(default=None, sa_type=BIGINT)
    """Requested memory as the raw Slurm `mem` TRES count. Slurm reports MB by
    convention (not asserted in-repo)."""
    requested_node: int | None = Field(default=None, sa_type=BIGINT)
    """Requested node count."""
    requested_billing: int | None = Field(default=None, sa_type=BIGINT)
    """Requested Slurm `billing` TRES: the scheduler's weighted-usage number
    derived from TRESBillingWeights. Dimensionless -- not currency, not GPU
    count."""
    requested_gres_gpu: int | None = Field(default=None, sa_type=BIGINT)
    """Requested GPU COUNT."""
    requested_gpu_type: str | None
    """Raw GPU model string from the requested TRES name (before harmonization)."""

    allocated_cpu: int | None = Field(default=None, sa_type=BIGINT)
    """Allocated CPU core COUNT (not core-seconds)."""
    allocated_mem: int | None = Field(default=None, sa_type=BIGINT)
    """Allocated memory as the raw Slurm `mem` TRES count. Slurm reports MB by
    convention (not asserted in-repo)."""
    allocated_node: int | None = Field(default=None, sa_type=BIGINT)
    """Allocated node count."""
    allocated_billing: int | None = Field(default=None, sa_type=BIGINT)
    """Allocated Slurm `billing` TRES: the scheduler's weighted-usage number
    derived from TRESBillingWeights. Dimensionless -- not currency, not GPU
    count."""
    allocated_gres_gpu: int | None = Field(default=None, sa_type=BIGINT)
    """Allocated GPU COUNT."""
    allocated_gpu_type: str | None
    """Raw GPU model string from the allocated TRES name (before harmonization);
    may be inferred from the node->GPU mapping."""
    harmonized_gpu_type: str | None
    """Canonicalized GPU name derived from allocated_gpu_type via
    Cluster.harmonize_gpu; the join key to the RGU weights in GpuRguDB (handles
    MIG partitions specially). Distinct from the raw requested_gpu_type /
    allocated_gpu_type above."""

    cluster_name: str | None = None
    statistics: dict[str, dict[str, float]] | None = Field(sa_type=JSON)
    """Per-job statistics as a JSON map: stat_name -> {mean, std, q05, q25,
    median, q75, max, unused} (e.g. "cpu_utilization", "gpu_sm_occupancy")."""

    # RGU (Reference GPU Unit) is a per-GPU-type weight that normalizes
    # heterogeneous GPU types to a common reference.
    gpu_type_rgu: float | None
    """RGU weight for this job's harmonized GPU type (mila/default weight).
    Equal to gpu_type_rgu_drac except for MIG partitions."""
    gpu_type_rgu_drac: float | None
    """DRAC reference RGU weight for this job's harmonized GPU type. Equal to
    gpu_type_rgu except for MIG partitions."""
    requested_rgu: float | None
    """RGU demand = requested GPU count x RGU weight (NOT a raw GPU count); a
    missing GPU count is coalesced to 0."""
    requested_rgu_drac: float | None
    """As requested_rgu but using the DRAC RGU weight."""
    allocated_rgu: float | None
    """RGU demand = allocated GPU count x RGU weight (NOT a raw GPU count); a
    missing GPU count is coalesced to 0."""
    allocated_rgu_drac: float | None
    """As allocated_rgu but using the DRAC RGU weight."""

    # Cost / waste / overbilling. requested_* uses what the user asked for,
    # allocated_* what the scheduler actually gave. Unlike the *_rgu columns
    # above, these use the raw (non-coalesced) GPU count, so they are NULL (not
    # 0) when the count/RGU is not computable.
    #
    # CPU columns are in CPU-SECONDS; GPU columns in RGU-SECONDS (DRAC weight).
    # For each: cost = elapsed_time x count (x rgu weight for GPU);
    # overbilling = elapsed_time x (allocated - requested) (x rgu weight).
    # Waste = (1 - utilization) x cost = the paid-for capacity left unused, and
    # the utilization term differs by resource: CPU uses the cpu_utilization
    # stat mean, while GPU uses the gpu_sm_occupancy mean -- the fraction of
    # streaming multiprocessors (SMs) occupied (finer-grained) -- NOT
    # gpu_utilization, which is only the GPU "busy"/active-time fraction.
    requested_cpu_cost: float | None
    """CPU-seconds the user requested: elapsed_time x requested_cpu."""
    requested_cpu_waste: float | None
    """Unused requested CPU-seconds: (1 - cpu_utilization mean) x
    requested_cpu_cost."""
    allocated_cpu_cost: float | None
    """CPU-seconds the scheduler allocated: elapsed_time x allocated_cpu."""
    allocated_cpu_waste: float | None
    """Unused allocated CPU-seconds: (1 - cpu_utilization mean) x
    allocated_cpu_cost."""
    cpu_overbilling_cost: float | None
    """CPU-seconds billed beyond the request: elapsed_time x (allocated_cpu -
    requested_cpu)."""
    requested_gpu_cost: float | None
    """RGU-seconds the user requested: elapsed_time x requested_gres_gpu x DRAC
    RGU weight."""
    requested_gpu_waste: float | None
    """Unused requested RGU-seconds: (1 - gpu_sm_occupancy mean) x
    requested_gpu_cost."""
    allocated_gpu_cost: float | None
    """RGU-seconds the scheduler allocated: elapsed_time x allocated_gres_gpu x
    DRAC RGU weight."""
    allocated_gpu_waste: float | None
    """Unused allocated RGU-seconds: (1 - gpu_sm_occupancy mean) x
    allocated_gpu_cost."""
    gpu_overbilling_cost: float | None
    """RGU-seconds billed beyond the request: elapsed_time x (allocated_gres_gpu
    - requested_gres_gpu) x DRAC RGU weight."""

    # User ID
    sarc_user_id: int
    display_name: str
    email: str
    member_type: MemberType | None = None
    """The user's member type valid at the job's submit time."""
    supervisors: list[int] | None = Field(sa_type=JSON)
    """Supervisor user ids, ordered, valid at the job's submit time."""
