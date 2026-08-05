from datetime import datetime

from sqlalchemy.dialects.postgresql import JSONB, aggregate_order_by
from sqlalchemy.orm import aliased
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import ColumnClause
from sqlalchemy.sql.expression import FromClause, Join, Select
from sqlmodel import BIGINT, JSON, Field, and_, col, func, select

from sarc.models.user import MemberType

from .cluster import SlurmClusterDB
from .job import JobStatisticDB, SlurmJobDB, SlurmState
from .sqlmodel import SQLModel
from .support import GpuRguDB
from .users import MemberTypeDB, SupervisorsDB, SupervisorsHelper, UserDB

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

#### member_type
# Correlated subquery, not a join: pruned when member_type is not selected (the
# dashboard never reads it; /v0/job/series caps pages at 100 rows so the per-row
# GiST lookup on membertypedb stays cheap). A LEFT join would be non-removable
# (the `valid @> submit_time` range predicate) and cost ~288ms on every wide query.
member_type_subq = (
    select(MemberTypeDB.member_type)
    .where(
        MemberTypeDB.user_id == SlurmJobDB.sarc_user_id,
        MemberTypeDB.valid.contains(SlurmJobDB.submit_time),
    )
    .scalar_subquery()
).label("member_type")

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
cpu_jsdb = aliased(JobStatisticDB)
cpu_utilization = col(cpu_jsdb.mean).label("cpu_utilization")
requested_cpu_cost = col(SlurmJobDB.elapsed_time) * col(SlurmJobDB.requested_cpu)
allocated_cpu_cost = col(SlurmJobDB.elapsed_time) * col(SlurmJobDB.allocated_cpu)
cpu_overbilling_cost = (
    SlurmJobDB.elapsed_time
    * (col(SlurmJobDB.allocated_cpu) - col(SlurmJobDB.requested_cpu))
).label("cpu_overbilling_cost")

sm_occ_jsdb = aliased(JobStatisticDB)
gpu_sm_occupancy = col(sm_occ_jsdb.mean).label("gpu_sm_occupancy_mean")
gpu_sm_occupancy_max = col(sm_occ_jsdb.max).label("gpu_sm_occupancy_max")

gpu_util_jsdb = aliased(JobStatisticDB)
gpu_utilization = col(gpu_util_jsdb.mean).label("gpu_utilization_mean")

gpu_memory_jsdb = aliased(JobStatisticDB)
gpu_memory_max = col(gpu_memory_jsdb.max).label("gpu_memory_max")

usage_metric = gpu_sm_occupancy.label("usage_metric")

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
    """Wide read-model view (slurm_jobs + users, clusters, gpurgudb, per-job stats,
    and per-row subqueries). Performance gotchas when querying it at /dash window sizes:

    - slurm_jobs stays index-only only while every column the query needs is in the
      covering index -- notably the join keys sarc_user_id (users LEFT join,
      member_type/supervisors subqueries) and cluster_id (clusters LEFT join). Both
      the users and clusters joins are LEFT so the planner drops them when their
      columns are unused (FK + NOT NULL make LEFT == INNER, so no row is lost).
      Even a dropped join pins its key columns to the scan, though; when the target
      index lacks them, build the query with ``job_series_select()`` below instead.
    - member_type and supervisors are per-row correlated subqueries:
      pruned when not selected, but evaluated once per output row otherwise -- cheap
      only on bounded/paginated selects, not over a wide unbounded window.
    - ``usage_metric``, the per-stat columns (``gpu_sm_occupancy_mean``/``_max``,
      ``gpu_utilization_mean``, ``gpu_memory_max``) and the ``*_gpu_waste`` /
      ``*_cpu_waste`` derived from them read jobstatisticdb through one set-based LEFT
      join per stat name: index-only (unique covering index on (name, job_id)),
      parallel, and each join is dropped when its stat is unused. usage_metric, both
      sm-occupancy columns and ``*_gpu_waste`` all ride on the same join, so they cost
      the same as any one of them; gpu_utilization_mean and gpu_memory_max each add
      one. A stat with no recorded row reads NULL, and so does the waste derived from
      it: a missing measurement, not zero waste.
    """

    __tablename__ = "job_series_view"  # This is filtered out in table creation
    __sql_view__ = (
        select(
            col(SlurmJobDB.id).label("job_db_id"),
            # sarc_user_id from slurm_jobs (not UserDB.id) so the users join below
            # can be LEFT and dropped by the planner when display_name/email are
            # unused. sarc_user_id is NOT NULL + FK to users, so LEFT == INNER here.
            col(SlurmJobDB.sarc_user_id).label("sarc_user_id"),
            *[
                c
                for c in SlurmJobDB.__table__.columns  # ty:ignore[unresolved-attribute]
                if c.name not in JOB_SERIES_EXCLUDED_JOB_COLS
            ],
            *[c for c in UserDB.__table__.columns if c.name != "id"],  # ty:ignore[unresolved-attribute]
            col(SlurmClusterDB.name).label("cluster_name"),
            member_type_subq,
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
            ((1 - usage_metric) * requested_gpu_cost).label("requested_gpu_waste"),
            allocated_gpu_cost.label("allocated_gpu_cost"),
            ((1 - usage_metric) * allocated_gpu_cost).label("allocated_gpu_waste"),
            gpu_overbilling_cost,
            usage_metric,
            gpu_sm_occupancy,
            gpu_sm_occupancy_max,
            gpu_utilization,
            gpu_memory_max,
        )  # ty:ignore[no-matching-overload]
        .join(UserDB, SlurmJobDB.sarc_user_id == UserDB.id, isouter=True)
        # LEFT so the planner drops it when cluster_name is unused (same as the
        # users join above). cluster_id is NOT NULL + FK to clusters.id (a unique
        # PK) with ondelete RESTRICT, so no row is ever dropped -- LEFT == INNER.
        .join(SlurmClusterDB, SlurmJobDB.cluster_id == SlurmClusterDB.id, isouter=True)
        .join(GpuRguDB, GpuRguDB.name == SlurmJobDB.harmonized_gpu_type, isouter=True)
        .join(
            sm_occ_jsdb,
            and_(
                sm_occ_jsdb.job_id == SlurmJobDB.id,
                sm_occ_jsdb.name == "gpu_sm_occupancy",
            ),
            isouter=True,
        )
        .join(
            cpu_jsdb,
            and_(cpu_jsdb.job_id == SlurmJobDB.id, cpu_jsdb.name == "cpu_utilization"),
            isouter=True,
        )
        .join(
            gpu_util_jsdb,
            and_(
                gpu_util_jsdb.job_id == SlurmJobDB.id,
                gpu_util_jsdb.name == "gpu_utilization",
            ),
            isouter=True,
        )
        .join(
            gpu_memory_jsdb,
            and_(
                gpu_memory_jsdb.job_id == SlurmJobDB.id,
                gpu_memory_jsdb.name == "gpu_memory",
            ),
            isouter=True,
        )
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
    # the utilization term differs by resource: CPU uses the cpu_utilization stat
    # mean, while GPU uses usage_metric -- whichever statistic currently defines
    # GPU usage (see its docstring below), so waste follows that definition
    # instead of pinning one statistic of its own.
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
    """Unused requested RGU-seconds: (1 - usage_metric) x requested_gpu_cost."""
    allocated_gpu_cost: float | None
    """RGU-seconds the scheduler allocated: elapsed_time x allocated_gres_gpu x
    DRAC RGU weight."""
    allocated_gpu_waste: float | None
    """Unused allocated RGU-seconds: (1 - usage_metric) x
    allocated_gpu_cost."""
    gpu_overbilling_cost: float | None
    """RGU-seconds billed beyond the request: elapsed_time x (allocated_gres_gpu
    - requested_gres_gpu) x DRAC RGU weight."""

    usage_metric: float | None
    """The GPU usage measure to read by default (fraction in [0, 1]), currently the
    gpu_sm_occupancy mean. One alias for whichever statistic SARC treats as "GPU
    usage", so redefining it stays confined to this module; read the named columns
    below only when one specific statistic is wanted."""

    # Quick access to most used Prometheus statistics.
    gpu_sm_occupancy_mean: float | None
    """Mean of GPU SM occupancy (between 0 and 1) for a GPU job."""
    gpu_sm_occupancy_max: float | None
    """Max of GPU SM occupancy (between 0 and 1) for a GPU job."""
    gpu_utilization_mean: float | None
    """Mean of GPU utilization (between 0 and 1) for a GPU job."""
    gpu_memory_max: float | None
    """Max of GPU memory usage (between 0 and 1) for a GPU job."""

    # User ID
    sarc_user_id: int
    display_name: str
    email: str
    member_type: MemberType | None = None
    """The user's member type valid at the job's submit time."""
    supervisors: list[int] | None = Field(sa_type=JSON)
    """Supervisor user ids, ordered, valid at the job's submit time."""


def _referenced_relations(expressions) -> set[FromClause]:
    """Tables/aliases whose columns appear in the expressions (subqueries included)."""
    return {
        el.table
        for expr in expressions
        for el in visitors.iterate(expr)
        if isinstance(el, ColumnClause) and el.table is not None
    }


def _prune_joins(node: FromClause, needed: set[FromClause]) -> FromClause:
    """Rebuild a join tree without the LEFT joins to unreferenced relations."""
    if not isinstance(node, Join):
        return node
    left = _prune_joins(node.left, needed)
    if node.isouter and node.right not in needed:
        return left
    return left.join(node.right, node.onclause, isouter=node.isouter)


def job_series_select(*columns: str) -> Select:
    """SELECT of the given job_series columns, minus the view joins they don't use.

    Querying the DB view keeps the removed joins' key columns marked as needed
    (Postgres never un-marks them), which alone can disqualify index-only scans.
    Building the same statement client-side, the planner never sees the unused
    joins or their keys. Dropping one is safe because every view join is LEFT on
    a unique key, so it never changes the row set. Use ``.subquery()`` and read
    every needed column through it.
    """
    view = JobSeriesDB.__sql_view__
    missing = sorted(set(columns) - set(view.selected_columns.keys()))
    if missing:
        raise KeyError(f"unknown job_series column(s): {missing}")
    keep = [view.selected_columns[name] for name in columns]
    froms = view.get_final_froms()
    assert len(froms) == 1, "job_series is expected to be a single join tree"
    return select(*keep).select_from(
        _prune_joins(froms[0], _referenced_relations(keep))
    )
