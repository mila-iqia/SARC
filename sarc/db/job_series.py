"""`job_series` -- the jobs read model: one wide materialized row per job.

`slurm_jobs` joined to `users`, `clusters`, `gpurgudb` and (pivoted) to
`jobstatisticdb`, with the RGU/cost/waste arithmetic precomputed and every
frequently-read statistic sitting in its own column: the single base that
/dash aggregates over, /v0/job/series pages out of, and the usage
notifications scan. Queries read `job_series` (directly, or through the thin
`job_series_view` wrapper that only adds the two time-ranged user subqueries
`member_type` and `supervisors`) and never touch the join fan-out themselves.

The table materializes `slurm_jobs` (one row per job, PK = job id, FK
CASCADE) maintained by triggers, so it is exact rather than a cache with a
staleness window: the triggers are AFTER ROW in the same transaction as the
base write, and a reader can only observe it lagging its base tables within
an in-flight transaction. The trigger set mirrors the old view's definition:

- AFTER INSERT/UPDATE on slurm_jobs (of the mirrored columns) upserts the
  job's row, re-reading users/clusters/gpurgudb and the whole stat pivot.
- AFTER INSERT/UPDATE/DELETE on jobstatisticdb patches one pivot column and
  rederives the four waste columns that consume it.
- AFTER UPDATE on gpurgudb re-weights every weight-derived column (rare).
- AFTER UPDATE on users / clusters re-syncs the copied display columns.

`member_type` and `supervisors` resolve validity ranges in `membertypedb` /
`supervisorsdb` retroactively edited by the user scrapers; materializing
those would need cascading range updates, so they stay read-time correlated
subqueries in the wrapper view -- evaluated per row only when selected.

Backfill and repair: `job_series_backfill_sql()` / `sarc db backfill-series`
(idempotent, `ON CONFLICT DO NOTHING` so it can run concurrently with the
triggers).

Columns are denormalized to what the window *scans* need (the covering
indexes' INCLUDE lists); everything else is reached by PK (a 50-row page) or
by the indexed lookups /v0 filters use. The GPU-only covering indexes are
partial on `ELIGIBILITY` -- the population every /dash panel reads.
"""

from sqlalchemy import Index, text
from sqlalchemy.dialects.postgresql import JSONB, aggregate_order_by
from sqlmodel import BIGINT, JSON, Field, col, func, select

from sarc.models.user import MemberType
from sarc.validators import datetime_utc

from .job import SlurmJobDB, SlurmState
from .sqlmodel import SQLModel, datetime_utc_field
from .users import MemberTypeDB, SupervisorsDB, SupervisorsHelper

# The statistics /dash reads, pivoted one (mean, max) pair per metric. Keep in
# sync with sarc.api.metrics._METRICS_0_1 (the metrics a request can name) plus
# the max columns the plots display. Derives the trigger SQL below, so the
# pivot and the table can never drift apart. `cpu_utilization` is mean-only:
# not a plottable dash metric, but needed for the cpu_waste columns the view
# exposes.
DASH_STATS: dict[str, tuple[str, str]] = {
    # jobstatisticdb.name -> (mean column, max column)
    "gpu_sm_occupancy": ("gpu_sm_occupancy_mean", "gpu_sm_occupancy_max"),
    "gpu_utilization": ("gpu_utilization_mean", "gpu_utilization_max"),
    "gpu_utilization_fp16": ("gpu_utilization_fp16_mean", "gpu_utilization_fp16_max"),
    "gpu_utilization_fp32": ("gpu_utilization_fp32_mean", "gpu_utilization_fp32_max"),
    "gpu_utilization_fp64": ("gpu_utilization_fp64_mean", "gpu_utilization_fp64_max"),
    "gpu_memory": ("gpu_memory_mean", "gpu_memory_max"),
    "system_memory": ("system_memory_mean", "system_memory_max"),
}
CPU_STAT = "cpu_utilization"
CPU_MEAN_COLUMN = "cpu_utilization_mean"

# The population every /dash panel reads: a GPU job whose RGU is computable.
# Exactly the old `_gpu_only` predicate plus the gpurgudb join the old view
# made with the same effect (a missing weight reads NULL everywhere).
ELIGIBILITY = "allocated_gres_gpu > 0 AND gpu_type_rgu_drac IS NOT NULL"

# slurm_jobs columns mirrored verbatim. `id` maps to job_db_id; the
# scraper-internal latest_scraped_* bookkeeping is not part of the view.
JOB_SERIES_EXCLUDED_JOB_COLS = frozenset(
    {"id", "latest_scraped_start", "latest_scraped_end"}
)
COPIED_JOB_COLUMNS = [
    c.name
    for c in SlurmJobDB.__table__.columns  # ty:ignore[unresolved-attribute]
    if c.name not in JOB_SERIES_EXCLUDED_JOB_COLS
]
# Copied from users (all columns but id) and clusters.
USER_COLUMNS = ["display_name", "email"]
CLUSTER_NAME_COLUMN = "cluster_name"
WEIGHT_COLUMNS = ["gpu_type_rgu", "gpu_type_rgu_drac"]
RGU_COLUMNS = [
    "requested_rgu",
    "requested_rgu_drac",
    "allocated_rgu",
    "allocated_rgu_drac",
]
COST_COLUMNS = [
    "requested_cpu_cost",
    "requested_cpu_waste",
    "allocated_cpu_cost",
    "allocated_cpu_waste",
    "cpu_overbilling_cost",
    "requested_gpu_cost",
    "requested_gpu_waste",
    "allocated_gpu_cost",
    "allocated_gpu_waste",
    "gpu_overbilling_cost",
]
PIVOT_COLUMNS = [*[c for pair in DASH_STATS.values() for c in pair], CPU_MEAN_COLUMN]
ALL_VALUE_COLUMNS = [
    *COPIED_JOB_COLUMNS,
    *USER_COLUMNS,
    CLUSTER_NAME_COLUMN,
    *WEIGHT_COLUMNS,
    *RGU_COLUMNS,
    *COST_COLUMNS,
    *PIVOT_COLUMNS,
]

# The columns the dash window scans read; the GPU-partial covering indexes'
# INCLUDE. Everything else on the table is reached by PK (a page of rows) or
# by the plain v0/notification indexes.
COVERING = [
    "job_db_id",
    "start_time",
    "elapsed_time",
    "allocated_rgu_drac",
    "cluster_id",
    "sarc_user_id",
    "job_state",
    "job_id",
    "submit_time",
    "harmonized_gpu_type",
    "requested_gres_gpu",
    "allocated_billing",
    *[c for pair in DASH_STATS.values() for c in pair],
]


# SQL-identifier quoting for the generated column lists (slurm_jobs carries a
# `group` column -- a reserved word, and the uppercase flag columns need their
# case preserved; everything else is a plain lowercase identifier).
def _q(name: str) -> str:
    plain = name.isascii() and name.isidentifier() and name.islower()
    return name if plain and name not in ("group",) else f'"{name}"'


def _stat_pivot_expr(agg: str) -> str:
    """The pivot's aggregate list: one FILTERed aggregate per pivot column."""
    return ",\n                ".join(
        f"{agg}(st.{src}) FILTER (WHERE st.name = '{name}') AS {column}"
        for name, (mean_col, max_col) in [
            *DASH_STATS.items(),
            (CPU_STAT, (CPU_MEAN_COLUMN, None)),
        ]
        for src, column in (("mean", mean_col), ("max", max_col))
        if column is not None
    )


def _pivot_select(job_id_expr: str) -> str:
    """One row: the pivot for the job named by ``job_id_expr``."""
    names = ", ".join(f"'{n}'" for n in [*DASH_STATS, CPU_STAT])
    return (
        f"select {_stat_pivot_expr('max')} from jobstatisticdb st"
        f" where st.job_id = {job_id_expr} and st.name in ({names})"
    )


def _derived_exprs(j: str, w: str, s: str) -> dict[str, str]:
    """The weight/cost/waste columns as SQL expressions over the job (``j``),
    the gpurgudb row (``w``) and the stat pivot (``s``) -- each a table alias
    or a plpgsql record prefix. Same expressions the view computed inline.
    """
    elapsed = f"{j}elapsed_time"
    req_cpu = f"{j}requested_cpu"
    alloc_cpu = f"{j}allocated_cpu"
    req_gpu = f"{j}requested_gres_gpu"
    alloc_gpu = f"{j}allocated_gres_gpu"
    sm = f"{s}gpu_sm_occupancy_mean"
    cpu = f"{s}{CPU_MEAN_COLUMN}"
    req_gpu_cost = f"{elapsed} * {req_gpu} * {w}drac_rgu"
    alloc_gpu_cost = f"{elapsed} * {alloc_gpu} * {w}drac_rgu"
    return {
        "gpu_type_rgu": f"{w}rgu",
        "gpu_type_rgu_drac": f"{w}drac_rgu",
        # The *_rgu coalesce a missing GPU count to 0 (the weights stay NULL
        # without a gpurgudb row); the costs/waste keep NULL meaning
        # "not computable", exactly like the view's raw-count products.
        "requested_rgu": f"coalesce({req_gpu}, 0) * {w}rgu",
        "requested_rgu_drac": f"coalesce({req_gpu}, 0) * {w}drac_rgu",
        "allocated_rgu": f"coalesce({alloc_gpu}, 0) * {w}rgu",
        "allocated_rgu_drac": f"coalesce({alloc_gpu}, 0) * {w}drac_rgu",
        "requested_cpu_cost": f"{elapsed} * {req_cpu}",
        "requested_cpu_waste": f"(1 - {cpu}) * ({elapsed} * {req_cpu})",
        "allocated_cpu_cost": f"{elapsed} * {alloc_cpu}",
        "allocated_cpu_waste": f"(1 - {cpu}) * ({elapsed} * {alloc_cpu})",
        "cpu_overbilling_cost": f"{elapsed} * ({alloc_cpu} - {req_cpu})",
        "requested_gpu_cost": req_gpu_cost,
        "requested_gpu_waste": f"(1 - {sm}) * ({req_gpu_cost})",
        "allocated_gpu_cost": alloc_gpu_cost,
        "allocated_gpu_waste": f"(1 - {sm}) * ({alloc_gpu_cost})",
        "gpu_overbilling_cost": (
            f"{elapsed} * ({alloc_gpu} - {req_gpu}) * {w}drac_rgu"
        ),
    }


# -- Trigger SQL ------------------------------------------------------------- #

_SYNC_JOB_BODY = ", ".join(f"new.{_q(c)}" for c in COPIED_JOB_COLUMNS)
_SYNC_JOB_DERIVED = ", ".join(
    _derived_exprs("new.", "w.", "s.")[c]
    for c in [*WEIGHT_COLUMNS, *RGU_COLUMNS, *COST_COLUMNS]
)
_SYNC_JOB_PIVOTS = ", ".join(f"s.{c}" for c in PIVOT_COLUMNS)

_SYNC_JOB = f"""
returns trigger
language plpgsql
as $$
begin
    insert into job_series (job_db_id, {", ".join(_q(c) for c in ALL_VALUE_COLUMNS)})
    select new.id, {_SYNC_JOB_BODY},
           u.display_name, u.email,
           c.name,
           {_SYNC_JOB_DERIVED},
           {_SYNC_JOB_PIVOTS}
      from (select 1) x
      left join users u on u.id = new.sarc_user_id
      left join clusters c on c.id = new.cluster_id
      left join gpurgudb w on w.name = new.harmonized_gpu_type
      left join lateral ({_pivot_select("new.id")}) s on true
    on conflict (job_db_id) do update set
        {", ".join(f"{_q(c)} = excluded.{_q(c)}" for c in ALL_VALUE_COLUMNS)};
    return new;
end;
$$"""

_STAT_SET = ",\n".join(
    [
        *[
            f"    {column} = case when new.name = '{name}' then new.{src}"
            f"\n                   else job_series.{column} end"
            for name, (mean_col, max_col) in DASH_STATS.items()
            for src, column in (("mean", mean_col), ("max", max_col))
        ],
        f"    {CPU_MEAN_COLUMN} = case when new.name = '{CPU_STAT}' then new.mean"
        f"\n                   else job_series.{CPU_MEAN_COLUMN} end",
    ]
)

_CLEAR_SET = ",\n".join(
    [
        *[
            f"    {column} = case when old.name = '{name}' then null"
            f"\n                   else job_series.{column} end"
            for name, (mean_col, max_col) in DASH_STATS.items()
            for _src, column in (("mean", mean_col), ("max", max_col))
        ],
        f"    {CPU_MEAN_COLUMN} = case when old.name = '{CPU_STAT}' then null"
        f"\n                   else job_series.{CPU_MEAN_COLUMN} end",
    ]
)

# The two stats that feed waste columns: when one of them moves, the four
# waste products must be re-derived (a separate statement, so it reads the
# pivot values the pivot UPDATE above just wrote).
_WASTE_SET = """    requested_cpu_waste = (1 - cpu_utilization_mean) * requested_cpu_cost,
    allocated_cpu_waste = (1 - cpu_utilization_mean) * allocated_cpu_cost,
    requested_gpu_waste = (1 - gpu_sm_occupancy_mean) * requested_gpu_cost,
    allocated_gpu_waste = (1 - gpu_sm_occupancy_mean) * allocated_gpu_cost"""

_STAT_NAMES = ", ".join(f"'{n}'" for n in [*DASH_STATS, CPU_STAT])

_SYNC_STAT = f"""
returns trigger
language plpgsql
as $$
begin
    if new.name not in ({_STAT_NAMES}) then
        return new;
    end if;
    update job_series set
{_STAT_SET}
    where job_db_id = new.job_id;
    if new.name in ('{CPU_STAT}', 'gpu_sm_occupancy') then
        update job_series set
{_WASTE_SET}
        where job_db_id = new.job_id;
    end if;
    return new;
end;
$$"""

_CLEAR_STAT = f"""
returns trigger
language plpgsql
as $$
begin
    if old.name not in ({_STAT_NAMES}) then
        return old;
    end if;
    update job_series set
{_CLEAR_SET}
    where job_db_id = old.job_id;
    if old.name in ('{CPU_STAT}', 'gpu_sm_occupancy') then
        update job_series set
{_WASTE_SET}
        where job_db_id = old.job_id;
    end if;
    return old;
end;
$$"""

_WEIGHT_DERIVED = ",\n".join(
    f"    {c} = {_derived_exprs('job_series.', 'g.', 'job_series.')[c]}"
    for c in [*WEIGHT_COLUMNS, *RGU_COLUMNS, *COST_COLUMNS]
    if c not in ("requested_cpu_waste", "allocated_cpu_waste")
)

_SYNC_WEIGHTS = f"""
returns trigger
language plpgsql
as $$
begin
    update job_series set
{_WEIGHT_DERIVED}
      from gpurgudb g
     where job_series.harmonized_gpu_type = g.name
       and (job_series.gpu_type_rgu_drac is distinct from g.drac_rgu
            or job_series.gpu_type_rgu is distinct from g.rgu);
    return null;
end;
$$"""

_SYNC_USER = """
returns trigger
language plpgsql
as $$
begin
    if new.display_name is distinct from old.display_name
       or new.email is distinct from old.email then
        update job_series
           set display_name = new.display_name, email = new.email
         where sarc_user_id = new.id;
    end if;
    return new;
end;
$$"""

_SYNC_CLUSTER = """
returns trigger
language plpgsql
as $$
begin
    if new.name is distinct from old.name then
        update job_series set cluster_name = new.name where cluster_id = new.id;
    end if;
    return null;
end;
$$"""

JOB_SERIES_FUNCTIONS: dict[str, str] = {
    "job_series_sync_job()": _SYNC_JOB,
    "job_series_sync_stat()": _SYNC_STAT,
    "job_series_clear_stat()": _CLEAR_STAT,
    "job_series_sync_weights()": _SYNC_WEIGHTS,
    "job_series_sync_user()": _SYNC_USER,
    "job_series_sync_cluster()": _SYNC_CLUSTER,
}

# name -> (table, definition), for PGTrigger registration in alembic/env.py.
# slurm_jobs deletions are handled by the FK's ON DELETE CASCADE. The slurm_jobs
# UPDATE column list = the mirrored columns, so a scrape cycle that only touches
# the excluded latest_scraped_* bookkeeping costs no materialization work.
JOB_SERIES_TRIGGERS: dict[str, tuple[str, str]] = {
    "slurm_jobs_job_series": (
        "slurm_jobs",
        f"AFTER INSERT OR UPDATE OF {', '.join(_q(c) for c in COPIED_JOB_COLUMNS)} ON slurm_jobs "
        "FOR EACH ROW EXECUTE FUNCTION job_series_sync_job()",
    ),
    "jobstatisticdb_job_series": (
        "jobstatisticdb",
        "AFTER INSERT OR UPDATE OF mean, max ON jobstatisticdb "
        "FOR EACH ROW EXECUTE FUNCTION job_series_sync_stat()",
    ),
    "jobstatisticdb_job_series_del": (
        "jobstatisticdb",
        "AFTER DELETE ON jobstatisticdb "
        "FOR EACH ROW EXECUTE FUNCTION job_series_clear_stat()",
    ),
    "gpurgudb_job_series": (
        "gpurgudb",
        "AFTER UPDATE OF rgu, drac_rgu ON gpurgudb "
        "FOR EACH STATEMENT EXECUTE FUNCTION job_series_sync_weights()",
    ),
    "users_job_series": (
        "users",
        "AFTER UPDATE OF display_name, email ON users "
        "FOR EACH ROW EXECUTE FUNCTION job_series_sync_user()",
    ),
    "clusters_job_series": (
        "clusters",
        "AFTER UPDATE OF name ON clusters "
        "FOR EACH ROW EXECUTE FUNCTION job_series_sync_cluster()",
    ),
}


def job_series_backfill_sql(where: str = "") -> str:
    """Idempotent bulk (re)sync of job_series from its base tables.

    `ON CONFLICT DO NOTHING` is the concurrency contract: the triggers may have
    written a row since this scan's snapshot; the trigger version is at least as
    fresh, so keep it. To force a rebuild, TRUNCATE job_series first.
    ``where`` is appended to the outer WHERE (e.g. an id range, for chunked
    backfills that keep transactions short).
    """
    derived = _derived_exprs("j.", "w.", "s.")
    return f"""
INSERT INTO job_series (job_db_id, {", ".join(_q(c) for c in ALL_VALUE_COLUMNS)})
SELECT j.id, {", ".join(f"j.{_q(c)}" for c in COPIED_JOB_COLUMNS)},
       u.display_name, u.email, c.name,
       {", ".join(derived[c] for c in [*WEIGHT_COLUMNS, *RGU_COLUMNS, *COST_COLUMNS])},
       {", ".join(f"s.{c}" for c in PIVOT_COLUMNS)}
  FROM slurm_jobs j
  LEFT JOIN users u ON u.id = j.sarc_user_id
  LEFT JOIN clusters c ON c.id = j.cluster_id
  LEFT JOIN gpurgudb w ON w.name = j.harmonized_gpu_type
  LEFT JOIN LATERAL (
    SELECT {_stat_pivot_expr("max")}
      FROM jobstatisticdb st
     WHERE st.job_id = j.id
  ) s ON true
 WHERE true
   {where}
ON CONFLICT (job_db_id) DO NOTHING
"""


class JobSeriesTable(SQLModel, table=True):
    """Materialized jobs read model -- see module docstring.

    One row per `slurm_jobs` row (every job, CPU ones included; the /dash GPU
    population is the `ELIGIBILITY` subset the partial indexes cover). Trigger-
    maintained; written only by the triggers and `job_series_backfill_sql()`.

    The three GPU-partial covering indexes serve every /dash window scan
    (window by run end, by user, range by submit_time) as parallel index-only
    scans; `ix_job_series_end_stats` exists only so ANALYZE collects a
    histogram for the `slurm_job_end(...)` expression (a partial index's
    expression stats aren't used to plan a scan of itself -- see the same trick
    on slurm_jobs). The plain indexes cover /v0's and the notifications'
    filters (windowed paging by submit/end time, single-job lookups, per-user
    and per-email filters).
    """

    __tablename__ = "job_series"
    __table_args__ = (
        Index(
            "ix_job_series_end",
            text("slurm_job_end(start_time, elapsed_time)"),
            postgresql_include=COVERING,
            postgresql_where=text(ELIGIBILITY),
        ),
        Index(
            "ix_job_series_user",
            "sarc_user_id",
            text("slurm_job_end(start_time, elapsed_time)"),
            postgresql_include=[c for c in COVERING if c != "sarc_user_id"],
            postgresql_where=text(ELIGIBILITY),
        ),
        # Submission-window queries: submitted-job counts and the time-limit
        # heatmaps select on submit_time and read no statistics (time_limit is
        # the one extra they need).
        Index(
            "ix_job_series_submit",
            "submit_time",
            postgresql_include=[
                "job_db_id",
                "start_time",
                "elapsed_time",
                "time_limit",
                "allocated_rgu_drac",
                "cluster_id",
                "sarc_user_id",
                "job_state",
            ],
            postgresql_where=text(ELIGIBILITY),
        ),
        # Statistics twin of ix_job_series_end (see docstring): non-partial,
        # INCLUDE-less, never scanned.
        Index(
            "ix_job_series_end_stats", text("slurm_job_end(start_time, elapsed_time)")
        ),
        # /v0 and notifications: windowed paging and end-time ranges.
        Index("ix_job_series_submit_time", "submit_time"),
        Index("ix_job_series_end_time", "end_time"),
        # /v0 single-job and per-user filters, notifications' email filter.
        Index("ix_job_series_job_id", "job_id"),
        Index("ix_job_series_sarc_user_id", "sarc_user_id"),
        Index("ix_job_series_email", "email"),
    )

    job_db_id: int = Field(
        primary_key=True, foreign_key="slurm_jobs.id", ondelete="CASCADE"
    )
    # -- mirrored verbatim from slurm_jobs (see COPIED_JOB_COLUMNS) --
    cluster_id: int
    account: str
    job_id: int
    array_job_id: int | None = None
    task_id: int | None = None
    name: str
    cluster_user: str
    group: str
    job_state: SlurmState
    exit_code: int | None = None
    signal: int | None = None
    partition: str
    nodes: list[str] = Field(sa_type=JSONB)
    work_dir: str
    submit_line: str | None = None
    constraints: str | None = None
    priority: int | None = None
    qos: str | None = None
    CLEAR_SCHEDULING: bool = False
    STARTED_ON_SUBMIT: bool = False
    STARTED_ON_SCHEDULE: bool = False
    STARTED_ON_BACKFILL: bool = False
    time_limit: int | None = None
    submit_time: datetime_utc = datetime_utc_field()
    start_time: datetime_utc | None = datetime_utc_field(default=None)
    end_time: datetime_utc | None = datetime_utc_field(default=None)
    elapsed_time: float
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
    harmonized_gpu_type: str | None = None
    sarc_user_id: int
    # -- copied display columns (kept in sync by the users/clusters triggers) --
    display_name: str
    email: str
    cluster_name: str | None = None
    # -- denormalized weights and derived RGU/cost/waste columns --
    gpu_type_rgu: float | None = None
    gpu_type_rgu_drac: float | None = None
    requested_rgu: float | None = None
    requested_rgu_drac: float | None = None
    allocated_rgu: float | None = None
    allocated_rgu_drac: float | None = None
    requested_cpu_cost: float | None = None
    requested_cpu_waste: float | None = None
    allocated_cpu_cost: float | None = None
    allocated_cpu_waste: float | None = None
    cpu_overbilling_cost: float | None = None
    requested_gpu_cost: float | None = None
    requested_gpu_waste: float | None = None
    allocated_gpu_cost: float | None = None
    allocated_gpu_waste: float | None = None
    gpu_overbilling_cost: float | None = None
    # -- jobstatisticdb pivoted, one (mean, max) column pair per metric --
    gpu_sm_occupancy_mean: float | None = None
    gpu_sm_occupancy_max: float | None = None
    gpu_utilization_mean: float | None = None
    gpu_utilization_max: float | None = None
    gpu_utilization_fp16_mean: float | None = None
    gpu_utilization_fp16_max: float | None = None
    gpu_utilization_fp32_mean: float | None = None
    gpu_utilization_fp32_max: float | None = None
    gpu_utilization_fp64_mean: float | None = None
    gpu_utilization_fp64_max: float | None = None
    gpu_memory_mean: float | None = None
    gpu_memory_max: float | None = None
    system_memory_mean: float | None = None
    system_memory_max: float | None = None
    cpu_utilization_mean: float | None = None


# -- the wrapper view: the materialized table plus the two time-ranged user
# subqueries, kept read-time (see module docstring) -------------------------- #

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
        SupervisorsDB.user_id == JobSeriesTable.sarc_user_id,
        SupervisorsDB.valid.contains(JobSeriesTable.submit_time),
    )
    .scalar_subquery()
).label("supervisors")

#### member_type
# Correlated subquery, not a join: pruned when member_type is not selected (the
# dashboard reads the table directly; /v0/job/series caps pages at 100 rows so
# the per-row GiST lookup on membertypedb stays cheap).
member_type_subq = (
    select(MemberTypeDB.member_type)
    .where(
        MemberTypeDB.user_id == JobSeriesTable.sarc_user_id,
        MemberTypeDB.valid.contains(JobSeriesTable.submit_time),
    )
    .scalar_subquery()
).label("member_type")


class JobSeriesDB(SQLModel, table=True):
    """`job_series_view`: the `job_series` table plus ``member_type`` and
    ``supervisors`` (time-ranged user lookups, evaluated per row only when
    selected) and the ``usage_metric`` alias.

    All of the heavy joins (users, clusters, gpurgudb, jobstatisticdb pivots)
    and the RGU/cost/waste arithmetic live in the table itself, maintained by
    triggers -- reading this view is a single-relation scan. ``usage_metric``
    is an alias for ``gpu_sm_occupancy_mean``: whichever statistic SARC treats
    as "GPU usage", so redefining it stays confined to this module.
    """

    __tablename__ = "job_series_view"  # This is filtered out in table creation
    __sql_view__ = select(
        *[
            col(JobSeriesTable.__table__.c[c.name])  # ty:ignore[unresolved-attribute]
            for c in JobSeriesTable.__table__.columns  # ty:ignore[unresolved-attribute]
        ],
        col(JobSeriesTable.gpu_sm_occupancy_mean).label("usage_metric"),
        member_type_subq,
        supervisors_subq,
    )  # ty: ignore[no-matching-overload]

    # job identification
    job_db_id: int = Field(primary_key=True)
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
    # datetime_utc_field, matching SlurmJobDB: a bare `datetime` maps to
    # DateTime(timezone=False), so tz-aware bounds went out naive and Postgres
    # read them back in the session TimeZone. Python-mapping only.
    submit_time: datetime_utc = datetime_utc_field()
    start_time: datetime_utc | None = datetime_utc_field(default=None)
    end_time: datetime_utc | None = datetime_utc_field(default=None)
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
    """RGU-seconds the scheduler allocated: elapsed_time x allocated_gres_gpu x DRAC
    RGU weight."""
    allocated_gpu_waste: float | None
    """Unused allocated RGU-seconds: (1 - usage_metric) x allocated_gpu_cost."""
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
