import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.db.job_series import CPU_MEAN_COLUMN, CPU_STAT, DASH_STATS, ELIGIBILITY

logger = logging.getLogger(__name__)

# A GPU job as `slurm_jobs` sees it -- the same population as job_series'
# ELIGIBILITY: harmonized_gpu_type is a foreign key on gpurgudb.name, whose rgu
# columns are NOT NULL, so a non-NULL type always yields a non-NULL rgu.
GPU_JOBS = "allocated_gres_gpu > 0 AND harmonized_gpu_type IS NOT NULL"

# All counts in one statement, so they share one snapshot. The triggers run in
# the transaction that writes the job, so a snapshot never sees a job without
# its job_series row; taken separately, a concurrent scrape would read as a
# missing row.
COUNTS = f"""
SELECT j.jobs, j.gpu_jobs,
       s.series, s.with_rgu, s.with_sm_occupancy,
       t.sm_occupancy_stats
  FROM (SELECT count(id) AS jobs,
               count(id) FILTER (WHERE {GPU_JOBS}) AS gpu_jobs
          FROM slurm_jobs) j,
       (SELECT count(job_db_id) AS series,
               count(job_db_id) FILTER (WHERE {ELIGIBILITY}) AS with_rgu,
               count(job_db_id) FILTER (WHERE gpu_sm_occupancy_mean IS NOT NULL)
                   AS with_sm_occupancy
          FROM job_series) s,
       (SELECT count(id) AS sm_occupancy_stats
          FROM jobstatisticdb
         WHERE name = 'gpu_sm_occupancy' AND mean IS NOT NULL) t
"""

MISSING_JOBS = """
SELECT j.id
  FROM slurm_jobs j
  LEFT JOIN job_series s ON s.job_db_id = j.id
 WHERE s.job_db_id IS NULL
 ORDER BY j.id
 LIMIT :limit
"""

# -- what only job_series has, by the trigger that maintains it -------------- #

# From slurm_jobs. One trigger writes every mirrored column in a single
# statement, so they cannot drift apart: these few stand for all of them, and
# are the ones the arithmetic below consumes.
COPIED = [
    "elapsed_time",
    "requested_cpu",
    "allocated_cpu",
    "requested_gres_gpu",
    "allocated_gres_gpu",
    "harmonized_gpu_type",
]

# From users and clusters, each by its own trigger.
DISPLAY = {
    "display_name": "u.display_name",
    "email": "u.email",
    "cluster_name": "c.name",
}

# From gpurgudb.
WEIGHTS = {"gpu_type_rgu": "w.rgu", "gpu_type_rgu_drac": "w.drac_rgu"}

# From jobstatisticdb, pivoted: (statistic, source column, job_series column).
STATS = [
    *(
        (stat, src, column)
        for stat, (mean_column, max_column) in DASH_STATS.items()
        for src, column in (("mean", mean_column), ("max", max_column))
    ),
    (CPU_STAT, "mean", CPU_MEAN_COLUMN),
]

# Computed. Every input is itself a job_series column checked above, so these
# read the row alone. Same arithmetic as `_derived_exprs` in sarc/db/job_series.py,
# and like the trigger that maintains them the waste columns read the stored cost.
DERIVED = {
    "requested_rgu": "coalesce(t.requested_gres_gpu, 0) * t.gpu_type_rgu",
    "requested_rgu_drac": "coalesce(t.requested_gres_gpu, 0) * t.gpu_type_rgu_drac",
    "allocated_rgu": "coalesce(t.allocated_gres_gpu, 0) * t.gpu_type_rgu",
    "allocated_rgu_drac": "coalesce(t.allocated_gres_gpu, 0) * t.gpu_type_rgu_drac",
    "requested_cpu_cost": "t.elapsed_time * t.requested_cpu",
    "requested_cpu_waste": "(1 - t.cpu_utilization_mean) * t.requested_cpu_cost",
    "allocated_cpu_cost": "t.elapsed_time * t.allocated_cpu",
    "allocated_cpu_waste": "(1 - t.cpu_utilization_mean) * t.allocated_cpu_cost",
    "cpu_overbilling_cost": "t.elapsed_time * (t.allocated_cpu - t.requested_cpu)",
    "requested_gpu_cost": "t.elapsed_time * t.requested_gres_gpu * t.gpu_type_rgu_drac",
    "requested_gpu_waste": "(1 - t.gpu_sm_occupancy_mean) * t.requested_gpu_cost",
    "allocated_gpu_cost": "t.elapsed_time * t.allocated_gres_gpu * t.gpu_type_rgu_drac",
    "allocated_gpu_waste": "(1 - t.gpu_sm_occupancy_mean) * t.allocated_gpu_cost",
    "gpu_overbilling_cost": (
        "t.elapsed_time * (t.allocated_gres_gpu - t.requested_gres_gpu)"
        " * t.gpu_type_rgu_drac"
    ),
}

# `NaN = NaN` is true on Postgres, so two NaNs read as equal here -- wanted:
# a recorded NaN is a measurement, not drift.
DIFFERS = "\n        OR ".join(
    [
        *(f"t.{column} IS DISTINCT FROM j.{column}" for column in COPIED),
        *(f"t.{column} IS DISTINCT FROM {expr}" for column, expr in DISPLAY.items()),
        *(f"t.{column} IS DISTINCT FROM {expr}" for column, expr in WEIGHTS.items()),
        *(f"t.{column} IS DISTINCT FROM s.{column}" for _, _, column in STATS),
        *(f"t.{column} IS DISTINCT FROM {expr}" for column, expr in DERIVED.items()),
    ]
)

PIVOT = ",\n           ".join(
    f"max({src}) FILTER (WHERE name = '{stat}') AS {column}"
    for stat, src, column in STATS
)

STALE_ROWS = f"""
SELECT j.id
  FROM slurm_jobs j
  JOIN job_series t ON t.job_db_id = j.id
  LEFT JOIN users u ON u.id = j.sarc_user_id
  LEFT JOIN clusters c ON c.id = j.cluster_id
  LEFT JOIN gpurgudb w ON w.name = j.harmonized_gpu_type
  LEFT JOIN LATERAL (
    SELECT {PIVOT}
      FROM jobstatisticdb
     WHERE job_id = j.id
  ) s ON true
 WHERE j.submit_time >= :start
   AND ({DIFFERS})
 ORDER BY j.id
"""


def _sample(ids: list[int], limit: int) -> str:
    """`ids` as a message fragment, cut to `limit` entries."""
    shown = ", ".join(str(i) for i in ids[:limit])
    return f"{shown}, ..." if len(ids) > limit else shown


def check_job_series_coherence(
    time_interval: timedelta | None = timedelta(days=1), report_limit: int = 20
) -> bool:
    """
    Check that the job_series table agrees with the tables it is built from.

    - every job has a job_series row;
    - every GPU job has its RGU columns;
    - every recorded `gpu_sm_occupancy` reached the matching pivot column;
    - over `time_interval`, every column job_series adds on its own -- what the
      triggers copy, and the RGU/cost/waste arithmetic -- still holds.

    The first three only count, so they span the whole database; the last reads
    each row, hence the window.

    Parameters
    ----------
    time_interval: timedelta
        Width of the window, ending now and taken on `submit_time`, over which
        rows are compared column by column. Default is 1 day. None skips that
        comparison, leaving only the counts.
    report_limit: int
        How many job ids to name per alert. Default is 20.

    Returns
    -------
    bool
        True if check succeeds, False otherwise.
    """
    from sarc.config import config

    ok = True
    with config.db.session() as sess:
        counts = sess.exec(text(COUNTS)).one()  # ty: ignore[no-matching-overload]

        # Counting is enough: job_db_id is both primary key and foreign key on
        # slurm_jobs.id, so job_series can hold neither a duplicate nor an orphan.
        if counts.jobs != counts.series:
            missing = sess.exec(  # ty: ignore[no-matching-overload]
                text(MISSING_JOBS), params={"limit": report_limit + 1}
            ).all()
            logger.error(
                f"[job_series] {counts.jobs - counts.series} jobs have no job_series row "
                f"({counts.series} rows for {counts.jobs} jobs), "
                f"e.g. job_db_id {_sample([row.id for row in missing], report_limit)}"
            )
            ok = False

        if counts.gpu_jobs != counts.with_rgu:
            logger.error(
                f"[job_series] {counts.gpu_jobs} GPU jobs but {counts.with_rgu} rows "
                f"have RGU columns"
            )
            ok = False

        if counts.sm_occupancy_stats != counts.with_sm_occupancy:
            logger.error(
                f"[job_series] {counts.sm_occupancy_stats} gpu_sm_occupancy statistics "
                f"but {counts.with_sm_occupancy} rows carry one"
            )
            ok = False

        if time_interval is not None:
            start = datetime.now(tz=UTC) - time_interval
            stale = sess.exec(  # ty: ignore[no-matching-overload]
                text(STALE_ROWS), params={"start": start}
            ).all()
            if stale:
                logger.error(
                    f"[job_series] {len(stale)} of the jobs submitted since {start} "
                    f"disagree with their source tables, e.g. job_db_id "
                    f"{_sample([row.id for row in stale], report_limit)}"
                )
                ok = False

    return ok


@dataclass
class JobSeriesCoherenceCheck(HealthCheck):
    """Health check for the job_series table"""

    time_interval: timedelta | None = timedelta(days=1)
    report_limit: int = 20

    def check(self) -> CheckResult:
        if check_job_series_coherence(
            time_interval=self.time_interval, report_limit=self.report_limit
        ):
            return self.ok()
        else:
            return self.fail()
