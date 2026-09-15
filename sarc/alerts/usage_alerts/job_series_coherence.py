import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.db.job_series import (
    CPU_MEAN_COLUMN,
    CPU_STAT,
    DASH_STATS,
    ELIGIBILITY,
    derived_exprs,
)

logger = logging.getLogger(__name__)

# Every way job_series can disagree with its sources, in one statement: an exact
# count and a sample of job ids for each. One statement, one snapshot -- the
# triggers write the row in the transaction that writes the job, so separate
# queries would read a concurrent scrape as a missing row. Driving from
# slurm_jobs covers everything: job_db_id is primary key and foreign key on
# slurm_jobs.id, so job_series can hold neither a duplicate nor an orphan.
MISMATCHES = f"""
SELECT count(*) FILTER (WHERE no_row) AS n_no_row,
       (array_agg(job_db_id ORDER BY job_db_id) FILTER (WHERE no_row))[1 : :limit]
           AS ids_no_row,
       count(*) FILTER (WHERE wrong_rgu) AS n_wrong_rgu,
       (array_agg(job_db_id ORDER BY job_db_id) FILTER (WHERE wrong_rgu))[1 : :limit]
           AS ids_wrong_rgu,
       count(*) FILTER (WHERE wrong_sm) AS n_wrong_sm,
       (array_agg(job_db_id ORDER BY job_db_id) FILTER (WHERE wrong_sm))[1 : :limit]
           AS ids_wrong_sm
  FROM (
    SELECT j.job_db_id,
           s.job_db_id IS NULL AS no_row,
           -- a job with no row at all is reported once, as no_row
           s.job_db_id IS NOT NULL
               AND coalesce(j.gpu, false) <> coalesce(s.gpu, false) AS wrong_rgu,
           s.job_db_id IS NOT NULL
               AND (st.mean IS NOT NULL) <> (s.sm IS NOT NULL) AS wrong_sm
      -- a subquery per side: both predicates name columns the two tables share,
      -- and the same population: harmonized_gpu_type is a foreign key on
      -- gpurgudb.name, whose rgu columns are NOT NULL, so a GPU type always
      -- yields an RGU.
      FROM (SELECT id AS job_db_id, (allocated_gres_gpu > 0 AND harmonized_gpu_type IS NOT NULL) AS gpu
            FROM slurm_jobs) j
      LEFT JOIN (SELECT job_db_id, gpu_sm_occupancy_mean AS sm, {ELIGIBILITY} AS gpu
                   FROM job_series) s ON s.job_db_id = j.job_db_id
      LEFT JOIN jobstatisticdb st
             ON st.job_id = j.job_db_id AND st.name = 'gpu_sm_occupancy'
  ) d
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

# One entry per statistic column: (name, jobstatisticdb column, job_series
# column) -- jobstatisticdb holds one row per (job, name), job_series one
# column per name.
STATS = [
    *(
        (stat, src, column)
        for stat, (mean_column, max_column) in DASH_STATS.items()
        for src, column in (("mean", mean_column), ("max", max_column))
    ),
    (CPU_STAT, "mean", CPU_MEAN_COLUMN),
]

# The RGU/cost/waste arithmetic, from the expressions the triggers themselves
# are generated from: the stored row (`t.`) for job columns and statistics,
# gpurgudb (`w.`) for the per-GPU-type RGU. Their inputs are compared above, so
# a mismatch here is the arithmetic's own.
DERIVED = derived_exprs("t.", "w.", "t.")

# `NaN = NaN` is true on Postgres, so two NaNs read as equal here -- wanted:
# a recorded NaN is a measurement, not drift.
DIFFERS = "\n        OR ".join(
    [
        *(f"t.{column} IS DISTINCT FROM j.{column}" for column in COPIED),
        *(f"t.{column} IS DISTINCT FROM {expr}" for column, expr in DISPLAY.items()),
        *(f"t.{column} IS DISTINCT FROM s.{column}" for _, _, column in STATS),
        *(f"t.{column} IS DISTINCT FROM {expr}" for column, expr in DERIVED.items()),
    ]
)

# A job's statistic rows read back as that set of columns, so the comparison
# below reads them like any other column.
PIVOT = ",\n           ".join(
    f"max({src}) FILTER (WHERE name = '{stat}') AS {column}"
    for stat, src, column in STATS
)

STALE_ROWS = f"""
SELECT j.id AS job_db_id
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


def _examples(ids: Sequence[int] | None) -> str:
    """`ids` as a message tail; empty when there are none. The count that
    precedes it in the message is exact, so the sample needs no ellipsis."""
    return f", e.g. job_db_id {', '.join(str(i) for i in ids)}" if ids else ""


def check_job_series_whole(report_limit: int = 20) -> bool:
    """
    Check what the whole of job_series can be compared for, in one statement.

    - every job has a job_series row;
    - every GPU job has its RGU value, and no other job has one;
    - every recorded `gpu_sm_occupancy` reached its job_series column.

    Scans both tables end to end. The column values need a window, and are
    `check_job_series_recent`'s half.

    Parameters
    ----------
    report_limit: int
        How many job ids to name per alert. Default is 20.

    Returns
    -------
    bool
        True if check succeeds, False otherwise.
    """
    from sarc.config import config

    if report_limit < 1:
        logger.error(
            f"Invalid report_limit (must be > 0) for job_series coherence: {report_limit}"
        )
        return False

    ok = True
    with config.db.session() as sess:
        m = sess.exec(  # ty: ignore[no-matching-overload]
            text(MISMATCHES), params={"limit": report_limit}
        ).one()

        if m.n_no_row:
            logger.error(
                f"[job_series] {m.n_no_row} jobs have no job_series row"
                f"{_examples(m.ids_no_row)}"
            )
            ok = False

        if m.n_wrong_rgu:
            logger.error(
                f"[job_series] {m.n_wrong_rgu} jobs have an RGU in job_series "
                f"but not in slurm_jobs, or the reverse{_examples(m.ids_wrong_rgu)}"
            )
            ok = False

        if m.n_wrong_sm:
            logger.error(
                f"[job_series] {m.n_wrong_sm} jobs have a gpu_sm_occupancy in job_series "
                f"but not in jobstatisticdb, or the reverse{_examples(m.ids_wrong_sm)}"
            )
            ok = False

    return ok


def check_job_series_recent(
    time_interval: timedelta = timedelta(days=1), report_limit: int = 20
) -> bool:
    """
    Check the columns job_series adds on its own, over recently submitted jobs.

    What the triggers copy, and the RGU/cost/waste arithmetic, recomputed from
    the source tables and compared to the stored row. Reads every row in the
    window, hence the window; what a whole-table pass can do instead is
    `check_job_series_whole`'s half.

    Parameters
    ----------
    time_interval: timedelta
        Width of the window, ending now and taken on `submit_time`. Default is
        1 day.
    report_limit: int
        How many job ids to name per alert. Default is 20.

    Returns
    -------
    bool
        True if check succeeds, False otherwise.
    """
    from sarc.config import config

    if time_interval <= timedelta(0):
        logger.error(
            f"Invalid time_interval (must be > 0) for job_series coherence: {time_interval}"
        )
        return False
    if report_limit < 1:
        logger.error(
            f"Invalid report_limit (must be > 0) for job_series coherence: {report_limit}"
        )
        return False

    with config.db.session() as sess:
        start = datetime.now(tz=UTC) - time_interval
        stale = sess.exec(  # ty: ignore[no-matching-overload]
            text(STALE_ROWS), params={"start": start}
        ).all()
        if stale:
            logger.error(
                f"[job_series] {len(stale)} of the jobs submitted since {start} "
                f"disagree with expected values recomputed from source tables"
                f"{_examples([row.job_db_id for row in stale][:report_limit])}"
            )
            return False

    return True


@dataclass
class JobSeriesWholeCheck(HealthCheck):
    """Health check comparing job_series to its sources, table-wide."""

    report_limit: int = 20

    def check(self) -> CheckResult:
        if check_job_series_whole(report_limit=self.report_limit):
            return self.ok()
        else:
            return self.fail()


@dataclass
class JobSeriesRecentCheck(HealthCheck):
    """Health check recomputing job_series columns for recently submitted jobs."""

    time_interval: timedelta = timedelta(days=1)
    report_limit: int = 20

    def check(self) -> CheckResult:
        if check_job_series_recent(
            time_interval=self.time_interval, report_limit=self.report_limit
        ):
            return self.ok()
        else:
            return self.fail()
