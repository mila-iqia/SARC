import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlmodel import col, func, select

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB

logger = logging.getLogger(__name__)


def _unharmonized(start: datetime | None, cluster_names: list[str] | None, *columns):
    """Select `columns` over the GPU jobs that have no harmonized GPU type.

    `allocated_gres_gpu > 0` is the first conjunct of the `ELIGIBILITY` predicate in
    `sarc/db/job_series.py`; the second one is the `gpurgudb` row reached through
    `harmonized_gpu_type`. So this is exactly the population that drops out of the
    whole `/dash` scope, and whose RGU, cost and waste are NULL.
    """
    query = (
        select(*columns)
        .select_from(SlurmJobDB)
        .join(SlurmClusterDB, col(SlurmJobDB.cluster_id) == col(SlurmClusterDB.id))
        .where(
            col(SlurmJobDB.allocated_gres_gpu) > 0,
            col(SlurmJobDB.harmonized_gpu_type).is_(None),
        )
    )
    if start is not None:
        query = query.where(SlurmJobDB.submit_time >= start)
    if cluster_names:
        query = query.where(col(SlurmClusterDB.name).in_(cluster_names))
    return query


def check_harmonized_gpu_types(
    time_interval: timedelta | None = timedelta(days=1),
    report_limit: int = 5,
    cluster_names: list[str] | None = None,
) -> bool:
    """
    Check that GPU jobs have a harmonized GPU type.
    Log an alert per cluster and `allocated_gpu_type` that has jobs without one.

    A job counted here silently leaves the `/dash` population and has NULL RGU, cost
    and waste: the symptom is an under-count, never an error. There are two ways in,
    with two different fixes:

    - an `allocated_gpu_type` that recurs is a key missing from the cluster's
      `gpus_per_nodes` mapping -- look up the named jobs' `nodes` to see which entry;
    - no `allocated_gpu_type` at all means sacct reported no GPU name, so
      `fix_gpu_types` -- which only reads jobs that have one -- can never repair them.

    Parameters
    ----------
    time_interval: timedelta
        Width of the window, ending now and taken on `submit_time`. Default is 1 day.
        If None, all jobs are checked.
    report_limit: int
        How many example job ids to name per alert. Default is 5. Which ones is up to
        the planner: they are there to be looked up, not to be the latest.
    cluster_names: list
        Clusters to check. Jobs from other clusters are ignored.
        If empty (or not specified), check every cluster.

    Returns
    -------
    bool
        True if check succeeds, False otherwise.
    """
    from sarc.config import config

    if time_interval is not None and time_interval <= timedelta(0):
        logger.error(
            f"Invalid time_interval (must be > 0) for harmonized GPU types: {time_interval}"
        )
        return False
    if report_limit < 1:
        logger.error(
            f"Invalid report_limit (must be > 0) for harmonized GPU types: {report_limit}"
        )
        return False

    # A name that is not a configured cluster would silently narrow the check to nothing.
    for cluster_name in sorted(set(cluster_names or ()) - set(config.clusters)):
        logger.warning(f"[{cluster_name}] unknown cluster, nothing to check")

    start = None if time_interval is None else datetime.now(tz=UTC) - time_interval
    window = f"submitted since {start}" if start is not None else "in database"

    with config.db.session() as sess:
        # Grouped by raw GPU name, which is what makes an alert actionable. Every column
        # read is in ix_slurm_jobs_submit, so a windowed run stays index-only.
        counts = sess.exec(
            _unharmonized(
                start,
                cluster_names,
                SlurmClusterDB.name,
                SlurmJobDB.allocated_gpu_type,
                func.count(col(SlurmJobDB.id)).label("jobs"),
            ).group_by(col(SlurmClusterDB.name), col(SlurmJobDB.allocated_gpu_type))
        ).all()

        # Biggest offender first, per cluster; the name breaks ties so alerts stay
        # comparable from one run to the next.
        for cluster_name, gpu_type, nb_jobs in sorted(
            counts, key=lambda row: (row[0], -row[2], row[1] or "")
        ):
            # Unordered on purpose: with an ORDER BY, LIMIT cannot stop before the
            # whole group has been examined, which costs a scan per group. Examples
            # are examples.
            examples = sess.exec(
                _unharmonized(start, cluster_names, SlurmJobDB.job_id)
                .where(
                    SlurmClusterDB.name == cluster_name,
                    col(SlurmJobDB.allocated_gpu_type).is_(None)
                    if gpu_type is None
                    else SlurmJobDB.allocated_gpu_type == gpu_type,
                )
                .limit(report_limit)
            ).all()
            subject = (
                "no allocated_gpu_type"
                if gpu_type is None
                else f"allocated_gpu_type '{gpu_type}'"
            )
            logger.error(
                f"[{cluster_name}] {subject}: {nb_jobs} GPU job{'s' if nb_jobs > 1 else ''} "
                f"{window} have no harmonized GPU type, "
                f"e.g. job_id {', '.join(str(job_id) for job_id in examples)}"
            )

    return not counts


@dataclass
class HarmonizedGpuTypeCheck(HealthCheck):
    """Health check for GPU jobs without a harmonized GPU type."""

    time_interval: timedelta | None = timedelta(days=1)
    report_limit: int = 5
    cluster_names: list[str] | None = None

    def check(self) -> CheckResult:
        if check_harmonized_gpu_types(
            time_interval=self.time_interval,
            report_limit=self.report_limit,
            cluster_names=self.cluster_names,
        ):
            return self.ok()
        else:
            return self.fail()
