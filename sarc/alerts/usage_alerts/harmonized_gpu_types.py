import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlmodel import col, func, select

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB

logger = logging.getLogger(__name__)


def _unharmonized(start: datetime | None):
    """Conditions matching a GPU job whose raw GPU name was not harmonized.

    `allocated_gres_gpu > 0` is the first conjunct of the `ELIGIBILITY` predicate in
    `sarc/db/job_series.py`; the second one is the `gpurgudb` row reached through
    `harmonized_gpu_type`. So these jobs drop out of the whole `/dash` scope, with
    NULL RGU, cost and waste.

    Jobs with no `allocated_gpu_type` at all are left out: sacct reported no GPU name
    for them, so there is nothing to harmonize -- they are repaired by re-running the
    node->GPU inference, not by fixing a mapping.
    """
    conditions = [
        col(SlurmJobDB.allocated_gres_gpu) > 0,
        col(SlurmJobDB.allocated_gpu_type).is_not(None),
        col(SlurmJobDB.harmonized_gpu_type).is_(None),
    ]
    if start is not None:
        conditions.append(SlurmJobDB.submit_time >= start)
    return conditions


def check_harmonized_gpu_types(
    time_interval: timedelta | None = timedelta(days=1),
    report_limit: int = 5,
    cluster_names: list[str] | None = None,
) -> bool:
    """
    Check that GPU jobs whose GPU name is known have a harmonized one.
    Log an alert per cluster and `allocated_gpu_type` that has jobs without harmonized GPU name.

    Parameters
    ----------
    time_interval: timedelta
        Width of the window, ending now and taken on `submit_time`. Default is 1 day.
        If None, all jobs are checked.
    report_limit: int
        How many example job ids to name per alert, newest first. Default is 5.
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
        # Grouped by raw GPU name, which is what makes an alert actionable. Every
        # column read is in ix_slurm_jobs_submit, so a windowed run stays index-only.
        counts_query = (
            select(
                SlurmClusterDB.id,
                SlurmClusterDB.name,
                SlurmJobDB.allocated_gpu_type,
                func.count(col(SlurmJobDB.id)).label("jobs"),
            )
            .select_from(SlurmJobDB)
            .join(SlurmClusterDB, col(SlurmJobDB.cluster_id) == col(SlurmClusterDB.id))
            .where(*_unharmonized(start))
            .group_by(
                col(SlurmClusterDB.id),
                col(SlurmClusterDB.name),
                col(SlurmJobDB.allocated_gpu_type),
            )
        )
        if cluster_names:
            counts_query = counts_query.where(
                col(SlurmClusterDB.name).in_(cluster_names)
            )
        counts = sess.exec(counts_query).all()

        # Biggest offender first, per cluster.
        for cluster_id, cluster_name, gpu_type, nb_jobs in sorted(
            counts, key=lambda row: (row[1], -row[3], row[2])
        ):
            # Newest first, and the cluster pinned by id rather than joined: only
            # then can the planner prove the scan is already in submit_time order
            # and stop at LIMIT instead of sorting the whole group. Through the
            # join it sorts, which costs a full scan per group.
            examples = sess.exec(
                select(SlurmJobDB.job_id)
                .where(
                    *_unharmonized(start),
                    SlurmJobDB.cluster_id == cluster_id,
                    SlurmJobDB.allocated_gpu_type == gpu_type,
                )
                .order_by(col(SlurmJobDB.submit_time).desc())
                .limit(report_limit)
            ).all()
            logger.error(
                f"[{cluster_name}] allocated_gpu_type '{gpu_type}': "
                f"{nb_jobs} GPU job{'s' if nb_jobs > 1 else ''} {window} "
                f"{'have' if nb_jobs > 1 else 'has'} no harmonized GPU type, "
                f"e.g. job_id {', '.join(str(job_id) for job_id in examples)}"
            )

    return not counts


@dataclass
class HarmonizedGpuTypeCheck(HealthCheck):
    """Health check for GPU jobs whose GPU name was not harmonized."""

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
