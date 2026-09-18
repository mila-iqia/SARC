import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlmodel import col, func, select

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB

logger = logging.getLogger(__name__)


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

    ok = True

    # A name that is not a configured cluster would silently narrow the check to nothing.
    for cluster_name in sorted(set(cluster_names or ()) - set(config.clusters)):
        logger.error(f"[{cluster_name}] unknown cluster, nothing to check")
        ok = False

    start = None if time_interval is None else datetime.now(tz=UTC) - time_interval
    window = f"submitted since {start}" if start is not None else "in database"

    # A GPU job whose raw GPU name was not harmonized.
    # Jobs with no `allocated_gpu_type` at all are left out: they are repaired by
    # re-running the node->GPU inference, not by fixing a mapping.
    conditions = [
        col(SlurmJobDB.allocated_gres_gpu) > 0,
        col(SlurmJobDB.allocated_gpu_type).is_not(None),
        col(SlurmJobDB.harmonized_gpu_type).is_(None),
    ]
    if start is not None:
        conditions.append(SlurmJobDB.submit_time >= start)
    if cluster_names:
        conditions.append(col(SlurmClusterDB.name).in_(cluster_names))

    nb_jobs = func.count(col(SlurmJobDB.id)).label("jobs")
    with config.db.session() as sess:
        # Grouped by raw GPU name, and the examples taken in the same pass: a separate
        # `ORDER BY submit_time DESC LIMIT n` per group would walk the submit_time index backwards
        # until it has filled the limit, so a group whose newest job is old would cost a scan back to it.
        query = (
            select(
                SlurmClusterDB.name,
                SlurmJobDB.allocated_gpu_type,
                nb_jobs,
                func.array_agg(
                    aggregate_order_by(
                        # value collected
                        col(SlurmJobDB.job_id),
                        # order by these columns
                        col(SlurmJobDB.submit_time).desc(),
                        col(SlurmJobDB.job_id).desc(),
                    )
                )[1:report_limit].label("examples"),
            )
            .select_from(SlurmJobDB)
            .join(SlurmClusterDB, col(SlurmJobDB.cluster_id) == col(SlurmClusterDB.id))
            .where(*conditions)
            .group_by(col(SlurmClusterDB.name), col(SlurmJobDB.allocated_gpu_type))
            # Biggest offender first, per cluster.
            .order_by(
                col(SlurmClusterDB.name),
                nb_jobs.desc(),
                col(SlurmJobDB.allocated_gpu_type),
            )
        )
        groups = sess.exec(query).all()

    for cluster_name, gpu_type, jobs, examples in groups:
        logger.error(
            f"[{cluster_name}] allocated_gpu_type '{gpu_type}': "
            f"{jobs} GPU job{'s' if jobs > 1 else ''} {window} "
            f"{'have' if jobs > 1 else 'has'} no harmonized GPU type, "
            f"e.g. job_id {', '.join(str(job_id) for job_id in examples)}"
        )

    return ok and not groups


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
