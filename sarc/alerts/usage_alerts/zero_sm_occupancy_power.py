import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.orm import aliased
from sqlmodel import and_, col, func, select

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB

logger = logging.getLogger(__name__)

OCCUPANCY_STAT = "gpu_sm_occupancy"
POWER_STAT = "gpu_power"

# `slurm_job_power_gpu` is exposed in mW by the exporter
# (github.com/guilbaults/slurm-job-exporter) and sarc stores it untouched,
# so the stored statistics are mW too.
WATTS_PER_MW = 1000.0


def check_zero_sm_occupancy_power(
    time_interval: timedelta | None = timedelta(days=7),
    min_power_watts: float = 100.0,
    report_limit: int = 5,
    cluster_names: list[str] | None = None,
) -> bool:
    """
    Check that no job drew significant GPU power while its SM occupancy measured zero.
    Log an alert per cluster with jobs whose `gpu_sm_occupancy` max is 0 while their
    `gpu_power` max is at least `min_power_watts`: a GPU burning watts without any
    SM activity is most likely a broken SM occupancy measurement.

    Zero is trustworthy on both sides: the stats pipeline drops DCGM BLANK/NaN
    samples before computing the statistics, so a stored 0 occupancy means real
    zero samples (a fully filtered series would read NULL, not 0).

    Parameters
    ----------
    time_interval: timedelta
        Width of the window, ending now and taken on `submit_time`. Default is 7
        days: Prometheus stats are only fetched after a job ends, so the window
        must stay open long past submission to cover jobs that run for days.
        If None, all jobs are checked.
    min_power_watts: float
        Alert from this per-GPU power draw up, in watts (compared against the
        mW-stored `gpu_power.max`). Default is 100.
    report_limit: int
        How many example jobs to name per alert, newest first. Default is 5.
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
            f"Invalid time_interval (must be > 0) for zero-occupancy power: {time_interval}"
        )
        return False
    if min_power_watts <= 0:
        logger.error(
            f"Invalid min_power_watts (must be > 0) for zero-occupancy power: {min_power_watts}"
        )
        return False
    if report_limit < 1:
        logger.error(
            f"Invalid report_limit (must be > 0) for zero-occupancy power: {report_limit}"
        )
        return False

    ok = True

    # A name that is not a configured cluster would silently narrow the check to nothing.
    for cluster_name in sorted(set(cluster_names or ()) - set(config.clusters)):
        logger.error(f"[{cluster_name}] unknown cluster, nothing to check")
        ok = False

    start = None if time_interval is None else datetime.now(tz=UTC) - time_interval
    window = f"submitted since {start}" if start is not None else "in database"
    min_power_mw = min_power_watts * WATTS_PER_MW

    # The two statistics of a job, as separate joins: one row per (job, name).
    occupancy = aliased(JobStatisticDB)
    power = aliased(JobStatisticDB)

    conditions = [col(occupancy.max) == 0, col(power.max) >= min_power_mw]
    if start is not None:
        conditions.append(SlurmJobDB.submit_time >= start)
    if cluster_names:
        conditions.append(col(SlurmClusterDB.name).in_(cluster_names))

    nb_jobs = func.count(col(SlurmJobDB.id)).label("jobs")
    with config.db.session() as sess:
        # Examples carry their power max: the watt value is what makes each job
        # worth a look, and it costs nothing to aggregate in the same pass.
        query = (
            select(
                SlurmClusterDB.name,
                nb_jobs,
                func.array_agg(
                    aggregate_order_by(
                        # value collected
                        func.concat(
                            col(SlurmJobDB.job_id), " (", col(power.max), " mW)"
                        ),
                        # order by these columns
                        col(SlurmJobDB.submit_time).desc(),
                        col(SlurmJobDB.job_id).desc(),
                    )
                )[1:report_limit].label("examples"),
            )
            .select_from(SlurmJobDB)
            .join(SlurmClusterDB, col(SlurmJobDB.cluster_id) == col(SlurmClusterDB.id))
            .join(
                occupancy,
                and_(
                    col(occupancy.job_id) == col(SlurmJobDB.id),
                    col(occupancy.name) == OCCUPANCY_STAT,
                ),
            )
            .join(
                power,
                and_(
                    col(power.job_id) == col(SlurmJobDB.id),
                    col(power.name) == POWER_STAT,
                ),
            )
            .where(*conditions)
            .group_by(col(SlurmClusterDB.name))
            .order_by(col(SlurmClusterDB.name))
        )
        groups = sess.exec(query).all()

    for cluster_name, jobs, examples in groups:
        logger.error(
            f"[{cluster_name}] {jobs} job{'s' if jobs > 1 else ''} {window} "
            f"{'have' if jobs > 1 else 'has'} gpu_sm_occupancy.max = 0 but "
            f"gpu_power.max >= {min_power_watts:g} W ({min_power_mw:g} mW): "
            f"e.g. job_id {'; '.join(examples)}"
        )

    return ok and not groups


@dataclass
class ZeroSmOccupancyPowerCheck(HealthCheck):
    """Health check for GPU jobs drawing power with zero SM occupancy."""

    time_interval: timedelta | None = timedelta(days=7)
    min_power_watts: float = 100.0
    report_limit: int = 5
    cluster_names: list[str] | None = None

    def check(self) -> CheckResult:
        if check_zero_sm_occupancy_power(
            time_interval=self.time_interval,
            min_power_watts=self.min_power_watts,
            report_limit=self.report_limit,
            cluster_names=self.cluster_names,
        ):
            return self.ok()
        else:
            return self.fail()
