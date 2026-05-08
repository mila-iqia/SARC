import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import sqlalchemy
from sqlmodel import case, col, func, select

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.db.job import SlurmJobDB

logger = logging.getLogger(__name__)


def check_gpu_util_per_user(
    threshold: timedelta | None = None,
    time_interval: timedelta | None = timedelta(days=7),
    minimum_runtime: timedelta | None = timedelta(minutes=5),
) -> bool:
    """
    Check if users have enough utilization of GPUs.
    Log an alert for each user if average GPU-util of user jobs
    in time interval is lower than a given threshold.

    For a given user job, GPU-util is computed as
    gpu_utilization * gpu_equivalent_cost
    (with gpu_equivalent_cost as elapsed_time * allocated.gres_gpu).

    Parameters
    ----------
    threshold: timedelta
        Minimum value for average GPU-util expected per user.
        We assume GPU-util is expressed in GPU-seconds,
        thus threshold can be expressed with a timedelta.
    time_interval
        If given, only jobs which ran in [now - time_interval, now] will be used for checking.
        Default is last 7 days.
        If None, all jobs are used.
    minimum_runtime
        If given, only jobs which ran at least for this minimum runtime will be used for checking.
        Default is 5 minutes.
        If None, set to 0.

    Returns
    -------
    bool
        True if check succeeds, False otherwise.
    """
    from sarc.config import config
    from sarc.db.job import JobStatisticDB

    if threshold is None:
        logger.error("No threshold specified.")
        return False

    # Parse minimum_runtime
    if minimum_runtime is None:
        minimum_runtime = timedelta(seconds=0)

    # Effective start_time: fall back to submit_time when the job never started.
    eff_start = func.coalesce(SlurmJobDB.start_time, SlurmJobDB.submit_time)
    # Effective end_time: end_time if recorded; else `start_time + elapsed_time` for jobs that started
    # (still running or transitioning); else submit_time, so jobs that never ran
    # (e.g. PENDING) collapse to a point at submission.
    eff_end = case(
        (col(SlurmJobDB.end_time).is_not(None), SlurmJobDB.end_time),
        (
            col(SlurmJobDB.start_time).is_not(None),
            SlurmJobDB.start_time
            + SlurmJobDB.elapsed_time
            * sqlalchemy.literal(timedelta(seconds=1), type_=sqlalchemy.Interval),
        ),
        else_=SlurmJobDB.submit_time,
    )

    ok = True
    with config().db.session() as sess:
        if not sess.exec(select(func.count(SlurmJobDB.id))).one():
            logger.error("No jobs in database.")
            return False

        # Determine [start, end] bounds for frame iteration.
        # We compute clip elapsed time if start and end are available,
        # so that minimum_runtime is compared to job running time in given interval.
        if time_interval is None:
            start, end = sess.exec(
                select(func.min(eff_start), func.max(eff_end))
            ).one_or_none()
            clipped_elapsed_time = SlurmJobDB.elapsed_time * sqlalchemy.literal(
                timedelta(seconds=1), type_=sqlalchemy.Interval
            )
        else:
            end = datetime.now(tz=UTC)
            start = end - time_interval
            clipped_elapsed_time = func.least(eff_end, end) - func.greatest(
                eff_start, start
            )

        # SQL query to compute average GPU-util per user.
        # GPU-util for a job = gpu_utilization * clipped_elapsed_time * allocated_gres_gpu.
        # Note: SlurmJobDB.statistics is a relation to JobStatisticDB.

        gpu_utilization = JobStatisticDB.median
        # Replace gpu_utilization > 1 with NULL (consistent with load_job_series replacing with NaN)
        gpu_utilization = case((gpu_utilization > 1.0, None), else_=gpu_utilization)

        clipped_elapsed_seconds = func.extract("epoch", clipped_elapsed_time)
        gpu_equivalent_cost = clipped_elapsed_seconds * SlurmJobDB.allocated_gres_gpu
        gpu_util = gpu_utilization * gpu_equivalent_cost

        query = (
            select(SlurmJobDB.user, func.avg(gpu_util).label("avg_gpu_util"))
            .join(
                JobStatisticDB,
                (JobStatisticDB.job_id == SlurmJobDB.id)
                & (JobStatisticDB.name == "gpu_utilization"),
                isouter=True,
            )
            .where(
                eff_start < end,
                eff_end > start,
                clipped_elapsed_time
                >= sqlalchemy.literal(minimum_runtime, type_=sqlalchemy.Interval),
                SlurmJobDB.allocated_gres_gpu > 0,
            )
            .group_by(SlurmJobDB.user)
        )

        for user, avg_gpu_util in sess.exec(query):
            if avg_gpu_util is None:
                logger.error(
                    f"[{user}] average gpu_util cannot be computed (no statistics found for matching jobs)."
                )
                ok = False
            elif avg_gpu_util < threshold.total_seconds():
                logger.error(
                    f"[{user}] insufficient average gpu_util: {avg_gpu_util} GPU-seconds; "
                    f"minimum required: {threshold} ({threshold.total_seconds()} GPU-seconds)"
                )
                ok = False

    return ok


@dataclass
class GpuUtilPerUserCheck(HealthCheck):
    """Health check for GPU-utilization per user."""

    threshold: timedelta | None = None  # ** required **
    time_interval: timedelta | None = timedelta(days=7)
    minimum_runtime: timedelta | None = timedelta(minutes=5)

    def check(self) -> CheckResult:
        if check_gpu_util_per_user(
            threshold=self.threshold,
            time_interval=self.time_interval,
            minimum_runtime=self.minimum_runtime,
        ):
            return self.ok()
        else:
            return self.fail()
