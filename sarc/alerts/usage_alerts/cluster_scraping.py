import logging
import math
import statistics
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import sqlalchemy
from sqlmodel import case, col, func, select

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB

logger = logging.getLogger(__name__)


def check_nb_jobs_per_cluster_per_time(
    time_interval: timedelta | None = timedelta(days=7),
    time_unit: timedelta = timedelta(days=1),
    cluster_names: list[str] | None = None,
    nb_stddev: float = 2.0,
    verbose: bool = False,
) -> bool:
    """
    Check if we have scraped enough jobs per time unit per cluster on given time interval.
    Log an alert for each cluster where number of jobs per time unit is lower than a limit
    computed using mean and standard deviation statistics from this cluster.

    Parameters
    ----------
    time_interval: timedelta
        If given, only jobs which ran in [now - time_interval, now] will be used for checking.
        Default is last 7 days.
        If None, all jobs are used.
    time_unit: timedelta
        Time unit in which we must check cluster usage through time_interval. Default is 1 day.
    cluster_names: list
        Optional list of clusters to check.
        If empty (or not specified), use all clusters available among jobs retrieved with time_interval.
    nb_stddev: float
        Amount of standard deviation to remove from average statistics to compute checking threshold.
        For each cluster, threshold is computed as:
        max(0, average - nb_stddev * stddev)
    verbose: bool
        If True, print supplementary info about clusters statistics.

    Returns
    -------
    bool
        True if check succeeds, False otherwise
    """
    from sarc.config import config

    if time_unit.total_seconds() <= 0:
        logger.error(
            f"Invalid time unit (must be > 0) for cluster usage checking: {time_unit}"
        )
        return False
    if nb_stddev < 0:
        logger.error(
            f"Invalid nb_stddev (must be >= 0) for cluster usage checking: {nb_stddev}"
        )
        return False

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

    with config().db.session() as sess:
        if not sess.exec(select(func.count(SlurmJobDB.id))).one():
            logger.error("No jobs in database.")
            return False

        # Determine [start, end] bounds for frame iteration.
        if time_interval is None:
            start, end = sess.exec(
                select(func.min(eff_start), func.max(eff_end))
            ).one_or_none()
        else:
            end = datetime.now(tz=UTC)
            start = end - time_interval

        # Pre-compute all timestamps in Python,
        # to avoid missing any frames with no jobs
        timestamps: list[datetime] = []
        frame_start_py = start
        while frame_start_py < end:
            timestamps.append(frame_start_py)
            frame_start_py += time_unit

        # Use a Postgresql query: time frange + join
        time_unit_sql = sqlalchemy.literal(time_unit, type_=sqlalchemy.Interval)

        frames = select(
            func.generate_series(start, end, time_unit_sql).label("frame_start")
        ).subquery()

        # NB: Since we iterate over frames, we want to capture point jobs located at frame bounds,
        # e.g. PENDING jobs which run in [submit_time, submit_time]. So, we grab everything in
        # [frame_start included, frame_end excluded), instead of (frame_start excluded, frame_end excluded)
        query = (
            select(SlurmClusterDB.name, frames.c.frame_start, func.count(SlurmJobDB.id))
            .select_from(frames)
            .join(
                SlurmJobDB,
                (eff_start < (frames.c.frame_start + time_unit_sql))
                & (eff_end >= frames.c.frame_start),
            )
            .join(SlurmClusterDB, SlurmJobDB.cluster_id == SlurmClusterDB.id)
            .group_by(SlurmClusterDB.name, frames.c.frame_start)
        )

        cluster_counts: dict[str, dict[datetime, int]] = {}
        # For each frame, count jobs per cluster.
        for cluster_name, frame_start_db, count in sess.exec(query):
            cluster_counts.setdefault(cluster_name, {})[frame_start_db] = count

    # Determine which clusters to report on.
    if cluster_names:
        sorted_clusters = sorted(cluster_names)
    else:
        sorted_clusters = sorted(cluster_counts.keys())

    ok = True
    for cluster_name in sorted_clusters:
        counts = [cluster_counts.get(cluster_name, {}).get(ts, 0) for ts in timestamps]
        avg = statistics.mean(counts)
        stddev = statistics.stdev(counts) if len(counts) > 1 else math.nan
        threshold = 0.0 if math.isnan(stddev) else max(0.0, avg - nb_stddev * stddev)

        if verbose:
            print(f"[{cluster_name}]", file=sys.stderr)  # noqa: T201
            for ts, c in zip(timestamps, counts):
                print(f"  {ts}  {c}", file=sys.stderr)  # noqa: T201
            print(f"avg {avg}, stddev {stddev}, threshold {threshold}", file=sys.stderr)  # noqa: T201
            print(file=sys.stderr)  # noqa: T201

        if threshold == 0:
            # If threshold is zero, no check can be done, as jobs count will be always >= 0.
            # Instead, we log a general alert.
            msg = f"[{cluster_name}] threshold 0 ({avg} - {nb_stddev} * {stddev})."
            if len(timestamps) == 1:
                msg += (
                    f" Only 1 timestamp found. Either time_interval ({time_interval}) is too short, "
                    f"or this cluster should not be currently checked"
                )
            else:
                msg += (
                    f" Either nb_stddev is too high, time_interval ({time_interval}) is too short, "
                    f"or this cluster should not be currently checked"
                )
            logger.error(msg)
            ok = False
        else:
            for ts, c in zip(timestamps, counts):
                if c < threshold:
                    logger.error(
                        f"[{cluster_name}][{ts}] "
                        f"insufficient cluster scraping: {c} jobs / cluster / time unit; "
                        f"minimum required for this cluster: {threshold} ({avg} - {nb_stddev} * {stddev}); "
                        f"time unit: {time_unit}"
                    )
                    ok = False

    return ok


@dataclass
class ClusterScrapingCheck(HealthCheck):
    """Health check for cluster scraping"""

    time_interval: timedelta | None = timedelta(days=7)
    time_unit: timedelta = timedelta(days=1)
    cluster_names: list[str] | None = None
    nb_stddev: float = 2.0
    verbose: bool = False

    def check(self) -> CheckResult:
        if check_nb_jobs_per_cluster_per_time(
            time_interval=self.time_interval,
            time_unit=self.time_unit,
            cluster_names=self.cluster_names,
            nb_stddev=self.nb_stddev,
            verbose=self.verbose,
        ):
            return self.ok()
        else:
            return self.fail()
