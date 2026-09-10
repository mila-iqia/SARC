import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlmodel import col, func, select

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticsFetchDateDB, SlurmJobDB
from sarc.db.runstate import get_parsed_date

logger = logging.getLogger(__name__)


def check_prometheus_stats_coverage(
    time_interval: timedelta = timedelta(days=1),
    min_coverage: float = 0.5,
    cluster_names: list[str] | None = None,
) -> bool:
    """
    Check that enough of the jobs recently parsed for Prometheus stats actually got some stats.
    Log an alert for each cluster whose coverage is below `min_coverage`.

    The population is the fetch attempts recorded in `jobstatistics_fetchdate`, windowed
    on `fetch_date`.

    Parameters
    ----------
    time_interval: timedelta
        Width of the window of fetch attempts to check. Default is 1 day.
    min_coverage: float
        Alert below this ratio of attempts that yielded stats. Default is 0.5.
    cluster_names: list
        Clusters to check. Attempts from other clusters are ignored.
        If empty (or not specified), check every cluster configured with a
        `prometheus_url`, which is exactly the set `sarc fetch prometheus` scrapes.
        Either way, a cluster with no attempt at all in the window is an alert of its
        own: that is what a stopped fetch looks like.

    Returns
    -------
    bool
        True if check succeeds, False otherwise.
    """
    from sarc.config import config

    if time_interval <= timedelta(0):
        logger.error(
            f"Invalid time_interval (must be > 0) for Prometheus stats coverage: {time_interval}"
        )
        return False
    if not 0 <= min_coverage <= 1:
        logger.error(
            f"Invalid min_coverage (must be in [0, 1]) for Prometheus stats coverage: {min_coverage}"
        )
        return False

    with config.db.session() as sess:
        # `jobstatistic_id` is only filled in by `sarc parse prometheus`, so end the window
        # at the last parsed date rather than now: attempts already fetched but not yet
        # parsed would all read as uncovered.
        end = get_parsed_date(sess, "prometheus")
        if end is None:
            logger.error(
                "No parsed date for Prometheus, cannot check stats coverage: "
                "has `sarc parse prometheus` ever run?"
            )
            return False
        start = end - time_interval

        # Sorted either way, so the alerts always come out in cluster order.
        expected = sorted(
            cluster_names
            or (
                cluster.name
                for cluster in config.clusters.values()
                if cluster.prometheus_url and cluster.name
            )
        )
        if not expected:
            logger.error(
                "No cluster configured with a prometheus_url, nothing to check"
            )
            return False

        now = datetime.now(tz=UTC)
        if end < now - time_interval:
            logger.warning(
                f"Latest Prometheus data parsed at {end}: stats coverage checked on a stale window"
            )

        query = (
            select(
                SlurmClusterDB.name,
                func.count(col(JobStatisticsFetchDateDB.id)).label("attempts"),
                # count(<column>) ignores NULLs, so this counts the attempts that got stats.
                func.count(col(JobStatisticsFetchDateDB.jobstatistic_id)).label(
                    "covered"
                ),
            )
            .select_from(JobStatisticsFetchDateDB)
            .join(
                SlurmJobDB, col(JobStatisticsFetchDateDB.job_id) == col(SlurmJobDB.id)
            )
            .join(SlurmClusterDB, col(SlurmJobDB.cluster_id) == col(SlurmClusterDB.id))
            .where(
                col(SlurmClusterDB.name).in_(expected),
                col(JobStatisticsFetchDateDB.fetch_date) >= start,
                # The parsed date is read back from the cache entry filename, truncated
                # to the millisecond (`Cache.create_entry`), while `fetch_date` keeps
                # the microseconds it was written with. Bound on the next millisecond,
                # or the very run `end` comes from falls outside its own window.
                col(JobStatisticsFetchDateDB.fetch_date)
                < end + timedelta(milliseconds=1),
            )
            .group_by(col(SlurmClusterDB.name))
        )
        counts = {
            cluster_name: (attempts, covered)
            for cluster_name, attempts, covered in sess.exec(query).all()
        }

    ok = True
    for cluster_name in expected:
        if cluster_name not in counts:
            logger.error(
                f"[{cluster_name}] no Prometheus fetch attempt in [{start}, {end}]: "
                "Prometheus scraping may have stopped"
            )
            ok = False
            continue
        attempts, covered = counts[cluster_name]
        coverage = covered / attempts
        if coverage < min_coverage:
            logger.error(
                f"[{cluster_name}] insufficient Prometheus stats coverage: "
                f"{covered} of {attempts} jobs fetched in [{start}, {end}] got stats "
                f"({coverage:.1%}); minimum required: {min_coverage:.1%}"
            )
            ok = False

    return ok


@dataclass
class PrometheusStatsCoverageCheck(HealthCheck):
    """Health check for Prometheus stats coverage."""

    time_interval: timedelta = timedelta(days=1)
    min_coverage: float = 0.5
    cluster_names: list[str] | None = None

    def check(self) -> CheckResult:
        if check_prometheus_stats_coverage(
            time_interval=self.time_interval,
            min_coverage=self.min_coverage,
            cluster_names=self.cluster_names,
        ):
            return self.ok()
        else:
            return self.fail()
