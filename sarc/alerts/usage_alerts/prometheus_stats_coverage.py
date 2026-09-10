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
        Clusters to check. Attempts from other clusters are ignored, and an alert is
        logged for a listed cluster with no attempt at all.
        If empty (or not specified), check every cluster present in the window.

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
                col(JobStatisticsFetchDateDB.fetch_date) >= start,
                col(JobStatisticsFetchDateDB.fetch_date) <= end,
            )
            .group_by(col(SlurmClusterDB.name))
            .order_by(col(SlurmClusterDB.name))
        )
        if cluster_names:
            query = query.where(col(SlurmClusterDB.name).in_(cluster_names))
        rows = sess.exec(query).all()

    ok = True
    for cluster_name, attempts, covered in rows:
        coverage = covered / attempts
        if coverage < min_coverage:
            logger.error(
                f"[{cluster_name}] insufficient Prometheus stats coverage: "
                f"{covered} of {attempts} jobs fetched in [{start}, {end}] got stats "
                f"({coverage:.1%}); minimum required: {min_coverage:.1%}"
            )
            ok = False

    if not rows:
        logger.error(
            f"No Prometheus fetch attempt in [{start}, {end}]: "
            "nothing to check, Prometheus scraping may have stopped"
        )
        ok = False

    seen = {cluster_name for cluster_name, _, _ in rows}
    for cluster_name in cluster_names or ():
        if cluster_name not in seen:
            logger.error(
                f"[{cluster_name}] no Prometheus fetch attempt in [{start}, {end}]"
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
