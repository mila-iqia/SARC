import json
import logging
from datetime import UTC, datetime

from sqlmodel import Session, col, select

from sarc.cache import Cache, CacheEntry
from sarc.config import ClusterConfig, config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.runstate import get_parsed_date, set_parsed_date
from sarc.jobs.series import (
    JOB_STATISTICS_METRIC_NAMES,
    compute_job_statistics,
    get_job_time_series_data,
)
from sarc.models.job import SlurmState
from sarc.traces import trace_decorator

logger = logging.getLogger(__name__)


@trace_decorator()
def fetch_prometheus(
    sess: Session, cluster: ClusterConfig, after: datetime | None, max_jobs: int | None
) -> None:
    """
    Fetch Prometheus metrics for jobs on the specified cluster.
    """
    if cluster.name is None:
        logger.error("cluster name not set, can't fetch")
        return
    cluster_id = SlurmClusterDB.id_by_name(sess, cluster.name)
    if cluster_id is None:
        logger.error("Unknown cluster %, skipping cluster", cluster.name)
    subquery = (
        select(JobStatisticDB.job_id)
        .where(JobStatisticDB.job_id == SlurmJobDB.id)
        .exists()
    )
    query = select(SlurmJobDB).where(
        SlurmJobDB.cluster_id == cluster_id,
        SlurmJobDB.elapsed_time != 0,
        SlurmJobDB.job_state != SlurmState.RUNNING,
        ~subquery,  # not users that have statistics
    )
    if after is not None:
        query = query.where(SlurmJobDB.submit_time >= after)
    if max_jobs is not None:
        query = query.order_by(col(SlurmJobDB.submit_time)).limit(max_jobs)
    nb_jobs = 0
    cache = Cache("prometheus")
    with cache.create_entry(datetime.now(UTC)) as ce:
        for entry in sess.exec(query):
            raw_prom_data = get_job_time_series_data(
                job=entry, metric=JOB_STATISTICS_METRIC_NAMES, max_points=10_000
            )
            if raw_prom_data == []:
                continue
            nb_jobs += 1
            ce.add_value(
                f"{entry.cluster.name}${entry.job_id}${entry.submit_time.isoformat(timespec='seconds')}",
                json.dumps(raw_prom_data).encode("utf-8"),
            )
    logger.info(f"Fetched Prometheus metrics for {nb_jobs} jobs.")


@trace_decorator()
def parse_prometheus(since: datetime | None, update_parsed_date: bool) -> None:
    cache = Cache("prometheus")
    with config("scraping").db.session() as sess:
        if since is None:
            since = get_parsed_date(sess, "prometheus")

        assert since is not None
        for ce in cache.read_from(from_time=since):
            error = parse_prometheus_ce(sess, ce)
            if update_parsed_date and not error:
                logger.info(f"Set parsed_dates for jobs to {ce.get_entry_datetime()}.")
                set_parsed_date(sess, "prometheus", ce.get_entry_datetime())
            sess.commit()


def parse_prometheus_ce(sess: Session, ce: CacheEntry) -> bool:
    error = False
    nb_jobs = 0

    logger.info(
        f"Parsing prometheus data from cache entry: {ce.get_entry_datetime().isoformat(timespec='milliseconds')}"
    )
    for key, value in ce.items():
        nb_jobs += 1
        cluster_name, job_id_str, submit_time_str = key.split("$")
        cluster = config("scraping").clusters.get(cluster_name, None)
        if cluster is None:
            logger.error("Could not find cluster '%s' in config", cluster_name)
            error = True
            continue
        job_id = int(job_id_str)
        submit_time = datetime.fromisoformat(submit_time_str)
        data = json.loads(value.decode("utf-8"))
        if data == []:
            logger.warning(
                f"Empty data found for job {job_id} on cluster {cluster_name} (submit_time {submit_time}), skipping cache entry"
            )
            continue
        cluster_id = SlurmClusterDB.id_by_name(sess, cluster_name)
        if cluster_id is None:
            logger.error("Unknown cluster name %s in entry key %s", cluster_name, key)
            error = True
            continue
        entry = SlurmJobDB.by_ref(sess, cluster_id, job_id, submit_time)
        if entry is None:
            logger.error("Could not find job for %s", key)
            error = True
            continue
        gpu_type = data[0]["metric"].get("gpu_type", None)
        if gpu_type is not None:
            entry.allocated_gpu_type = (
                cluster.harmonize_gpu_from_nodes(entry.nodes, gpu_type) or gpu_type
            )
        statistics = compute_job_statistics(entry, data)
        if len(statistics) != 0:
            entry.statistics = statistics

    logger.info(f"Saved Prometheus metrics for {nb_jobs} jobs.")
    return error
