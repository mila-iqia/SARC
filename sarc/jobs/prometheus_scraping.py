import json
import logging
from datetime import UTC, datetime
from typing import Iterable

from tqdm import tqdm

from sarc.cache import Cache
from sarc.client.job import SlurmJob, _jobs_collection
from sarc.config import ClusterConfig, config
from sarc.core.models.runstate import get_parsed_date, set_parsed_date
from sarc.core.scraping.jobs_utils import _time_auto_first_date, parse_auto_intervals
from sarc.jobs.series import (
    JOB_STATISTICS_METRIC_NAMES,
    compute_job_statistics,
    get_job_time_series_data,
)
from sarc.traces import trace_decorator

logger = logging.getLogger(__name__)

AUTO_END_FIELD = "end_time_prometheus"


def parse_prometheus_auto_intervals(
    cluster_name: str, minutes: int, max_intervals: int | None
) -> list[tuple[datetime, datetime]]:
    """
    When scraping with auto intervals, we don't want to scrape Prometheus metrics
    after end_time_sacct.
    """
    end_time_sacct = _time_auto_first_date(cluster_name, "end_time_sacct")
    return parse_auto_intervals(
        cluster_name, AUTO_END_FIELD, minutes, max_intervals, end=end_time_sacct
    )


def get_jobs_in_scraped_period(
    cluster_name: str, start: datetime, end: datetime
) -> Iterable[SlurmJob]:
    """
    Get jobs whom latest scraped period instersects with given [start, end].

    There is an intersection if:
    start < latest_scraped_end and latest_scraped_start < end

    NB: We check "<" instead of "<=" because
    we want intervals to have an overlap,
    not just 1 common border date.
    """
    query = {
        "cluster_name": cluster_name,
        "latest_scraped_start": {"$lt": end},
        "latest_scraped_end": {"$gt": start},
    }
    coll_jobs = config().mongo.database_instance.jobs
    nb_jobs = coll_jobs.count_documents(query)
    yield from tqdm(
        _jobs_collection().find_by(query), total=nb_jobs, desc="Prometheus metrics"
    )


@trace_decorator()
def fetch_prometheus(cluster: ClusterConfig, start: datetime, end: datetime) -> None:
    """
    Fetch Prometheus metrics for jobs from start to end on the specified cluster.
    """
    if cluster.name is None:
        logger.error("cluster name not set, can't fetch")
        return
    nb_jobs = 0
    cache = Cache("prometheus")
    with cache.create_entry(datetime.now(UTC)) as ce:
        for entry in get_jobs_in_scraped_period(cluster.name, start, end):
            nb_jobs += 1
            raw_prom_data = get_job_time_series_data(
                job=entry, metric=JOB_STATISTICS_METRIC_NAMES, max_points=10_000
            )
            ce.add_value(
                f"{entry.cluster_name}${entry.job_id}${entry.submit_time.isoformat('seconds')}",
                json.dumps(raw_prom_data).encode("utf-8"),
            )
    logger.info(f"Fetched Prometheus metrics for {nb_jobs} jobs.")


@trace_decorator()
def parse_prometheus(since: datetime | None, update_parsed_date: bool) -> None:
    cache = Cache("prometheus")
    collection = _jobs_collection()
    db = config().mongo.database_instance
    if since is None:
        since = get_parsed_date(db, "prometheus")

    nb_jobs = 0
    for ce in cache.read_from(from_time=since):
        error = False
        logger.info(
            f"Parsing prometheus data from cache entry: {ce.get_entry_datetime().isoformat('milliseconds')}"
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
            entry = collection.find_one_by(
                {
                    "cluster_name": cluster_name,
                    "job_id": job_id,
                    "submit_time": submit_time,
                }
            )
            if entry is None:
                logger.error("Could not find job for %s", key)
                error = True
                continue
            data = json.loads(value.decode("utf-8"))
            gpu_type = data[0]["metric"]["gpu_type"]
            if gpu_type is not None:
                entry.allocated.gpu_type = (
                    cluster.harmonize_gpu_from_nodes(entry.nodes, gpu_type) or gpu_type
                )
            statistics = compute_job_statistics(entry, data)
            if not statistics.empty():
                entry.stored_statistics = statistics
                entry.save()

        logger.info(f"Saved Prometheus metrics for {nb_jobs} jobs.")

        if update_parsed_date and not error:
            logger.info(f"Set parsed_dates for jobs to {ce.get_entry_datetime()}.")
            set_parsed_date(db, "prometheus", ce.get_entry_datetime())
