from datetime import datetime
import logging
import pickle
from tqdm import tqdm
from typing import Optional

from sarc.cache import Cache
from sarc.client.job import _jobs_collection
from sarc.config import ClusterConfig, UTC
from sarc.core.scraping.jobs_utils import (
    DATE_FORMAT_HOUR,
    parse_auto_intervals,
    parse_intervals,
    set_auto_end_time,
    SacctScraper,
    update_allocated_gpu_type_from_nodes,
)
from sarc.traces import using_trace


logger = logging.getLogger(__name__)


def get_jobs(cluster: ClusterConfig, start: datetime, end: datetime) -> list[dict]:
    scraper = SacctScraper(cluster, start, end)

    logger.info(
        f"Getting the sacct data for cluster {cluster.name}, time {start} to {end}..."
    )

    scraper.get_raw()

    jobs = [job for job in scraper]

    return jobs


def fetch_jobs(
    cluster_names: list[str],
    clusters: dict[str, ClusterConfig],
    unparsed_intervals: Optional[list[str]],
    auto_interval: Optional[int],
    with_cache: Optional[bool],
) -> None:
    """
    Fetch jobs and place the results in cache.

    Parameters:
        cluster_names           List of the names of the clusters on which we want
                                to fetch the jobs

        clusters                Dictionary linking a cluster name to the
                                associated cluster config

        unparsed_intervals      Intervals during which we want to fetch the jobs.
                                Expected format for each interval: <date-from>-<date-to>,
                                with <date-from> and <date-to> in format: YYYY-MM-DDTHH:mm
                                (e.g.: 2020-01-01T17:05-2020-01-01T18:00).
                                Dates will be interpreted as UTC.
                                Mutually exclusive with --auto_interval.

        auto_interval           Acquire jobs every <auto_interval> minutes since latest scraping date until now.
                                If <= 0, use only one interval since latest scraping date until now. Mutually
                                exclusive with --intervals.
    """

    auto_end_field = "end_time_sacct"  # Used to parse the intervals

    def _fetch_jobs(
        cluster_name: str,
        clusters: dict[str, ClusterConfig],
        auto_interval: Optional[int],
    ):
        # Fetch the jobs for each time interval
        for time_from, time_to in intervals:
            with using_trace(
                "FetchJobs",
                "acquire_cluster_data_from_time_interval",
                exception_types=(),
            ) as span:
                span.set_attribute("cluster_name", cluster_name)
                span.set_attribute("time_from", str(time_from))
                span.set_attribute("time_to", str(time_to))
                interval_minutes = (time_to - time_from).total_seconds() / 60
                try:
                    logger.info(
                        f"Acquire data on {cluster_name} for interval: "
                        f"{time_from} to {time_to} ({interval_minutes} min)"
                    )

                    key = f"{time_from.strftime(DATE_FORMAT_HOUR)}_{time_to.strftime(DATE_FORMAT_HOUR)}"

                    yield (key, get_jobs(clusters[cluster_name], time_from, time_to))

                    if auto_interval is not None:
                        set_auto_end_time(cluster_name, auto_end_field, time_to)
                # pylint: disable=broad-exception-caught
                except Exception as e:
                    logger.error(
                        f"Failed to acquire data on {cluster_name} for interval: "
                        f"{time_from} to {time_to}: {type(e).__name__}: {e}"
                    )
                    raise e

    for cluster_name in cluster_names:
        # Define the time intervals on which we want to retrieve the jobs
        intervals: list[tuple[datetime, datetime]] = []

        try:
            if unparsed_intervals is not None:
                intervals = parse_intervals(unparsed_intervals)
            elif auto_interval is not None:
                intervals = parse_auto_intervals(
                    cluster_name, auto_end_field, auto_interval
                )
            if not intervals:
                logger.warning(
                    "No --intervals or --auto_interval parsed, nothing to do."
                )
                continue

            if with_cache:
                # Define cache directory
                cache = Cache(subdirectory=f"jobs/{cluster_name}")

                with cache.create_entry(datetime.now(UTC)) as cache_entry:
                    for cache_key, jobs in _fetch_jobs(
                        cluster_name, clusters, auto_interval
                    ):
                        cache_entry.add_value(
                            key=cache_key,
                            value=pickle.dumps(jobs),
                        )
            else:
                for key, value in _fetch_jobs(cluster_name, clusters, auto_interval):
                    pass  # yield value?

        # pylint: disable=broad-exception-caught
        except Exception as e:
            logger.error(
                f"Error while acquiring data on {cluster_name}: {type(e).__name__}: {e} ; skipping cluster."
            )
        # Continue to next cluster.
        continue


def parse_jobs(
    cluster_names: list[str], clusters: dict[str, ClusterConfig], since: datetime
) -> None:  # Iterable[SlurmJob]:
    collection = _jobs_collection()

    for cluster_name in cluster_names:
        logger.info(
            f"Saving jobs of cluster {cluster_name} into mongodb collection '{collection.Meta.collection_name}'..."
        )

        # Retrieve from the cache
        cache = Cache(subdirectory=f"jobs/{cluster_name}")
        for cache_entry in cache.read_from(from_time=since):
            nb_jobs = 0
            nb_entries = 0

            # Retrieve all jobs associated to the time intervals
            for key, value in cache_entry.items():
                logger.info(
                    f"Acquire data on {cluster_name} for job identified by: {key}"
                )

                jobs = pickle.loads(value)
                nb_jobs += len(jobs)

                # Store the jobs in the database, beginning by the
                # oldest intervals
                for entry in tqdm(jobs):
                    if entry is not None:
                        nb_entries += 1
                        update_allocated_gpu_type_from_nodes(
                            clusters[cluster_name], entry
                        )
                        collection.save_job(entry)

            logger.info(f"Saved {nb_entries}/{nb_jobs} entries.")
