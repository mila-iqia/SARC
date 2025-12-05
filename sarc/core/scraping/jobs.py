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


def get_jobs(cluster: ClusterConfig, start: datetime, end: datetime) -> bytes:
    scraper = SacctScraper(cluster, start, end)

    logger.info(
        f"Getting the sacct data for cluster {cluster.name}, time {start} to {end}..."
    )

    scraper.get_raw()

    jobs = [job for job in scraper]

    return pickle.dumps(jobs)


def fetch_jobs(
    cluster_names: list[str],
    clusters: dict[str, ClusterConfig],
    unparsed_intervals: Optional[list[tuple[datetime, datetime]]],
    auto_interval: Optional[int],
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

    for cluster_name in cluster_names:
        # Define cache directory
        cache = Cache(subdirectory=f"jobs/{cluster_name}")
        ce = cache.create_entry(datetime.now(UTC))

        try:
            # Define the time intervals on which we want to retrieve the jobs
            intervals: list[tuple[datetime, datetime]] = []
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

                        ce.add_value(
                            key=key,
                            value=get_jobs(clusters[cluster_name], time_from, time_to),
                        )

                        if auto_interval is not None:
                            set_auto_end_time(cluster_name, auto_end_field, time_to)
                    # pylint: disable=broad-exception-caught
                    except Exception as e:
                        logger.error(
                            f"Failed to acquire data on {cluster_name} for interval: "
                            f"{time_from} to {time_to}: {type(e).__name__}: {e}"
                        )
                        raise e

        # pylint: disable=broad-exception-caught
        except Exception as e:
            logger.error(
                f"Error while acquiring data on {cluster_name}: {type(e).__name__}: {e} ; skipping cluster."
            )
            # Continue to next cluster.
            continue

        ce.close()


def parse_jobs(
    cluster_names: list[str], clusters: dict[str, ClusterConfig], from_: datetime
) -> None:  # Iterable[SlurmJob]:
    collection = _jobs_collection()

    for cluster_name in cluster_names:
        logger.info(
            f"Saving jobs of cluster {cluster_name} into mongodb collection '{collection.Meta.collection_name}'..."
        )

        # Retrieve from the cache
        cache = Cache(subdirectory=f"jobs/{cluster_name}")
        for ce in cache.read_from(from_time=from_):

            def get_datetimes_from_key(time_interval):
                str_dates = time_interval.split("T")
                start_date = datetime.strptime(str_dates[0])
                end_date = datetime.strptime(str_dates[1])
                return (start_date, end_date)

            time_intervals = [get_datetimes_from_key(key) for key in ce.get_keys()]

            nb_jobs = 0
            nb_entries = 0

            # Sort the time intervals stored in cluster_jobs
            # The intervals are preferably sorted by the oldest end time,
            # then sorted by the oldest start time
            time_intervals.sort(key=lambda x: (x[1], x[0]))

            # Retrieve all jobs associated to the time intervals
            for time_interval in time_intervals:
                key = f"{time_interval[0].strftime(DATE_FORMAT_HOUR)}_{time_interval[1].strftime(DATE_FORMAT_HOUR)}"
                jobs = pickle.loads(ce.get_value(key))
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
