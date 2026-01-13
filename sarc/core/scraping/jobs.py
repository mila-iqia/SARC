import json
import logging
from collections.abc import Iterable
from datetime import UTC, datetime

from sarc.cache import Cache
from sarc.client.job import SlurmJob
from sarc.config import ClusterConfig, config
from sarc.jobs.sacct import SAcctScraper, update_allocated_gpu_type_from_nodes

logger = logging.getLogger(__name__)


def fetch_jobs(clusters_and_intervals: list[tuple[ClusterConfig, datetime, datetime]]) -> None:
    """Fetch sacct data and place the results in cache.

    This method should never raise any exceptions that would prevent other
    clusters from being scraped. Errors are logged but execution continues.

    Parameters
    ----------
    clusters_and_intervals : list[tuple[ClusterConfig, datetime, datetime]]
        List of (cluster, start_time, end_time) tuples to fetch jobs for.
    """
    cache = Cache(subdirectory="jobs_sacct")
    with cache.create_entry(datetime.now(UTC)) as ce:
        for cluster, start, end in clusters_and_intervals:
            try:
                logger.info(
                    f"Fetching jobs for cluster {cluster.name}, time {start} to {end}..."
                )
                scraper = SAcctScraper(cluster, start, end)
                raw_json = scraper.fetch_raw()

                # Create cache key: cluster.start.end.json
                fmt = "%Y-%m-%dT%H:%M"
                key = f"{cluster.name}.{start.strftime(fmt)}.{end.strftime(fmt)}.json"

                # Store JSON as bytes
                json_bytes = json.dumps(raw_json).encode('utf-8')
                ce.add_value(key, json_bytes)

                num_jobs = len(raw_json.get("jobs", []))
                logger.info(f"Cached {num_jobs} jobs for {cluster.name}")
            except Exception as e:
                logger.error(
                    f"Failed to fetch jobs for {cluster.name} ({start} to {end}): {e}",
                    exc_info=e
                )


def parse_jobs(from_time: datetime, cluster_filter: str | None = None) -> Iterable[tuple[ClusterConfig, SlurmJob]]:
    """Parse job data from the cache.

    Reads cached sacct data and yields (cluster_config, slurm_job) tuples.
    Individual job conversion errors are logged but don't stop processing.

    Parameters
    ----------
    from_time : datetime
        Start parsing cached data from this date (must be in UTC).
    cluster_filter : str | None
        Optional cluster name to filter by. If None, processes all clusters.

    Yields
    ------
    tuple[ClusterConfig, SlurmJob]
        Tuple of (cluster_config, slurm_job) for each successfully parsed job.
    """
    cache = Cache(subdirectory="jobs_sacct")
    cfg = config("scraping")

    for ce in cache.read_from(from_time=from_time):
        for key, blob in ce.items():
            try:
                # Parse key: cluster.start.end.json
                parts = key.rstrip('.json').split('.')
                if len(parts) < 5:  # cluster.YYYY-MM-DDTHH:MM.YYYY-MM-DDTHH:MM
                    logger.warning(f"Invalid cache key format: {key}")
                    continue

                cluster_name = parts[0]

                # Filter by cluster if specified
                if cluster_filter and cluster_name != cluster_filter:
                    continue

                # Get cluster config
                if cluster_name not in cfg.clusters:
                    logger.warning(f"Unknown cluster in cache: {cluster_name}")
                    continue

                cluster = cfg.clusters[cluster_name]

                # Decode JSON
                raw_json = json.loads(blob.decode('utf-8'))

                # Extract metadata for conversion
                version = (
                    raw_json.get("meta", {}).get("Slurm", None)
                    or raw_json.get("meta", {}).get("slurm", {})
                ).get("version", None)

                # Parse time range from key for scraping metadata
                # Key format: cluster.YYYY-MM-DDTHH:MM.YYYY-MM-DDTHH:MM.json
                try:
                    start_str = '.'.join(parts[1:3])  # YYYY-MM-DDTHH:MM
                    end_str = '.'.join(parts[3:5])    # YYYY-MM-DDTHH:MM
                    start_time = datetime.fromisoformat(start_str).replace(tzinfo=UTC)
                    end_time = datetime.fromisoformat(end_str).replace(tzinfo=UTC)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Could not parse timestamps from key {key}: {e}")
                    # Use dummy times if parsing fails
                    start_time = from_time
                    end_time = datetime.now(UTC)

                # Create scraper for context (needed for convert method)
                scraper = SAcctScraper(cluster, start_time, end_time)

                # Convert each job entry
                for entry in raw_json.get("jobs", []):
                    try:
                        job = scraper.convert(entry, version)
                        if job is not None:
                            # Update GPU type from nodes
                            update_allocated_gpu_type_from_nodes(cluster, job)
                            yield cluster, job
                    except Exception as e:
                        logger.warning(
                            f"Failed to convert job entry for {cluster_name}: {e}",
                            exc_info=False  # Don't log full traceback for each job
                        )

            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON from cache key {key}: {e}")
            except Exception as e:
                logger.error(
                    f"Failed to process cache entry {key}: {e}",
                    exc_info=e
                )
