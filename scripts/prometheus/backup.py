import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from tqdm import tqdm
from simple_parsing import ArgumentParser

from sarc.client.job import _jobs_collection, JobStatistics, SlurmJob
from sarc.config import config, UTC
from sarc.core.models.validators import datetime_utc
from sarc.jobs.sacct import update_allocated_gpu_type_from_nodes
from sarc.jobs.series import (
    _get_job_time_series_data_cache_key,
    JOB_STATISTICS_METRIC_NAMES,
)

logger = logging.getLogger("prometheus_dump.backup")


DATE_FORMAT_HOUR = "%Y-%m-%dT%H:%M"


class JobPrometheusData(BaseModel):
    """Helper class to store Prometheus metrics for a job"""

    cluster_name: str
    job_id: int
    submit_time: datetime_utc
    stored_statistics: JobStatistics | None = None
    gpu_type: str | None = None


def main() -> int:
    """
    Backup Prometheus metrics from database into cache.
    Currently, save:
    - SlurmJob.stored_statistics,
      if not null and not empty.
    - SlurmJob.allocated_gpu_type,
      if GPU cannot be inferred from job nodes.
    """
    parser = ArgumentParser(description=main.__doc__)
    parser.parse_args()

    cfg = config("scraping")
    coll_jobs = _jobs_collection()
    # Get oldest submit_time.
    # Will be used to get jobs through pagination.
    (oldest_job,) = coll_jobs.find_by({}, sort=[("submit_time", 1)], limit=1)
    oldest_time = oldest_job.submit_time
    newest_time = datetime.now(tz=UTC)
    assert isinstance(oldest_time, datetime)
    assert oldest_time < newest_time
    logger.info(
        f"Oldest submit time in db: {oldest_time} (since {newest_time - oldest_time})"
    )
    # Count relevant jobs
    # We look for jobs which have either allocated.gpu_type or stored_statistics.
    base_query = {
        "$or": [
            {"allocated.gpu_type": {"$type": "string"}},
            {"stored_statistics": {"$type": "object"}},
        ],
    }
    logger.info("Counting jobs ...")
    expected = cfg.mongo.database_instance.jobs.count_documents(base_query)
    # We will save Prometheus metrics in a JSONL file,
    # one line per job, in `<sarc-cache>/prometheus_backup`.
    # Note that there is also a folder `<sarc-cache>/prometheus`,
    # which instead contains raw Prometheus data cached from calls
    # to get_job_time_series().
    assert cfg.cache is not None
    output_dir = (cfg.cache / "prometheus_backup").resolve()
    output_path = output_dir / f"{newest_time.isoformat()}.jsonl"
    logger.info(f"Writing prometheus backup to {output_path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    # Get Prometheus metrics from jobs
    interval = timedelta(days=180)
    count = 0
    updated = 0
    gpu_in_raw_cache = 0
    gpu_from_nodes = 0
    gpu_collected = 0
    stats_in_raw_cache = 0
    stats_collected = 0
    current_time = oldest_time
    # We save jobs gradually in a JSONL file,
    # instead of collecting-then-saving, to prevent any lack-of-memory issue.
    with open(output_path, "w", encoding="utf-8") as jsonl_file:
        with tqdm(total=expected, desc="job(s)") as pbar:
            while current_time < newest_time:
                # Get jobs so that: current_time <= job.submit_time < current_time + interval
                next_time = current_time + interval
                for job in coll_jobs.find_by(
                    {
                        **base_query,
                        "submit_time": {"$gte": current_time, "$lt": next_time},
                    },
                    sort=[("submit_time", 1)],
                ):
                    assert (
                        job.allocated.gpu_type is not None
                        or job.stored_statistics is not None
                    )
                    # Get Prometheus metrics
                    data: dict[str, Any] = {}

                    if (
                        job.stored_statistics is not None
                        and not job.stored_statistics.empty()
                    ):
                        if has_prometheus_cache_for_statistics(cfg.cache, job):
                            # There is Prometheus raw data in cache for these stats
                            # We don't save it.
                            stats_in_raw_cache += 1
                        else:
                            data["stored_statistics"] = job.stored_statistics
                            stats_collected += 1

                    if job.allocated.gpu_type is not None:
                        skip = False
                        if gpu_type_can_be_inferred_from_nodes(job):
                            # This GPU type can be inferred from job nodes
                            # on job parsing. No need to save it.
                            gpu_from_nodes += 1
                            skip = True
                        if has_prometheus_cache_for_gpu_type(cfg.cache, job):
                            # There is Prometheus raw data in cache for this GPU type.
                            # We don't save it.
                            gpu_in_raw_cache += 1
                            skip = True
                        if not skip:
                            data["gpu_type"] = job.allocated.gpu_type
                            gpu_collected += 1

                    if data:
                        # There are metrics to save.
                        # Create a record.
                        record = JobPrometheusData(
                            cluster_name=job.cluster_name,
                            job_id=job.job_id,
                            submit_time=job.submit_time.astimezone(UTC),
                            **data,
                        )
                        # Convert record to JSON-compatible dict.
                        # We don't directly use record.model_dump_json()
                        # because it replace NaN with null, and we don't want that.
                        # There can be NaN values in stored_statistics.
                        record_dict = record.model_dump(mode="json", exclude_unset=True)
                        # Now, we generate JSON wieh NaN explicitly allowed.
                        json_output = json.dumps(record_dict, allow_nan=True)
                        # Finally, we save JSON in a line in JSONL file.
                        print(json_output, file=jsonl_file)
                        updated += 1
                    pbar.update(1)
                    count += 1
                current_time = next_time

    # Log some info about backup.
    logger.info(f"Wrote prometheus backup to {output_path}")
    logger.info(f"Collected Prometheus metrics for {updated}/{expected} jobs")
    logger.info(f"Collected GPU types: {gpu_collected}")
    logger.info(
        f"Skipped GPU types that can be inferred from job nodes: {gpu_from_nodes}"
    )
    logger.info(
        f"Skipped GPU types already in raw prometheus cache: {gpu_in_raw_cache}"
    )
    logger.info(f"Collected statistics: {stats_collected}")
    logger.info(
        f"Skipped statistics already in raw prometheus cache: {stats_in_raw_cache}"
    )
    if count != expected:
        logger.warning(
            f"ERROR: Expected {expected} jobs, actually processed {count} jobs"
        )
    return 0


def has_prometheus_cache_for_statistics(cache: Path, job: SlurmJob) -> bool:
    """Return True if there is Prometheus raw cache for job.stored_statistics."""
    # We check if a cache file exists for the call to get_job_time_series()
    # which should be used to generate stored_statistics.
    cache_key = _get_job_time_series_data_cache_key(
        job, JOB_STATISTICS_METRIC_NAMES, max_points=10_000
    )
    return cache_key is not None and (cache / "prometheus" / cache_key).exists()


def has_prometheus_cache_for_gpu_type(cache: Path, entry: SlurmJob) -> bool:
    """Return True if there is Prometheus raw cache for job.allocated.gpu_type."""
    # We check if a cache file exists for the call to get_job_time_series()
    # which should be used to get GPU type from Prometheus.
    cache_key = _get_job_time_series_data_cache_key(
        job=entry,
        metric="slurm_job_utilization_gpu_memory",
        max_points=1,
    )
    return cache_key is not None and (cache / "prometheus" / cache_key).exists()


def gpu_type_can_be_inferred_from_nodes(entry: SlurmJob) -> bool:
    """
    Return True if GPU type can be inferred from job nodes.

    There are 3 sources of info for GPU type:
    1) `sacct`
    2) `job nodes` (overwrite `sacct` if available)
    3) `prometheus` (overwrite `sacct` and `job nodes` if available)

    `job nodes` source will always be computed when running
    `acquire jobs`. So, if it returns same GPU as the one currently saved,
    then we don't need to save it here.

    PS: We would also like to check `sacct` vs `prometheus` sources,
    but we would need to check `sacct` cache to do so: too complicated here.
    """

    # Get current job GPU type
    saved_gpu_type = entry.allocated.gpu_type

    # If update_allocated_gpu_type_from_nodes() cannot infer GPU type from nodes,
    # it returns current value of allocated.gpu_type. So, we set it to None
    # so that the function returns None by default.
    entry.allocated.gpu_type = None

    # Now, try to infer GPU type from nodes.
    gpu_type_from_nodes = update_allocated_gpu_type_from_nodes(
        entry.fetch_cluster_config(), entry
    )

    # We set allocated.gpu_type back to saved value
    entry.allocated.gpu_type = saved_gpu_type

    # If inferred value is same as saved value, then GPU type can be inferred from nodes,
    # and we won't need to save it as Prometheus data.
    return gpu_type_from_nodes is not None and gpu_type_from_nodes == saved_gpu_type


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, force=True)
    sys.exit(main())
