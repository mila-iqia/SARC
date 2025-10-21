import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from tqdm import tqdm

from sarc.client.job import _jobs_collection, JobStatistics, SlurmJob
from sarc.config import config, TZLOCAL
from sarc.jobs.sacct import update_allocated_gpu_type_from_nodes
from sarc.jobs.series import (
    _get_job_time_series_data_cache_key,
    JOB_STATISTICS_METRIC_NAMES,
)

logger = logging.getLogger(__name__)


DATE_FORMAT_HOUR = "%Y-%m-%dT%H:%M"


class JobPrometheusData(BaseModel):
    """Helper class to store Prometheus metrics for a job"""

    cluster_name: str
    job_id: int
    submit_time: datetime
    stored_statistics: JobStatistics | None = None
    gpu_type: str | None = None


@dataclass
class DbPrometheusBackup:
    """
    Backup Prometheus metrics from database into cache.
    Currently, save:
    - SlurmJob.stored_statistics,
      if not null and not empty.
    - SlurmJob.allocated_gpu_type,
      if GPU cannot be inferred from sacct or job nodes.
    """

    def execute(self) -> int:
        cfg = config("scraping")
        coll_jobs = _jobs_collection()
        # Get oldest submit_time.
        # Will be used to get jobs through pagination.
        (oldest_job,) = coll_jobs.find_by({}, sort=[("submit_time", 1)], limit=1)
        oldest_time = oldest_job.submit_time
        newest_time = datetime.now(tz=TZLOCAL)
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
        output_path = output_dir / f"{newest_time.strftime(DATE_FORMAT_HOUR)}.jsonl"
        logger.info(f"Writing prometheus backup to {output_path}")
        output_dir.mkdir(parents=True, exist_ok=True)
        # Get Prometheus metrics from jobs
        interval = timedelta(days=180)
        count = 0
        updated = 0
        gpu_in_raw_cache = 0
        gpu_sacct_or_nodes = 0
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
                            current_gpu_type = job.allocated.gpu_type
                            sacct_or_nodes_gpu_type = (
                                update_allocated_gpu_type_from_nodes(
                                    job.fetch_cluster_config(), job
                                )
                            )
                            if current_gpu_type == sacct_or_nodes_gpu_type:
                                # When parsing sacct cache, this GPU type
                                # can be inferred from sacct data or job nodes.
                                # No need to save it.
                                gpu_sacct_or_nodes += 1
                            elif has_prometheus_cache_for_gpu_type(cfg.cache, job):
                                # There is Prometheus raw data in cache for this GPU type.
                                # We don't save it.
                                gpu_in_raw_cache += 1
                            else:
                                data["gpu_type"] = current_gpu_type
                                gpu_collected += 1

                        if data:
                            # There are metrics to save.
                            # Create a record.
                            record = JobPrometheusData(
                                cluster_name=job.cluster_name,
                                job_id=job.job_id,
                                submit_time=job.submit_time,
                                **data,
                            )
                            # Convert record to JSON-compatible dict.
                            # We don't directly use record.model_dump_json()
                            # because it replace NaN with null, and we don't want that.
                            # There can be NaN values in stored_statistics.
                            record_dict = record.model_dump(
                                mode="json", exclude_unset=True
                            )
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
            f"Skipped GPU types that can be inferred from sacct or nodes: {gpu_sacct_or_nodes}"
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
