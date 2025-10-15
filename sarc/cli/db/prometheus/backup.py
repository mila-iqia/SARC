import os
from dataclasses import dataclass

from pydantic import BaseModel

from sarc.cache import using_cache_policy, CachePolicy, CacheException
from sarc.client.job import _jobs_collection, JobStatistics, SlurmJob
from datetime import datetime, timedelta
from sarc.config import config, TZLOCAL
import logging
from tqdm import tqdm

from sarc.jobs.prometheus_scraping import update_allocated_gpu_type_from_prometheus
from sarc.jobs.sacct import update_allocated_gpu_type_from_nodes
from sarc.jobs.series import compute_job_statistics

logger = logging.getLogger(__name__)


DATE_FORMAT_HOUR = "%Y-%m-%dT%H:%M"


class JobPrometheusData(BaseModel):
    cluster_name: str
    job_id: int
    submit_time: datetime
    stored_statistics: JobStatistics | None = None
    gpu_type: str | None = None


@dataclass
class DbPrometheusBackup:
    def execute(self) -> int:
        cfg = config("scraping")
        prometheus_cluster_names: list[str] = [
            cluster.name for cluster in cfg.clusters.values() if cluster.prometheus_url
        ]
        if not prometheus_cluster_names:
            logger.error(
                "No cluster with prometheus_url configured, "
                "we don't expect any job to have Prometheus metrics."
            )
            return -1

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
        # Count jobs
        base_query = {
            "cluster_name": {"$in": prometheus_cluster_names},
            "$or": [
                {"allocated.gpu_type": {"$type": "string"}},
                {"stored_statistics": {"$type": "object"}},
            ],
        }
        logger.info("Counting jobs ...")
        expected = cfg.mongo.database_instance.jobs.count_documents(base_query)
        # Prepare JSONL file
        output_dir = (cfg.cache / "prometheus_backup").resolve()
        output_path = output_dir / f"{newest_time.strftime(DATE_FORMAT_HOUR)}.jsonl"
        logger.info(f"Writing prometheus backup to {output_path}")
        os.makedirs(output_dir, exist_ok=True)
        # Get Prometheus metrics from jobs
        interval = timedelta(days=180)
        count = 0
        updated = 0
        gpu_in_raw_cache = 0
        gpu_collected = 0
        stats_in_raw_cache = 0
        stats_collected = 0
        current_time = oldest_time
        with open(output_path, "w", encoding="utf-8") as jsonl_file:
            with tqdm(total=expected, desc="job(s)") as pbar:
                while current_time < newest_time:
                    # Get jobs so that: current_time <= job.submit_time < current_time + interval
                    next_time = current_time + interval
                    for job in coll_jobs.find_by(
                        {
                            **base_query,
                            "submit_time": {"$gte": current_time, "$lt": next_time},
                        }
                    ):
                        assert (
                            job.allocated.gpu_type is not None
                            or job.stored_statistics is not None
                        )
                        # Get Prometheus metrics
                        data = {}

                        if (
                            job.stored_statistics is not None
                            and not job.stored_statistics.empty()
                        ):
                            if has_prometheus_cache_for_statistics(job):
                                stats_in_raw_cache += 1
                            else:
                                data["stored_statistics"] = job.stored_statistics
                                stats_collected += 1

                        if job.allocated.gpu_type is not None:
                            if has_prometheus_cache_for_gpu_type(job):
                                gpu_in_raw_cache += 1
                            else:
                                current_gpu_type = job.allocated.gpu_type
                                sacct_or_nodes_gpu_type = (
                                    update_allocated_gpu_type_from_nodes(
                                        job.fetch_cluster_config(), job
                                    )
                                )
                                if current_gpu_type != sacct_or_nodes_gpu_type:
                                    data["gpu_type"] = current_gpu_type
                                    gpu_collected += 1

                        if data:
                            record = JobPrometheusData(
                                cluster_name=job.cluster_name,
                                job_id=job.job_id,
                                submit_time=job.submit_time,
                                **data,
                            )
                            print(
                                record.model_dump_json(exclude_unset=True),
                                file=jsonl_file,
                            )
                            updated += 1
                        pbar.update(1)
                        count += 1
                    current_time = next_time
        logger.info(f"Wrote prometheus backup to {output_path}")
        logger.info(f"Collected Prometheus metrics for {updated}/{expected} jobs")
        logger.info(f"Collected GPU types: {gpu_collected}")
        logger.info(f"Collected statistics: {stats_collected}")
        logger.info(
            f"Skipped GPU types already in raw prometheus cache: {gpu_in_raw_cache}"
        )
        logger.info(
            f"Skipped statistics already in raw prometheus cache: {stats_in_raw_cache}"
        )
        if count != expected:
            logger.warning(
                f"ERROR: Expected {expected} jobs, actually processed {count} jobs",
            )
        return 0


def has_prometheus_cache_for_statistics(job: SlurmJob) -> bool:
    try:
        with using_cache_policy(CachePolicy.always):
            compute_job_statistics(job)
            return True
    except CacheException:
        return False


def has_prometheus_cache_for_gpu_type(job: SlurmJob) -> bool:
    previous_gpu_type = job.allocated.gpu_type
    try:
        with using_cache_policy(CachePolicy.always):
            update_allocated_gpu_type_from_prometheus(job.fetch_cluster_config(), job)
            return True
    except CacheException:
        return False
    finally:
        job.allocated.gpu_type = previous_gpu_type
