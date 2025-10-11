import json
import os
from dataclasses import dataclass

from pydantic import BaseModel
from simple_parsing import field
from sarc.client.job import _jobs_collection, JobStatistics
from datetime import datetime, timedelta
from sarc.config import config, TZLOCAL
import logging
from tqdm import tqdm


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
    skip_cache: bool = field(
        type=bool,
        action="store_true",
        help="If True, skip jobs which already have prometheus cached data in <sarc-cache>/prometheus",
    )

    def execute(self) -> int:
        logger.debug(f"skip cache: {self.skip_cache}")

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
        base_query = {}
        cfg = config()
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
        current_time = oldest_time
        with open(output_path, "w", encoding="utf-8") as file:
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
                        # Get Prometheus metrics
                        data = {}
                        if (
                            job.stored_statistics is not None
                            and not job.stored_statistics.empty()
                        ):
                            data["stored_statistics"] = job.stored_statistics
                        if job.allocated.gpu_type is not None:
                            data["gpu_type"] = job.allocated.gpu_type
                        if data:
                            record = JobPrometheusData(
                                cluster_name=job.cluster_name,
                                job_id=job.job_id,
                                submit_time=job.submit_time,
                                **data,
                            )
                            print(record.model_dump_json(), file=file)
                            updated += 1
                        pbar.update(1)
                        count += 1
                    current_time = next_time
        logger.info(f"Wrote prometheus backup to {output_path}")
        logger.info(f"Collected Prometheus metrics for {updated}/{expected} jobs")
        if count != expected:
            logger.error(
                f"ERROR: Expected {expected} jobs, actually processed {count} jobs",
            )
            return -1
        return 0
