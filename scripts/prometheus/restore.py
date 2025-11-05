import json
import logging
import math
import sys
from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel
from simple_parsing import ArgumentParser, field
from tqdm import tqdm

from sarc.client.job import _jobs_collection, JobStatistics, Statistics
from sarc.core.models.validators import datetime_utc

logger = logging.getLogger("prometheus_dump.restore")


class JobPrometheusData(BaseModel):
    """Helper class to store Prometheus metrics for a job"""

    # Copied from backup.py

    cluster_name: str
    job_id: int
    submit_time: datetime_utc
    stored_statistics: JobStatistics | None = None
    gpu_type: str | None = None


def job_stats_equal(left: JobStatistics | None, right: JobStatistics | None) -> bool:
    """
    Return True if JobStatistics objects have exact same content.
    We don't use default `==` because
    job statistics can contain NaN values,
    however NaN != NaN by default.
    """
    if left is None and right is None:
        return True
    if (left is None and right is not None) or (left is not None and right is None):
        return False
    return all(
        stats_equal(getattr(left, key), getattr(right, key))
        for key in JobStatistics.model_fields.keys()
    )


def stats_equal(left: Statistics | None, right: Statistics | None) -> bool:
    """Return True if Statistics have exact same content."""
    if left is None and right is None:
        return True
    if (left is None and right is not None) or (left is not None and right is None):
        return False
    return all(
        floats_equals(getattr(left, key), getattr(right, key))
        for key in Statistics.model_fields.keys()
    )


def floats_equals(left: float, right: float) -> bool:
    """Return True if floats are equal, even if they are both NaN."""
    return (math.isnan(left) and math.isnan(right)) or left == right


@dataclass
class Options:
    input: str = field(alias=["-i"], help="Prometheus backup file (JSONL) to load")
    force: bool = field(
        alias=["-f"],
        action="store_true",
        help="If True, overwrite existing prometheus metrics.",
    )


def main() -> int:
    """Restore already-computed Prometheus metrics from cache into database."""
    parser = ArgumentParser(description=main.__doc__)
    parser.add_arguments(Options, dest="options")
    options = parser.parse_args().options

    logger.info(f"Counting lines into: {options.input}")
    with open(options.input, "r") as jsonl_file:
        nb_lines = sum((1 for _ in jsonl_file), start=0)

    if not nb_lines:
        logger.info("No lines in backup file, exiting.")
        return 0

    if not options.force:
        logger.warning(
            "No --force. Only null GPU type or stored_statistics in DB would be updated."
        )

    nb_found = 0
    nb_to_update_gpu_type = 0
    nb_to_update_stats = 0
    nb_to_update_jobs = 0
    nb_updated_gpu_type = 0
    nb_updated_stats = 0
    nb_updated_jobs = 0
    coll_jobs = _jobs_collection()
    with open(options.input, "r", encoding="utf-8") as json_file:
        for line in tqdm(json_file, total=nb_lines, desc="Job Prometheus data"):
            data = json.loads(line)
            data["submit_time"] = datetime.fromisoformat(data["submit_time"])
            record = JobPrometheusData.model_validate(data)
            jobs = list(
                coll_jobs.find_by(
                    {
                        "cluster_name": record.cluster_name,
                        "job_id": record.job_id,
                        "submit_time": record.submit_time,
                    }
                )
            )
            if jobs:
                (job,) = jobs
                nb_found += 1
                to_update = False
                to_save = False

                # Restore GPU type
                if (
                    record.gpu_type is not None
                    and job.allocated.gpu_type != record.gpu_type
                ):
                    to_update = True
                    nb_to_update_gpu_type += 1
                    if job.allocated.gpu_type is None or options.force:
                        job.allocated.gpu_type = record.gpu_type
                        to_save = True
                        nb_updated_gpu_type += 1

                # Restore stored_statistics
                if record.stored_statistics is not None and not job_stats_equal(
                    job.stored_statistics, record.stored_statistics
                ):
                    to_update = True
                    nb_to_update_stats += 1
                    if job.stored_statistics is None or options.force:
                        job.stored_statistics = record.stored_statistics
                        to_save = True
                        nb_updated_stats += 1

                # Save job
                if to_update:
                    nb_to_update_jobs += 1
                if to_save:
                    nb_updated_jobs += 1
                    coll_jobs.save_job(job)

    # Log some info about restoration
    logger.info(f"Jobs found: {nb_found} / {nb_lines}")
    logger.info(f"Jobs: to update: {nb_to_update_jobs}, updated: {nb_updated_jobs}")
    logger.info(
        f"GPU types: to update: {nb_to_update_gpu_type}, updated: {nb_updated_gpu_type}"
    )
    logger.info(
        f"Prometheus stats: to update: {nb_to_update_stats}, updated: {nb_updated_stats}"
    )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, force=True)
    sys.exit(main())
