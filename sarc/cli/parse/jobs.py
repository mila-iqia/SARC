import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from simple_parsing import field

from sarc.client.job import _jobs_collection
from sarc.core.scraping.jobs import parse_jobs

logger = logging.getLogger(__name__)


@dataclass
class ParseJobs:
    since: str = field(help="Start parsing cache from this date (ISO format, e.g., 2024-01-15T00:00:00)")

    cluster: str | None = field(
        default=None,
        help="Optional: filter by cluster name. If not specified, parses all clusters."
    )

    dry_run: bool = field(
        default=False,
        help="If True, parse jobs but don't save to MongoDB (for testing)"
    )

    def execute(self) -> int:
        # Parse since timestamp
        ts = datetime.fromisoformat(self.since)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        ts = ts.astimezone(UTC)

        logger.info(f"Parsing jobs from cache since {ts}")
        if self.cluster:
            logger.info(f"Filtering for cluster: {self.cluster}")
        if self.dry_run:
            logger.info("DRY RUN: Jobs will not be saved to MongoDB")

        collection = _jobs_collection()
        count = 0
        error_count = 0

        try:
            for cluster_cfg, job in parse_jobs(ts, self.cluster):
                count += 1
                if not self.dry_run:
                    try:
                        collection.save_job(job)
                    except Exception as e:
                        logger.warning(f"Failed to save job {job.job_id}: {e}")
                        error_count += 1

                # Log progress periodically
                if count % 100 == 0:
                    logger.info(f"Parsed {count} jobs...")

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            return 1
        except Exception as e:
            logger.error(f"Error during parsing: {e}", exc_info=e)
            return 1

        logger.info(f"Total: {count} jobs parsed")
        if error_count > 0:
            logger.warning(f"{error_count} jobs failed to save")

        if self.dry_run:
            logger.info("DRY RUN completed - no jobs were saved to MongoDB")

        return 0
