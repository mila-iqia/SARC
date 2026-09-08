import logging
from dataclasses import dataclass

import simple_parsing
from sqlalchemy import text

from sarc.config import config
from sarc.db.job_series import job_series_backfill_sql

logger = logging.getLogger(__name__)


@dataclass
class BackfillSeriesCommand:
    """Bulk (re)sync of the job_series read-model table from its base tables.

    Idempotent and safe to run concurrently with the maintenance triggers: rows
    already in job_series are left alone (the triggers' version is never older
    than this scan). Run once after the migration that creates the table; rerun
    whenever the table must be trusted anew.
    """

    truncate: bool = simple_parsing.field(
        default=False,
        alias=["--truncate"],
        help="Empty job_series first, rebuilding every row from scratch "
        "(the triggers keep new writes correct during the rebuild).",
    )

    def execute(self) -> int:
        # Chunked by job id on purpose. One big INSERT..SELECT would hold row
        # locks on every job_series row for the whole scan, and the slurm_jobs
        # trigger's ON CONFLICT DO UPDATE would then block the scraper behind
        # it. Short transactions keep both sides wait-free.
        chunk = 100_000
        total = 0
        with config.db.session() as sess:
            if self.truncate:
                logger.info("Truncating job_series")
                sess.exec(text("TRUNCATE job_series"))  # ty: ignore[no-matching-overload]
                sess.commit()
            lo, hi = sess.exec(
                text(
                    "SELECT coalesce(min(id), 0), coalesce(max(id), 0) FROM slurm_jobs"
                )
            ).one()  # ty: ignore[no-matching-overload]
            insert = text(
                job_series_backfill_sql(where="AND j.id >= :lo AND j.id < :hi")
            )
            while lo <= hi:
                result = sess.exec(insert, {"lo": lo, "hi": lo + chunk})  # ty: ignore[no-matching-overload]
                sess.commit()
                total += result.rowcount
                logger.info(
                    "job_series backfill: id < %d (%d rows so far)", lo + chunk, total
                )
                lo += chunk
            logger.info("Backfilled %d job_series rows", total)
            sess.exec(text("ANALYZE job_series"))  # ty: ignore[no-matching-overload]
            sess.commit()
        return 0
