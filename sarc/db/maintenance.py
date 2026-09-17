"""Lock-light database maintenance (VACUUM / ANALYZE).

These statements refuse to run inside a transaction block, so they need a
connection in AUTOCOMMIT isolation — a regular ``Session`` (which always opens
a transaction) makes them fail with SQLSTATE 25001, quietly killing what looks
like a perfectly ordinary ``session.execute(text("VACUUM ..."))`` call.

The scheduled-scrape path vacuums the database right after a scrape lands.
Scraping rewrites recent rows everywhere (slurm_jobs in place, jobstatisticdb
upserted, and the job_series triggers on top of both), which clears the
visibility-map bits the index-only window scans depend on — without a vacuum
between scrapes those scans degrade into per-row heap fetches (measured ~15x
on a 6-week window with a fully dirty map). It also re-Analyzes everything:
the size-based autoanalyze thresholds (10% of a multi-million-row table)
leave these tables weeks between runs, letting the upper histogram edges that
recent-window estimates ride on — including the ``slurm_job_end`` expression
stats — lag further and further behind the data.
"""

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Connection, text

from sarc.config import config

logger = logging.getLogger(__name__)

# Fraction of shared_buffers the vacuum may touch, bounding how much of the
# dashboard's cached working set its (sequential, one-shot) reads can evict.
_VACUUM_BUFFER_SHARE = 4


@contextmanager
def maintenance_connection() -> Iterator[Connection]:
    """A connection that VACUUM/ANALYZE may run on (no transaction)."""
    conn = config.db.engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    try:
        yield conn
    finally:
        conn.close()


def vacuum_database() -> None:
    """Restore the visibility maps and statistics of every table post-scrape.

    Runs every hour from the parse cron, so it is shaped for that:
    TRUNCATE off skips the truncation phase's lock attempt entirely (the
    scraped tables only grow at the tail, so there is never a tail to
    truncate); PARALLEL 2 spreads the heap/index scan over maintenance
    workers; vacuum_buffer_usage_limit caps the shared_buffers share (a
    quarter of it, resolved from the server itself so dev boxes and prod both
    get a sane fraction). No cost throttling: measured on the local full-size
    copy, an already-clean full-database pass takes ~30 s and one pass over
    the big churned tables takes ~2 min (job_series alone: 25 s churned, 4 s
    clean), so it is cheaper to let it run at full speed than to throttle it
    and leave the consumers scanning a dirty map for minutes on top of the
    scrape delay. If the shaped form is unavailable (older server, missing
    option), fall back to a plain ``VACUUM ANALYZE``."""

    t0 = time.monotonic()
    with maintenance_connection() as conn:
        stmt = "VACUUM (ANALYZE, PARALLEL 2, TRUNCATE off)"
        try:
            sb_pages = conn.execute(
                text(
                    "SELECT setting::bigint FROM pg_settings WHERE name = 'shared_buffers'"
                )
            ).scalar()
            limit_kb = max(int(sb_pages * 8 / _VACUUM_BUFFER_SHARE), 128)  # 8 kB pages
            conn.execute(
                text(f"SET vacuum_buffer_usage_limit = {limit_kb}")  # size in kB
            )
        except Exception:  # noqa: BLE001 - tuning only ever, never required
            logger.warning(
                "could not set vacuum_buffer_usage_limit; vacuuming unbounded",
                exc_info=True,
            )
        try:
            conn.execute(text(stmt))
        except Exception:  # noqa: BLE001 - a failed vacuum must not fail the scrape
            logger.warning(
                f"{stmt} failed; falling back to plain VACUUM ANALYZE", exc_info=True
            )
            conn.execute(text("VACUUM ANALYZE"))
    logger.info("vacuumed database in %.1fs", time.monotonic() - t0)
