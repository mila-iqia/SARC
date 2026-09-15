#!/usr/bin/env python3
"""Backfill slurm_jobs.reservation and JOB_ALTERED from the raw jobs cache.

Both fields were added after the sacct/fastsacct payloads were already
fetched into the jobs cache, so the raw data is there but the columns are
not populated. This pass rereads the jobs cache from its beginning and, for
each cached job entry that carries a reservation or the JOB_ALTERED flag,
fills just those two fields on the corresponding slurm_jobs row (matched on
cluster_id / job_id / submit_time, the job's uniqueness key). Cached entries
with neither are ignored; so are cached jobs that have no row in the
database. Nothing else is touched: no upsert, no gpu-type fixups, and the
parsed_dates bookkeeping is left alone.

The update is additive and idempotent: a NULL reservation is filled, a false
JOB_ALTERED is raised, existing values are never overwritten -- running it
twice changes nothing the second time. job_series stays in sync by itself
(the slurm_jobs_job_series trigger watches these columns).

Usage (with SARC_CONFIG pointing at the production config):
    python scripts/backfill_reservation_and_job_altered.py [--dry-run]
"""

import argparse
import logging

from sqlmodel import select

from sarc.cache import Cache
from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB
from sarc.scraping.jobs import parse_date
from sarc.scraping.jobs_utils import parse_raw

logger = logging.getLogger("backfill_reservation_and_job_altered")


def backfill(*, dry_run: bool = False) -> dict[str, int]:
    stats = {
        "cache_entries": 0,
        "cache_keys": 0,
        "jobs_seen": 0,
        "candidates": 0,  # cached jobs carrying a reservation or JOB_ALTERED
        "updated": 0,  # db rows this pass changed
        "already_set": 0,  # db rows that had it all already
        "no_db_row": 0,  # cached jobs with no matching row in slurm_jobs
    }
    # The same job recurs in later scrape windows; settle each once.
    settled: set[tuple] = set()

    cache = Cache(subdirectory="jobs")
    with config.db.session() as sess:
        clusters = {c.name: c for c in sess.exec(select(SlurmClusterDB)).all()}

        for cache_entry in cache.read_from(cache.oldest_year()):
            stats["cache_entries"] += 1
            logger.info(f"Parsing cache entry: {cache_entry.get_entry_datetime()}")
            for key, value in cache_entry.items():
                stats["cache_keys"] += 1
                parts = key.split("_")
                cluster = clusters.get(parts[0])
                if cluster is None:
                    logger.warning(
                        f"Unknown cluster {parts[0]} in cache key {key}, skipping"
                    )
                    continue
                scraped_start = parse_date(parts[1])
                scraped_end = parse_date(parts[2])

                for entry in parse_raw(value, cluster.name, scraped_start, scraped_end):
                    if entry is None:
                        continue
                    stats["jobs_seen"] += 1

                    reservation = entry.get("reservation")
                    job_altered = bool(entry.get("JOB_ALTERED"))
                    if reservation is None and not job_altered:
                        # Nothing to add: ignore this entry entirely.
                        continue
                    stats["candidates"] += 1

                    ident = (cluster.id, entry["job_id"], entry["submit_time"])
                    if ident in settled:
                        continue

                    row = sess.exec(
                        select(SlurmJobDB).where(
                            SlurmJobDB.cluster_id == cluster.id,
                            SlurmJobDB.job_id == entry["job_id"],
                            SlurmJobDB.submit_time == entry["submit_time"],
                        )
                    ).first()
                    if row is None:
                        stats["no_db_row"] += 1
                        settled.add(ident)
                        continue

                    changed = False
                    if reservation is not None and row.reservation is None:
                        row.reservation = reservation
                        changed = True
                    if job_altered and not row.JOB_ALTERED:
                        row.JOB_ALTERED = True
                        changed = True
                    if changed:
                        stats["updated"] += 1
                        sess.add(row)
                    else:
                        stats["already_set"] += 1
                    settled.add(ident)

            # One transaction per cache entry keeps the passes short.
            if dry_run:
                sess.rollback()
            else:
                sess.commit()

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--dry-run", action="store_true", help="count what would change, commit nothing"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = backfill(dry_run=args.dry_run)

    outcome = "would have" if args.dry_run else ""
    print(  # noqa: T201
        f"Scanned {stats['cache_entries']} cache entries "
        f"({stats['cache_keys']} keys, {stats['jobs_seen']} job entries); "
        f"{stats['candidates']} carried a reservation or JOB_ALTERED. "
        f"{outcome}Updated {stats['updated']} job(s): "
        f"{stats['already_set']} already set, "
        f"{stats['no_db_row']} without a database row."
    )


if __name__ == "__main__":
    main()
