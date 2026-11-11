"""Repair gpu_utilization statistics stored during the exporter 0.5.0 x100.

slurm-job-exporter 0.5.0 in DCGM mode published the GPU utilization x100 (see
sarc/scraping/gpu_util_x100): jobs parsed while a cluster ran it are stored
inflated, and the rescale at parse time only helps jobs parsed after the fix.

Candidates are the `gpu_utilization` rows with `max >= 1.0` fetched from
X100_SINCE on: the rescale only changes a stored statistic when a divided
sample lands at >= 1.0, and it never divides samples before X100_SINCE. The
row's fetch_date names the cache entry that holds the job's raw series (one
entry per fetch run, the filename truncated to the millisecond), under the
key "<cluster>$<job_id>$<submit_time>".

For each candidate the script re-runs the parse-time judgement on the raw
series. When the rescale divides samples, the row gets the statistics the
fixed parse computes from them, unless it already holds them (a re-run, or a
job parsed after the fix). An ambiguous series -- a sample at 100 may be 1 %
or 100 % -- gets its gpu_utilization row deleted and its fetch record pointed
to a statistic the job still has, which is what the fixed parse stores for a
fresh job. Any other row is left alone. The job_series triggers mirror the
deletes and the updates.

Run where the prometheus file cache and the database are reachable, the way
`sarc fetch/parse prometheus` runs. The dry run, the default, writes nothing;
--no-dry-run writes:

    SARC_CONFIG=<file> uv run python scripts/fix_gpu_util_x100.py --dry-run
    SARC_CONFIG=<file> uv run python scripts/fix_gpu_util_x100.py --no-dry-run
"""

import argparse
import json
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import isnan

from sqlmodel import Session, and_, col, func, select
from tqdm import tqdm

from sarc.cache import Cache, CacheEntry
from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, JobStatisticsFetchDateDB, SlurmJobDB
from sarc.scraping.gpu_util_x100 import X100_SINCE, rescale_gpu_utilization
from sarc.scraping.series import (
    JOB_STATISTICS_METRIC_NAMES,
    STATS,
    _percent,
    compute_metric_statistics,
    gpu_activity_series,
)

logger = logging.getLogger("fix_gpu_util_x100")

NB_EXAMPLES = 10

STATUS_UNCHANGED = "unchanged"
STATUS_RESCALED = "rescaled"
STATUS_AMBIGUOUS = "ambiguous"
STATUS_MISSING_KEY = "missing_key"
STATUS_MISSING_ENTRY = "missing_entry"


@dataclass(frozen=True)
class Candidate:
    """One gpu_utilization row to re-judge, with the cache entry holding its raw series."""

    job: SlurmJobDB
    cluster_name: str
    stat: JobStatisticDB
    entry_time: datetime

    def label(self: Candidate) -> str:
        return f"{self.cluster_name}:{self.job.job_id}@{self.job.submit_time.isoformat(timespec='seconds')}"


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Re-judge the stored gpu_utilization statistics of the jobs fetched "
            "while slurm-job-exporter 0.5.0 could have published them x100."
        )
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Judge and report, write nothing to the database.",
    )
    args = parser.parse_args()
    # force=True: importing sarc modules already put a root handler in place at
    # WARNING level, which would silently swallow this script's INFO report.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s::%(levelname)s::%(name)s::%(message)s",
        force=True,
    )

    with config.db.session() as sess:
        candidates_by_entry = load_candidates(sess)
        total = sum(len(candidates) for candidates in candidates_by_entry.values())
        logger.info(
            f"{total} candidate rows across {len(candidates_by_entry)} cache entries"
        )
        cache = Cache("prometheus")
        results: list[tuple[Candidate, str]] = []
        for entry_time, candidates in tqdm(
            sorted(candidates_by_entry.items()), desc="read cache"
        ):
            results += process_entry(
                sess, cache, entry_time, candidates, dry_run=args.dry_run
            )
        # In the session: the report reloads jobs the last commit or rollback expired.
        log_report(results, args.dry_run)


def load_candidates(sess: Session) -> dict[datetime, list[Candidate]]:
    """The rows to re-judge, grouped by the cache entry that holds their raw series."""
    since = datetime.fromtimestamp(X100_SINCE, tz=UTC)
    rows = sess.exec(
        select(
            SlurmJobDB,
            SlurmClusterDB.name,
            JobStatisticDB,
            JobStatisticsFetchDateDB.fetch_date,
        )
        .join(SlurmClusterDB, col(SlurmClusterDB.id) == col(SlurmJobDB.cluster_id))
        .join(
            JobStatisticDB,
            and_(
                col(JobStatisticDB.job_id) == col(SlurmJobDB.id),
                JobStatisticDB.name == "gpu_utilization",
            ),
        )
        .join(
            JobStatisticsFetchDateDB,
            col(JobStatisticsFetchDateDB.job_id) == col(SlurmJobDB.id),
        )
        .where(JobStatisticDB.max >= 1.0, JobStatisticsFetchDateDB.fetch_date >= since)
        .order_by(col(JobStatisticsFetchDateDB.fetch_date))
    ).all()
    candidates_by_entry: dict[datetime, list[Candidate]] = defaultdict(list)
    for job, cluster_name, stat, fetch_date in rows:
        entry_time = _cache_entry_time(fetch_date)
        candidates_by_entry[entry_time].append(
            Candidate(
                job=job, cluster_name=cluster_name, stat=stat, entry_time=entry_time
            )
        )
    return candidates_by_entry


def _cache_entry_time(fetch_date: datetime) -> datetime:
    """The cache entry a fetch_date names: the filename keeps milliseconds only."""
    return fetch_date.replace(microsecond=fetch_date.microsecond // 1000 * 1000)


def process_entry(
    sess: Session,
    cache: Cache,
    entry_time: datetime,
    candidates: list[Candidate],
    *,
    dry_run: bool,
) -> list[tuple[Candidate, str]]:
    """Re-judge every candidate of one cache entry, write, and report each status."""
    wanted = {(c.cluster_name, c.job.job_id, c.job.submit_time): c for c in candidates}
    ce = _open_entry(cache, entry_time)
    if ce is None:
        return [(candidate, STATUS_MISSING_ENTRY) for candidate in candidates]

    statuses: list[tuple[Candidate, str]] = []
    ambiguous: list[Candidate] = []
    found: set[tuple[str, int, datetime]] = set()
    try:
        for key, value in ce.items():
            ref = _ref_of_key(key)
            candidate = wanted.get(ref)
            if candidate is None:
                continue
            found.add(ref)
            status, stats = judge_raw_series(
                candidate.stat,
                json.loads(value.decode("utf-8")),
                candidate.cluster_name,
            )
            statuses.append((candidate, status))
            if stats is not None:
                for name, stat_value in stats.items():
                    setattr(candidate.stat, name, stat_value)
            if status == STATUS_AMBIGUOUS:
                ambiguous.append(candidate)
    finally:
        ce.close()
    statuses += [
        (candidate, STATUS_MISSING_KEY)
        for ref, candidate in wanted.items()
        if ref not in found
    ]

    for candidate in ambiguous:
        # The fixed parse computes no gpu_utilization for this job, and an
        # upsert never deletes: remove the stale row here. The job_series
        # delete trigger nulls the pivot columns on its own.
        sess.delete(candidate.stat)
    # The deletes null the fetch records' pointer (ON DELETE SET NULL), and a
    # NULL pointer means a fetch that produced no statistics.
    sess.flush()
    for candidate in ambiguous:
        _repoint_fetch_record(sess, candidate.job)

    if dry_run:
        sess.rollback()
    else:
        sess.commit()
    return statuses


def _open_entry(cache: Cache, entry_time: datetime) -> CacheEntry | None:
    """The cache entry created exactly at `entry_time`, or None if the cache has none.

    read_from() excludes an entry of the exact time it starts from, hence
    starting one microsecond earlier; the first entry read is then the sought
    one, and nothing later is opened.
    """
    for ce in cache.read_from(entry_time - timedelta(microseconds=1)):
        if ce.get_entry_datetime() != entry_time:
            ce.close()
            return None
        return ce
    return None


def _ref_of_key(key: str) -> tuple[str, int, datetime]:
    cluster_name, job_id, submit_time = key.split("$")
    return (
        cluster_name,
        int(job_id),
        datetime.fromisoformat(submit_time).astimezone(UTC),
    )


def judge_raw_series(
    stat: JobStatisticDB, prom_stats: list[dict], cluster_name: str
) -> tuple[str, STATS | None]:
    """The row's status, with the statistics to write into it when it is rescaled."""
    metric_to_data: dict[str, list[dict]] = {
        metric: [] for metric in JOB_STATISTICS_METRIC_NAMES
    }
    for result in prom_stats:
        metric_to_data[result["metric"]["__name__"]].append(result)
    _, utilization = gpu_activity_series(metric_to_data)
    rescaled = rescale_gpu_utilization(utilization, metric_to_data, cluster_name)
    if rescaled is None:
        return STATUS_AMBIGUOUS, None
    if not _samples_changed(utilization, rescaled):
        return STATUS_UNCHANGED, None
    # As compute_job_statistics computes gpu_utilization.
    stats = compute_metric_statistics(rescaled, normalization=_percent)
    assert stats is not None  # a divided sample is a real one
    if _same_statistics(stat, stats):
        # Written by an earlier run, or by the fixed parse.
        return STATUS_UNCHANGED, None
    return STATUS_RESCALED, stats


def _samples_changed(before: list[dict], after: list[dict]) -> bool:
    """Whether the rescale divided any value; rescale returns `before` itself when it divides none."""
    old = [float(v) for s in before for _, v in s["values"]]
    new = [float(v) for s in after for _, v in s["values"]]
    return len(old) != len(new) or any(
        x != y and not (isnan(x) and isnan(y)) for x, y in zip(old, new, strict=True)
    )


def _same_statistics(stat: JobStatisticDB, stats: STATS) -> bool:
    """Whether the row holds exactly `stats`: same samples, same computation."""
    return all(getattr(stat, name) == value for name, value in stats.items())


def _repoint_fetch_record(sess: Session, job: SlurmJobDB) -> None:
    """Point the job's fetch record to a statistic the job still has, NULL if none."""
    remaining_id = sess.exec(
        select(func.min(col(JobStatisticDB.id))).where(JobStatisticDB.job_id == job.id)
    ).one()
    fetch_record = sess.exec(
        select(JobStatisticsFetchDateDB).where(
            JobStatisticsFetchDateDB.job_id == job.id
        )
    ).one()
    fetch_record.jobstatistic_id = remaining_id


def log_report(results: list[tuple[Candidate, str]], dry_run: bool) -> None:
    by_cluster: dict[str, Counter] = defaultdict(Counter)
    examples: dict[str, list[str]] = defaultdict(list)
    for candidate, status in results:
        by_cluster[candidate.cluster_name][status] += 1
        if status != STATUS_UNCHANGED and len(examples[status]) < NB_EXAMPLES:
            examples[status].append(candidate.label())

    for cluster in sorted(by_cluster):
        counts = by_cluster[cluster]
        details = ", ".join(f"{status}={n}" for status, n in sorted(counts.items()))
        logger.info(f"[{cluster}] {counts.total()} candidates: {details}")
    for status in sorted(examples):
        logger.warning(f"{status}, e.g. {', '.join(examples[status])}")

    rescaled = sum(s == STATUS_RESCALED for _, s in results)
    dropped = sum(s == STATUS_AMBIGUOUS for _, s in results)
    verb = "would be " if dry_run else ""
    logger.info(
        f"{len(results)} candidates: {rescaled} {verb}rescaled, {dropped} {verb}dropped"
    )


if __name__ == "__main__":
    main()
