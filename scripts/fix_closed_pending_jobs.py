"""
One-shot repair of closed runs stored as PENDING.

Until PR #704, the job parsers kept the first value of the sacct state list, so
every requeued run, ["PENDING", "REQUEUED"], was stored PENDING with a start
and an end time. For each such row, this parses again, with the fixed parser,
the cache value that last wrote it, and writes the state read there. It writes
`job_state` and nothing else; the `job_series` trigger mirrors it.

A row's `latest_scraped_start/end` are the bounds of the cache key
`<cluster>_<start>_<end>` its last parse came from, and the newest value of
that key is the one the parse read last. A row is written only if that value
lists its run with the row's end time and a flag state (RESIZING, REQUEUED or
REVOKED, which slurmdbd stores on a PENDING base). The report counts the other
rows by status, with a few examples.

Without `--dry-run`, the rows of a cache key are committed as soon as its value
is read: a run cut short can be started again, and only sees the rows still
PENDING.

Needs the configuration `sarc parse jobs` runs with (database and jobs cache).
Dry run by default:

    SARC_CONFIG=<file> uv run python scripts/fix_closed_pending_jobs.py

Then apply with:

    SARC_CONFIG=<file> uv run python scripts/fix_closed_pending_jobs.py --no-dry-run
"""

import argparse
import logging
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime

from sqlmodel import Session, col, select, update

from sarc.cache import Cache
from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB
from sarc.models.job import SlurmState
from sarc.scraping import jobs_utils
from sarc.scraping.jobs import parse_date

logger = logging.getLogger("fix_closed_pending_jobs")

NB_EXAMPLES = 10

FLAG_STATES = ("RESIZING", "REQUEUED", "REVOKED")

STATUS_FIXED = "fixed"
STATUS_STILL_PENDING = "still_pending"
STATUS_OTHER_STATE = "other_state"
STATUS_END_DIFFERS = "end_differs"
STATUS_RUN_ABSENT = "run_absent"
STATUS_PARSE_ERROR = "parse_error"
STATUS_NO_ENTRY = "no_entry"

type ClusterWindow = tuple[str, datetime | None, datetime | None]


@dataclass(frozen=True)
class Row:
    id: int
    cluster: str
    job_id: int
    submit_time: datetime
    end_time: datetime


@dataclass(frozen=True)
class Result:
    row: Row
    status: str
    state: str | None = None


def fix_closed_pending_jobs(sess: Session, dry_run: bool) -> None:
    rows_by_window = _closed_pending_rows(sess)
    if not rows_by_window:
        logger.info("No closed PENDING row: nothing to fix")
        return
    nb_rows = sum(len(rows) for rows in rows_by_window.values())
    logger.info(
        f"{nb_rows} closed PENDING rows in {len(rows_by_window)} cache keys, "
        f"walking the cache"
    )
    results: list[Result] = []
    for window, value in _newest_values(list(rows_by_window)):
        window_results = _results(value, window, rows_by_window.pop(window))
        if not dry_run:
            _write(sess, window_results)
        results.extend(window_results)
    # The windows left are in no cache entry.
    for rows in rows_by_window.values():
        results.extend(Result(row, STATUS_NO_ENTRY) for row in rows)
    log_report(results, dry_run)


def _closed_pending_rows(sess: Session) -> dict[ClusterWindow, list[Row]]:
    """The closed PENDING rows, by the window of the cache key that wrote them."""
    query = (
        select(
            SlurmJobDB.id,
            SlurmClusterDB.name,
            SlurmJobDB.job_id,
            SlurmJobDB.submit_time,
            SlurmJobDB.end_time,
            SlurmJobDB.latest_scraped_start,
            SlurmJobDB.latest_scraped_end,
        )
        .select_from(SlurmJobDB)
        .join(SlurmClusterDB, col(SlurmJobDB.cluster_id) == col(SlurmClusterDB.id))
        .where(
            col(SlurmJobDB.job_state) == SlurmState.PENDING,
            col(SlurmJobDB.end_time).is_not(None),
        )
    )
    rows_by_window: dict[ClusterWindow, list[Row]] = defaultdict(list)
    for row_id, cluster, job_id, submit_time, end_time, start, end in sess.exec(
        query
    ).all():
        row = Row(
            id=row_id,
            cluster=cluster,
            job_id=job_id,
            submit_time=submit_time,
            end_time=end_time,
        )
        rows_by_window[(cluster, start, end)].append(row)
    return rows_by_window


def _newest_values(
    windows: Iterable[ClusterWindow],
) -> Iterator[tuple[ClusterWindow, bytes]]:
    """Yield the newest cache value of each of `windows`, once."""
    left = set(windows)
    for entry in Cache(subdirectory="jobs").read_backward():
        # The parse reads an entry's values in order: the last one wins.
        # CacheEntry.items() decompresses every value, so read the zip directly.
        for info in reversed(entry._zf.infolist()):
            window = _window_of_key(info.filename)
            if window in left:
                left.remove(window)
                yield window, entry._zf.read(info)
        entry.close()
        if not left:
            return


def _window_of_key(key: str) -> ClusterWindow | None:
    """The (cluster, start, end) of a cache key, read as `parse_cache_entry` does."""
    parts = key.split("_")
    try:
        return parts[0], parse_date(parts[1]), parse_date(parts[2])
    except IndexError, ValueError:
        return None


def _results(value: bytes, window: ClusterWindow, rows: list[Row]) -> list[Result]:
    """The result of each row, read in the cache value of its window."""
    cluster, start, end = window
    try:
        jobs = {
            (job["job_id"], job["submit_time"]): job
            for job in jobs_utils.parse_raw(value, cluster, start, end)
            if job is not None
        }
    except Exception:
        logger.exception(f"Cannot parse the cache value of {window}")
        return [Result(row, STATUS_PARSE_ERROR) for row in rows]
    return [_result(row, jobs.get((row.job_id, row.submit_time))) for row in rows]


def _result(row: Row, job: dict | None) -> Result:
    if job is None:
        return Result(row, STATUS_RUN_ABSENT)
    state = job["job_state"]
    if job["end_time"] != row.end_time:
        return Result(row, STATUS_END_DIFFERS, state)
    if state in FLAG_STATES:
        return Result(row, STATUS_FIXED, state)
    if state == "PENDING":
        return Result(row, STATUS_STILL_PENDING, state)
    return Result(row, STATUS_OTHER_STATE, state)


def _write(sess: Session, results: list[Result]) -> None:
    """Write the state of the fixed rows, and commit."""
    for state in FLAG_STATES:
        ids = [
            result.row.id
            for result in results
            if result.status == STATUS_FIXED and result.state == state
        ]
        if ids:
            sess.exec(
                update(SlurmJobDB)
                .where(
                    col(SlurmJobDB.id).in_(ids),
                    col(SlurmJobDB.job_state) == SlurmState.PENDING,
                )
                .values(job_state=SlurmState(state))
            )
    sess.commit()


def log_report(results: list[Result], dry_run: bool) -> None:
    by_cluster: dict[str, Counter] = defaultdict(Counter)
    examples: dict[str, list[str]] = defaultdict(list)
    for result in results:
        row = result.row
        label = result.status
        if result.status in (STATUS_FIXED, STATUS_OTHER_STATE):
            label = f"{result.status}:{result.state}"
        by_cluster[row.cluster][label] += 1
        if result.status != STATUS_FIXED and len(examples[result.status]) < NB_EXAMPLES:
            examples[result.status].append(
                f"{row.cluster}:{row.job_id}@{row.submit_time.isoformat()}"
            )

    for cluster in sorted(by_cluster):
        counts = by_cluster[cluster]
        details = ", ".join(f"{label}={n}" for label, n in sorted(counts.items()))
        logger.info(f"[{cluster}] {counts.total()} closed PENDING rows: {details}")
    for status in sorted(examples):
        logger.warning(f"{status}, e.g. {', '.join(examples[status])}")

    nb_fixed = sum(result.status == STATUS_FIXED for result in results)
    verb = "would be fixed" if dry_run else "fixed"
    logger.info(f"{len(results)} closed PENDING rows, {nb_fixed} {verb}")


def main():
    parser = argparse.ArgumentParser(
        description="Fix closed runs stored as PENDING, from the jobs cache."
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Do not write anything to the database (default: dry run).",
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
        fix_closed_pending_jobs(sess, args.dry_run)


if __name__ == "__main__":
    main()
