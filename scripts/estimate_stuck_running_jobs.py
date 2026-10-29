"""
Estimate when runs stuck in RUNNING really ended, into a fix file.

A corrupted slurm db on some clusters leaves finished jobs stuck in the
RUNNING state forever, sacct reporting them with an elapsed time growing
past their time limit. The parser no longer lets those records overwrite a
job it already has (`sarc.scraping.jobs.parse_cache_entry`), so the rows
stop being updated but keep a bogus RUNNING state, a missing end time and a
stale elapsed time. This finds those rows -- it reads the database and never
writes it -- and writes a fix file that `fix_stuck_running_jobs.py` applies.

A RUNNING row counts as stuck when its time limit was reached more than
`--grace` hours ago: slurm ends a run that hits its limit, so a row still
RUNNING that long past `start_time + time_limit` can only be the corrupted
state. A row without a time limit cannot be told this way and is left alone.

For every stuck row, the `HEURISTICS` below are asked in order how long the
run *actually* lasted; the first estimate wins, and a row no heuristic can
estimate is only reported, never written. A heuristic is a function

    def name(job: StuckJob, ctx: HeuristicContext) -> Estimate | None

returning an `Estimate`, or None when it cannot tell. It may use the shared
`ctx` to reach the clusters' prometheus (`ctx.has_prometheus`,
`ctx.prometheus`) and the jobs cache (`ctx.jobs_cache`), and it may crash: a
failure is logged and the next heuristic is asked. To add a heuristic, write
it and put it in `HEURISTICS`, most trustworthy first.

The fix file is json: one entry per estimated job, the job identified by
(cluster_name, job_id, submit_time) -- the database's own unique key, so one
row matches each entry -- its duration in seconds, and the state to close it
with -- `Estimate.state` per job, else `--state`, NODE_FAIL by default:

    {
      "version": 1,
      "generated_at": "2026-10-29T12:00:00+00:00",
      "default_state": "NODE_FAIL",
      "jobs": [
        {
          "cluster_name": "narval",
          "job_id": 123,
          "submit_time": "2026-09-01T00:00:00+00:00",
          "duration": 7200.0,
          "state": "NODE_FAIL",
          "source": "name_of_the_heuristic",
          "note": "what the heuristic saw, free-form"
        }
      ]
    }

Needs the configuration `sarc parse jobs` runs with: the database, and
whatever the heuristics read (per-cluster prometheus urls, the jobs cache):

    SARC_CONFIG=<file> uv run python scripts/estimate_stuck_running_jobs.py -o fix.json
"""

import argparse
import json
import logging
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import func
from sqlmodel import Session, col, select

from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB
from sarc.models.job import SlurmState

logger = logging.getLogger("estimate_stuck_running_jobs")

NB_EXAMPLES = 10

# The version of the fix file format, and the state a job gets when neither a
# heuristic nor --state asked for another one.
FILE_VERSION = 1
DEFAULT_STATE = SlurmState.NODE_FAIL


@dataclass(frozen=True)
class StuckJob:
    """One row stuck in RUNNING, with everything a heuristic may look at."""

    # The fix-file identity of the job: the database's own unique key, so it
    # picks exactly the one row this StuckJob describes.
    cluster_name: str
    job_id: int
    submit_time: datetime

    # What the database says about the run.
    start_time: datetime
    time_limit: int | None
    elapsed_time: float
    latest_scraped_start: datetime | None
    latest_scraped_end: datetime | None
    # And the allocation it was running on.
    array_job_id: int | None
    task_id: int | None
    partition: str
    account: str
    cluster_user: str
    name: str
    nodes: tuple[str, ...]

    @property
    def key(self) -> tuple[str, int, datetime]:
        """The fix-file identity of this job."""
        return (self.cluster_name, self.job_id, self.submit_time)


@dataclass(frozen=True)
class Estimate:
    """What a heuristic says about one stuck job."""

    # How long the job actually ran, in seconds (> 0).
    duration: float
    # The state to close the job with, None keeping the default one.
    state: SlurmState | None = None
    # Free-form, ends up in the fix file for the human reviewing it.
    note: str = ""


type Heuristic = Callable[[StuckJob], Estimate | None]

# Asked in order for every stuck job, the first estimate wins. Add heuristics
# here, most trustworthy first. None means "cannot tell" and asks the next
# one; for example:
#
#     def last_sample(job: StuckJob, ctx: HeuristicContext) -> Estimate | None:
#         """Take the last point prometheus has for the job as its real end."""
#         if not ctx.has_prometheus(job.cluster_name):
#             return None
#         conn = ctx.prometheus(job.cluster_name)
#         ...
#         return Estimate(duration=..., note="last prometheus sample at ...")
HEURISTICS: list[Heuristic] = []


def label(job: StuckJob) -> str:
    return f"{job.cluster_name}:{job.job_id}@{job.submit_time.isoformat()}"


def find_stuck_jobs(
    sess: Session,
    *,
    cluster_names: Sequence[str] | None = None,
    since: datetime | None = None,
    grace: timedelta,
) -> list[StuckJob]:
    """The rows stuck in RUNNING, oldest first.

    A row is stuck when its time limit was reached more than `grace` ago:
    only the corrupted slurm state leaves a RUNNING row that old, and the
    grace period keeps a run that just hit its limit but was not rescraped
    yet out. Rows without a time limit cannot be told, and are not returned.
    """
    cutoff = datetime.now(UTC) - grace
    query = (
        select(SlurmJobDB, SlurmClusterDB.name)
        .join(SlurmClusterDB, col(SlurmJobDB.cluster_id) == col(SlurmClusterDB.id))
        .where(
            col(SlurmJobDB.job_state) == SlurmState.RUNNING,
            col(SlurmJobDB.start_time).is_not(None),
            col(SlurmJobDB.time_limit).is_not(None),
            # In epochs, as the limit's seconds join one of the operands.
            func.extract("epoch", col(SlurmJobDB.start_time))
            + col(SlurmJobDB.time_limit)
            < cutoff.timestamp(),
        )
        .order_by(
            col(SlurmClusterDB.name),
            col(SlurmJobDB.submit_time),
            col(SlurmJobDB.job_id),
        )
    )
    if cluster_names:
        query = query.where(col(SlurmClusterDB.name).in_(cluster_names))
    if since is not None:
        query = query.where(col(SlurmJobDB.submit_time) >= since)

    jobs: list[StuckJob] = []
    for row, cluster_name in sess.exec(query).all():
        assert row.start_time is not None  # the where clause promises it
        jobs.append(
            StuckJob(
                cluster_name=cluster_name,
                job_id=row.job_id,
                submit_time=row.submit_time,
                start_time=row.start_time,
                time_limit=row.time_limit,
                elapsed_time=row.elapsed_time,
                latest_scraped_start=row.latest_scraped_start,
                latest_scraped_end=row.latest_scraped_end,
                array_job_id=row.array_job_id,
                task_id=row.task_id,
                partition=row.partition,
                account=row.account,
                cluster_user=row.cluster_user,
                name=row.name,
                nodes=tuple(row.nodes),
            )
        )
    return jobs


def estimate_durations(
    jobs: Sequence[StuckJob], default_state: SlurmState
) -> tuple[list[dict], list[StuckJob]]:
    """Ask the heuristics about each job: (fix file entries, jobs left)."""
    entries: list[dict] = []
    no_estimate: list[StuckJob] = []
    for job in jobs:
        found = _first_estimate(job)
        if found is None:
            no_estimate.append(job)
            continue
        source, estimate = found
        entries.append(
            {
                "cluster_name": job.cluster_name,
                "job_id": job.job_id,
                "submit_time": job.submit_time.isoformat(),
                "duration": estimate.duration,
                "state": (estimate.state or default_state).value,
                "source": source,
                "note": estimate.note,
            }
        )
    return entries, no_estimate


def _first_estimate(job: StuckJob) -> tuple[str, Estimate] | None:
    """The first estimate the heuristics give for this job, with its source."""
    for heuristic in HEURISTICS:
        # A heuristic is a function, but it need not be one `__name__` describes.
        source = getattr(heuristic, "__name__", repr(heuristic))
        try:
            estimate = heuristic(job)
        except Exception:
            logger.exception(f"{source} failed on {label(job)}, asking the next one")
            continue
        if estimate is None:
            continue
        if estimate.duration <= 0:
            logger.warning(
                f"{source} estimated {label(job)} at "
                f"{estimate.duration}s: ignored, asking the next one"
            )
            continue
        if job.time_limit is not None and estimate.duration > job.time_limit:
            # No run outlives its limit -- unless the limit was altered, which
            # slurm can do. The estimate is kept, but it is worth a look.
            logger.warning(
                f"{source} estimated {label(job)} at "
                f"{estimate.duration}s, past its limit of {job.time_limit}s"
            )
        return source, estimate
    return None


def write_fix_file(
    path: Path, entries: Sequence[dict], default_state: SlurmState
) -> None:
    """Write the fix file for `fix_stuck_running_jobs.py` to apply."""
    entries = sorted(
        entries, key=lambda e: (e["cluster_name"], e["submit_time"], e["job_id"])
    )
    document = {
        "version": FILE_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "default_state": default_state.value,
        "jobs": list(entries),
    }
    path.write_text(json.dumps(document, indent=2) + "\n")


def log_report(
    jobs: Sequence[StuckJob],
    entries: Sequence[dict],
    no_estimate: Sequence[StuckJob],
    path: Path,
) -> None:
    by_cluster: defaultdict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for job in jobs:
        by_cluster[job.cluster_name]["stuck"] += 1
    for entry in entries:
        by_cluster[entry["cluster_name"]]["estimated"] += 1
    for cluster in sorted(by_cluster):
        counts = by_cluster[cluster]
        details = ", ".join(f"{what}={n}" for what, n in sorted(counts.items()))
        logger.info(f"[{cluster}] {counts['stuck']} stuck rows: {details}")
    for job in list(no_estimate)[:NB_EXAMPLES]:
        logger.warning(f"no estimate, e.g. {label(job)}")
    verb = "without an estimate" if no_estimate else "all estimated"
    logger.info(
        f"{len(jobs)} stuck rows, {len(entries)} estimated ({verb}), fix file {path}"
    )


def _utc_date(value: str) -> datetime:
    """An ISO date as a UTC datetime: naive dates are read as UTC."""
    date = datetime.fromisoformat(value)
    return date if date.tzinfo is not None else date.replace(tzinfo=UTC)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Estimate the real duration of runs stuck in RUNNING, "
            "into a fix file for fix_stuck_running_jobs.py."
        )
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("stuck_running_jobs.json"),
        help="the fix file to write (default: %(default)s).",
    )
    parser.add_argument(
        "--cluster",
        action="append",
        metavar="NAME",
        help="only consider this cluster, repeatable (default: all clusters).",
    )
    parser.add_argument(
        "--since",
        type=_utc_date,
        metavar="ISO_DATE",
        help="only consider jobs submitted at or after this date.",
    )
    parser.add_argument(
        "--grace",
        type=float,
        default=24.0,
        help=(
            "hours past the time limit before a RUNNING row counts as stuck "
            "(default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--state",
        type=SlurmState,
        default=DEFAULT_STATE,
        metavar="STATE",
        help=(
            "the state to close jobs with, unless a heuristic says otherwise "
            f"(default: {DEFAULT_STATE.value})."
        ),
    )
    args = parser.parse_args()
    # force=True: importing sarc modules already put a root handler in place at
    # WARNING level, which would silently swallow this script's INFO report.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s::%(levelname)s::%(name)s::%(message)s",
        force=True,
    )
    if not HEURISTICS:
        logger.warning(
            "No heuristic registered in HEURISTICS: "
            "every stuck job will be reported without an estimate."
        )

    with config.db.session() as sess:
        jobs = find_stuck_jobs(
            sess,
            cluster_names=args.cluster,
            since=args.since,
            grace=timedelta(hours=args.grace),
        )
    if not jobs:
        logger.info("No row stuck in RUNNING")
        return
    entries, no_estimate = estimate_durations(jobs, args.state)
    write_fix_file(args.output, entries, args.state)
    log_report(jobs, entries, no_estimate, args.output)


if __name__ == "__main__":
    main()
