"""
Ask the clusters themselves how the runs stuck in RUNNING ended, into the cache.

A corrupted slurm db on some clusters leaves finished jobs stuck in the
RUNNING state forever, and the periodic scrapes keep repeating that lie.
`estimate_stuck_running_jobs.py` guesses around it from what the cache holds;
this asks the source instead: for every row it finds stuck in RUNNING -- the
same rows, by the same criteria, as that script -- it connects to the cluster
the job came from and runs one sacct for that specific job (`-j <job_id>`),
outside of the periodic interval scraping.

A corrupted slurm db is not necessarily a permanently corrupted one: the job
record can heal on the cluster long before the next periodic scrape of its
interval rolls around, and a scrape of a job's own interval that starts
months later may never be run at all. The one-shot `-j` query sees both.

What the answer says decides what happens to it:

 - the answer has no record of the job: the row reports something the slurm
   db does not know; nothing is written and the row is only reported.
 - the record still says RUNNING: the slurm db tells the lie to this query
   too; nothing is written, the estimate script is the recourse.
 - the record's identity (submit_time) is not the row's: it is another run
   of that job id, not the run this row is stuck on; nothing is written.
 - otherwise the record closed the run: the raw answer is written to the
   jobs cache, under the same `<cluster>_<start>_<end>` key format the
   periodic `sarc fetch jobs` writes, so `sarc parse jobs` picks the entry
   up like any other fetch and the upsert closes the row with what the
   cluster itself reported -- state, end time, everything.

The cache write is the only writing this script does; it never touches the
database. Values are saved per cluster, in one cache entry per cluster at
the end of its scrape: a run interrupted mid-cluster costs only that
cluster's work, and the parser never sees a half-written entry (the cache
hides the `.current` file until it is complete).

Needs the configuration `sarc parse jobs` runs with: the database to find
the stuck rows, and the cache root and the cluster SSH configs to scrape
into and through. Dry run by default:

    SARC_CONFIG=<file> uv run python scripts/scrape_stuck_running_jobs.py

Then scrape and cache with:

    SARC_CONFIG=<file> uv run python scripts/scrape_stuck_running_jobs.py --no-dry-run

and close the rows with a plain:

    SARC_CONFIG=<file> sarc parse jobs
"""

import argparse
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from estimate_stuck_running_jobs import (
    NB_EXAMPLES,
    StuckJob,
    _utc_date,
    find_stuck_jobs,
    label,
)

from sarc.cache import Cache
from sarc.config import ClusterConfig, config
from sarc.models.job import SlurmState
from sarc.scraping.jobs_utils import DATE_FORMAT_HOUR, parse_raw

logger = logging.getLogger("scrape_stuck_running_jobs")

# The jobs cache, the one `sarc fetch jobs` writes and `sarc parse jobs`
# reads: what this script saves, that parse applies.
CACHE_SUBDIRECTORY = "jobs"

# What happened to one scraped job.
STATUS_SAVED = "saved"  # the cluster closed the run, its answer is in the cache
STATUS_RUNNING = "running"  # the cluster still tells the lie
STATUS_NOT_FOUND = "not_found"  # the slurm db does not know this job anymore
STATUS_ERROR = "error"  # ssh failed, or the answer did not parse


@dataclass(frozen=True)
class Outcome:
    """What one job's one-shot scrape gave back."""

    job: StuckJob
    status: str
    # The state the cluster reported, when it reported one.
    state: SlurmState | None = None
    # The cache key the raw answer was saved under, when it was saved.
    key: str | None = None
    # The raw answer itself, when it is worth caching.
    raw: bytes | None = None
    # Free-form: the error, or what the record's identity was.
    note: str = ""


def fetch_raw_job(cluster: ClusterConfig, job_id: int) -> bytes:
    """Ask one cluster about one specific job, however `fetch_raw` asks it."""
    # No `-S`/`-E` period and no account filter: `-j` names the job, and the
    # stuck ones are exactly the ones a period query keeps misfiring on.
    cmd = f"{cluster.sacct_bin} -X -j {job_id} --allusers --json --duplicates"
    logger.debug(f"{cluster.name} $ {cmd}")
    ssh = cluster.ssh
    ssh.config.run.env = {"TZ": "UTC"} if not cluster.ignore_tz_utc else {}
    results = ssh.run(cmd, hide=True)
    return results.stdout.encode("utf-8")


def cache_key(cluster_name: str, start: datetime, end: datetime) -> str:
    """A job-cache key for a one-shot scrape, in the fetch format.

    The period says what the value covers on the cluster: from the run's
    start to the moment it was asked about, which is the span this answer
    speaks for. `sarc parse jobs` reads it back as the scrape period.
    """
    return (
        f"{cluster_name}_{start.strftime(DATE_FORMAT_HOUR)}"
        f"_{end.strftime(DATE_FORMAT_HOUR)}"
    )


def scrape_one(cluster: ClusterConfig, job: StuckJob) -> Outcome:
    """Scrape one stuck job on its cluster and tell what the answer said.

    Raises when the scrape or the answer's parsing fails: the caller logs
    it as an error for this job and goes on with the next one.
    """
    scraped_start = job.start_time
    scraped_end = datetime.now(UTC)
    raw = fetch_raw_job(cluster, job.job_id)
    if b"{" not in raw:
        # sacct answers with nothing at all for a job it does not have.
        return Outcome(job, STATUS_NOT_FOUND, note="the answer was empty")

    record = None
    for entry in parse_raw(raw, cluster.name, scraped_start, scraped_end):  # ty: ignore[invalid-argument-type]
        if (
            entry is not None
            and entry["job_id"] == job.job_id
            and entry["submit_time"] == job.submit_time
        ):
            record = entry
            break
    if record is None:
        return Outcome(job, STATUS_NOT_FOUND, note="no record of the job")
    state = _state(record)
    if state is SlurmState.RUNNING:
        return Outcome(job, STATUS_RUNNING, state=state)
    # The run is closed on the cluster: hand the whole raw answer to the
    # cache, same format as a periodic fetch, for `sarc parse jobs` to apply.
    return Outcome(
        job,
        STATUS_SAVED,
        state=state,
        key=cache_key(cluster.name, scraped_start, scraped_end),  # ty: ignore[invalid-argument-type]
        raw=raw,
        note=record_note(record),
    )


def _state(record: dict) -> SlurmState | None:
    """The state of a parsed record, None if it is not one we know."""
    return SlurmState.__members__.get(str(record["job_state"]))


def record_note(record: dict) -> str:
    """What a closing record says about the end of the run, in one line."""
    if record["end_time"] is not None:
        return f"ended at {record['end_time'].isoformat()}"
    return f"with no end time and an elapsed time of {record['elapsed_time']}s"


def scrape_cluster(
    cache: Cache, cluster_name: str, jobs: list[StuckJob], *, dry_run: bool
) -> list[Outcome]:
    """Scrape every stuck job of one cluster and cache the closed ones.

    One cache entry per cluster, written once its whole scrape is done: the
    parser either sees all of a cluster's answers or none of them.
    """
    cluster = config.clusters.get(cluster_name)
    if cluster is None:
        # Without a way to ask, nothing was even attempted on the job.
        return [
            Outcome(job, STATUS_ERROR, note="no such cluster in the config")
            for job in jobs
        ]

    outcomes: list[Outcome] = []
    saved: list[tuple[str, bytes]] = []
    for job in jobs:
        logger.info(f"Scraping job ({job.cluster_name, job.job_id, job.submit_time})")
        try:
            outcome = scrape_one(cluster, job)
        except Exception as e:
            outcome = Outcome(job, STATUS_ERROR, note=f"{type(e).__name__}: {e}")
        outcomes.append(outcome)
        if outcome.status != STATUS_SAVED:
            # The lie simply continuing is the expected outcome of many a
            # scrape, not something to shout about; the rest is.
            level = (
                logging.INFO if outcome.state is SlurmState.RUNNING else logging.WARNING
            )
            logger.log(level, f"{label(job)}: {outcome.status} {outcome.note}".rstrip())
            continue
        logger.info(
            f"{label(job)}: the cluster says {outcome.state.value}, {outcome.note}"  # ty: ignore[unresolved-attribute]
        )
        # In dry run the scrape is all there is: the answer is reported and
        # dropped, the cache untouched.
        if not dry_run:
            saved.append((outcome.key, outcome.raw))  # ty: ignore[invalid-argument-type]

    if saved:
        with cache.create_entry(datetime.now(UTC)) as cache_entry:
            for key, raw in saved:
                cache_entry.add_value(key=key, value=raw)
    return outcomes


def log_report(outcomes: list[Outcome], dry_run: bool, took: timedelta) -> None:
    by_cluster: defaultdict[str, defaultdict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    examples: defaultdict[str, list[str]] = defaultdict(list)
    for outcome in outcomes:
        by_cluster[outcome.job.cluster_name][outcome.status] += 1
        if (
            outcome.status != STATUS_SAVED
            and len(examples[outcome.status]) < NB_EXAMPLES
        ):
            examples[outcome.status].append(label(outcome.job))

    for cluster_name in sorted(by_cluster):
        counts = by_cluster[cluster_name]
        details = ", ".join(f"{status}={n}" for status, n in sorted(counts.items()))
        logger.info(f"[{cluster_name}] {sum(counts.values())} stuck rows: {details}")
    for status in sorted(examples):
        logger.warning(f"{status}, e.g. {', '.join(examples[status])}")

    nb_saved = sum(o.status == STATUS_SAVED for o in outcomes)
    verb = "would be saved" if dry_run else "saved"
    where = "the cache" if not dry_run else "the cache (dry run, nothing written)"
    logger.info(
        f"{len(outcomes)} stuck rows, {nb_saved} closed by the clusters "
        f"{verb} to {where}, took {took}"
    )
    if nb_saved and not dry_run:
        logger.info("run `sarc parse jobs` to close the rows with these answers")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Scrape runs stuck in RUNNING one by one on their cluster, "
            "and cache what closes them for sarc parse jobs."
        )
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
        default=1.0,
        help=(
            "hours past the time limit before a RUNNING row counts as stuck "
            "(default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Scrape and report, but write nothing to the cache.",
    )
    args = parser.parse_args()
    # force=True: importing sarc modules already put a root handler in place at
    # WARNING level, which would silently swallow this script's INFO report.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s::%(levelname)s::%(name)s::%(message)s",
        force=True,
    )

    started = time.monotonic()
    cache = Cache(subdirectory=CACHE_SUBDIRECTORY)
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
    logger.info(f"Found {len(jobs)} stuck jobs")

    by_cluster: defaultdict[str, list[StuckJob]] = defaultdict(list)
    for job in jobs:
        by_cluster[job.cluster_name].append(job)
    outcomes: list[Outcome] = []
    for cluster_name, cluster_jobs in by_cluster.items():
        outcomes += scrape_cluster(
            cache, cluster_name, cluster_jobs, dry_run=args.dry_run
        )
    took = timedelta(seconds=round(time.monotonic() - started))
    log_report(outcomes, args.dry_run, took)


if __name__ == "__main__":
    main()
