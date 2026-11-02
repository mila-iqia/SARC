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

For every stuck row, the `HEURISTICS` below are asked in order how long the run
*actually* lasted; the first estimate wins, and a row no heuristic can
estimate is only reported, never written. A heuristic is a function

    def name(job: StuckJob) -> Estimate | None

returning an `Estimate`, or None when it cannot tell; it may crash, in which
case the failure is logged and the next heuristic is asked. To add a
heuristic, write it and put it in `HEURISTICS`, most trustworthy first.

The first heuristic is the jobs cache, which often still holds what the row
lost: the scrape that saw the run finish. It reads one job's own span of the
cache -- from its `start_time` to `CACHE_LOOKAHEAD` past its time limit, which
is two days or a few, not the six years the cache holds -- and keeps the first
record reporting a state the run cannot come back from, which gives both the
duration and the state to close the row with. A job the cache never closes is
left to the next heuristic.

The second heuristic is Prometheus: what the cache lost, the cluster's own
monitoring often still saw. The slurm-job-exporter samples every running job
on the node hosting it, so the run's samples stopping is the run being over.
Prometheus keys its data by job id and not by run though, and a requeued job
inherits the id of the run the row is stuck on: the two runs' samples are told
apart by the nodes they come from, since a requeue is allocated elsewhere --
the run's own data is what its start-time nodes report, and it ends where they
fall silent, or where they are left behind by nodes that never reported it. A
run whose samples never fall silent, or that predates what Prometheus holds,
is left without an estimate.

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
whatever the heuristics read (the jobs cache, and the clusters' Prometheus):

    SARC_CONFIG=<file> uv run python scripts/estimate_stuck_running_jobs.py -o fix.json
"""

import argparse
import json
import logging
import time
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zipfile import ZipFile

from prometheus_api_client.prometheus_connect import PrometheusConnect
from sqlalchemy import func
from sqlmodel import Session, col, select

from sarc.cache import Cache
from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB
from sarc.models.job import SlurmState
from sarc.scraping.jobs import parse_date
from sarc.scraping.jobs_utils import parse_raw

logger = logging.getLogger("estimate_stuck_running_jobs")

NB_EXAMPLES = 10

# The version of the fix file format, and the state a job gets when neither a
# heuristic nor --state asked for another one.
FILE_VERSION = 1
DEFAULT_STATE = SlurmState.NODE_FAIL

# The jobs cache, the one `sarc fetch jobs` writes and `sarc parse jobs` reads.
CACHE_SUBDIRECTORY = "jobs"

# Stateless besides its subdirectory, and its root comes from the config when it
# is used: the one instance is what every heuristic of every run reads.
JOBS_CACHE = Cache(subdirectory=CACHE_SUBDIRECTORY)

# How long past its time limit a job's span of the cache reaches: a run that
# really ended is reported closed by the next scrape or two, and a day covers a
# scrape of a slurm db still telling the lie. Past that span, a cache that says
# nothing has nothing left to say about this job.
CACHE_LOOKAHEAD = timedelta(days=1)

# How late a fetch may be and still be worth reading for a job. A cache entry is
# named for when it was fetched, and the period scraped on the cluster is in its
# keys; sacct tells what the slurm db said at fetch time, so a fetch long past
# the job's deadline only repeats the state that made the row stuck. The slack
# covers a scrape catching up on a backlog, and keeps a cluster missing from the
# cache from being walked to the end of it once per job.
CACHE_FETCH_SLACK = timedelta(days=30)

# The states that say a run stopped running, however it stopped. Not the ones
# of a run still winding down (COMPLETING, STAGE_OUT, SIGNALING, RESIZING) nor
# of one still queued or setting up (PENDING, CONFIGURING, RESV_DEL_HOLD), and
# not the paused ones either (STOPPED, SUSPENDED): all of those can run again.
CLOSED_STATES = frozenset(
    {
        SlurmState.BOOT_FAIL,
        SlurmState.CANCELLED,
        SlurmState.COMPLETED,
        SlurmState.DEADLINE,
        SlurmState.FAILED,
        SlurmState.NODE_FAIL,
        SlurmState.OUT_OF_MEMORY,
        SlurmState.PREEMPTED,
        SlurmState.REQUEUE_FED,
        SlurmState.REQUEUE_HOLD,
        SlurmState.REQUEUED,
        SlurmState.REVOKED,
        SlurmState.SPECIAL_EXIT,
        SlurmState.TIMEOUT,
    }
)

# What the fix file, and this script's own reports, identify a job by.
type JobKey = tuple[str, int, datetime]

# The cache values this run could not read, kept to only complain about each of
# them once: every job whose span covers one reads it as many times as there are
# jobs, and has nothing new to say about it the second time.
UNPARSABLE: set[str] = set()


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
    # The nodes the row says the run got: what the prometheus heuristic ties
    # the run's samples to. Empty when the row never had any.
    nodes: tuple[str, ...] = ()

    @property
    def key(self) -> JobKey:
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


@dataclass(frozen=True)
class CacheState:
    """What the jobs cache says ended one stuck run."""

    # The state it reported, once the run was over.
    state: SlurmState
    # How long the run lasted as that record says, > 0.
    duration: float
    # When the record says the run ended, None when it only has an elapsed time.
    end_time: datetime | None
    # The cache key of the value the record came from: the cluster and the period
    # it was scraped over, which is the date that actually means something here.
    key: str
    # When that value was fetched.
    seen_at: datetime


# What one heuristic gives: a callable that tells how a stuck run ended, or
# None when it cannot tell.
type Heuristic = Callable[[StuckJob], Estimate | None]


def closed_state_in_cache(job: StuckJob) -> Estimate | None:
    """Take the first state the jobs cache reported once the run was over.

    The corruption that keeps the row RUNNING does not always keep the end of
    the run out of the cache: a scrape of the job's own window saw it finish,
    and its record says both when and how. The row lost that record, so it is
    what the cache can give back, state and duration together.
    """
    seen = read_cache_state(JOBS_CACHE, job)
    if seen is None:
        return None
    if seen.end_time is not None:
        ended = f"ended at {seen.end_time.isoformat()}"
    else:
        ended = f"with no end time and an elapsed time of {seen.duration:.0f}s"
    return Estimate(
        duration=seen.duration,
        state=seen.state,
        note=(
            f"the jobs cache read {seen.state.value} for the run, {ended}, "
            f"in the value {seen.key} fetched at {seen.seen_at.isoformat()}"
        ),
    )


# --- What the cluster's Prometheus says about the run ------------------------

# The metric families the slurm-job-exporter exports from every job's cgroup,
# GPU job or not: wherever one of them has samples, the run was alive there.
PROM_METRICS = ("slurm_job_core_usage", "slurm_job_memory_usage")

# How far past its time limit the data of a run is looked for: slurm kills a
# run at its limit, and a day covers the kill, the epilog and the exporter
# noticing. Samples still coming at the end of the window say nothing about
# an end, and the job is left to the next heuristic.
PROM_LOOKAHEAD = timedelta(days=1)

# Prometheus knows job ids, not runs: a requeued job inherits the id, and with
# it the series of the run before it. The run's own share of the data is what
# the nodes reporting within this much of its start keep reporting: a requeue
# gets a fresh allocation and reports from other nodes, and that change of
# node set is where the old run's data ends. Two minutes covers the time the
# exporter takes to publish the job's first samples.
PROM_SEED_WINDOW = 120.0  # seconds

# The exporter is sampled about every 30s, so queries step at that interval:
# a range query hands back what was the freshest sample at one of its
# evaluations, so a coarser step would hide samples between two of them and
# blur the end by as much. One query is cut into chunks of that many
# evaluations, as Prometheus caps one at 20k points per series.
PROM_STEP = 30.0  # seconds
PROM_CHUNK_POINTS = 14_400  # evaluations per query, five days of them

# The silence that ends the run: with samples every ~30s, minutes of nothing
# is the exporter gone for good, not a scrape missed. What this cannot see is
# the requeue that restarts on the very same nodes with less than that in
# between -- its data simply continues, and the estimate runs long.
PROM_END_GAP = 300.0  # seconds

# The clusters whose Prometheus this run could not read, kept to only
# complain once: every later job of theirs fails the very same query.
PROM_FAILED: set[str] = set()


def _prom_node(labels: dict) -> str | None:
    """The node one series comes from: the exporter's instance, port dropped."""
    instance = labels.get("instance") or labels.get("node")
    if not instance:
        return None
    return instance.split(":")[0].casefold()


def _prom_samples(
    prom: PrometheusConnect, job_id: int, lo: datetime, hi: datetime
) -> set[tuple[float, str]]:
    """(time, node) of every sample Prometheus holds for a job id in [lo, hi].

    Raises on any failed query: a cluster whose Prometheus does not answer is
    not a cluster whose data can be estimated from.
    """
    selector = (
        '{__name__=~"^(' + "|".join(PROM_METRICS) + f')$", slurmjobid=~"^{job_id}$"}}'
    )
    samples: set[tuple[float, str]] = set()
    start = lo
    while start < hi:
        stop = min(start + timedelta(seconds=PROM_STEP * PROM_CHUNK_POINTS), hi)
        for series in prom.custom_query_range(
            selector, start, stop, step=f"{PROM_STEP:.0f}s"
        ):
            node = _prom_node(series.get("metric", {}))
            if node is None:
                continue  # a series that does not say what node it came from
            samples.update((float(ts), node) for ts, _ in series.get("values", []))
        start = stop
    return samples


def _run_end_in_samples(
    job: StuckJob, samples: set[tuple[float, str]], lo_ts: float, hi_ts: float
) -> tuple[float, set[str], set[str]] | None:
    """Where the run's own samples fall silent: (last, its nodes, later ones).

    None when the samples cannot say: none of them at the run's start, or none
    of them from a node the row had, or none of them silent before the end of
    the window -- a run still reporting there ended outside of it.
    """
    seed = {node for t, node in samples if t <= lo_ts + PROM_SEED_WINDOW}
    if not seed:
        # Data for the id, but none of it at the run's start: nothing ties it
        # to this row -- a later requeue, or a scrape of the id's whole life
        # that missed this run.
        logger.debug(
            f"{label(job)}: prometheus data for the id does not reach its start"
        )
        return None
    if job.nodes and seed.isdisjoint({n.casefold() for n in job.nodes}):
        # The id was reported at this row's start from nodes the row never
        # had: that is another run of the id, not this one's data.
        logger.debug(
            f"{label(job)}: the data starts on {sorted(seed)}, "
            f"not on the row's {sorted(job.nodes)}"
        )
        return None

    # Walk the run's own samples from the start, and stop at the first silence
    # no scrape explains: that is where the run ends.
    times = sorted(t for t, node in samples if node in seed)
    last = times[0]
    for t in times[1:]:
        if t - last > PROM_END_GAP:
            break
        last = t
    if hi_ts - last <= PROM_END_GAP:
        # Still reporting at the end of the window: an end is not in there --
        # a requeued run without a visible seam, or a row not stuck at all.
        return None
    return last, seed, {node for t, node in samples if t > last + PROM_END_GAP}


def stopped_data_in_prometheus(job: StuckJob) -> Estimate | None:
    """Estimate the run ended when the cluster's Prometheus stopped seeing it.

    The slurm-job-exporter samples every job's cgroup on the node hosting it,
    GPU job or not: its samples falling silent is the run being over, whatever
    the row still claims. Prometheus keys its data by job id and not by run
    though, so a requeued job shares the series of the run this row is stuck
    on; the run's own share of the data is the samples of the nodes it
    reported from at its start, walked from the start until silence. A
    requeue is allocated elsewhere and reports from other nodes, which do not
    extend the run, and the rare one restarting on the same nodes is cut by
    the silence its pending leaves. Samples that never fall silent end the run
    nowhere, and the job is left without an estimate; so is one too old for
    Prometheus to still hold its data, and so is one whose data the row's own
    nodes do not appear in at all -- that data belongs to another run.

    The state stays the default one: samples stopping says when the run ended,
    never how.
    """
    cluster = config.clusters.get(job.cluster_name)
    if cluster is None or cluster.prometheus_url is None:
        return None
    lo_ts = job.start_time.timestamp()
    hi = min(
        job.start_time + timedelta(seconds=job.time_limit or 0) + PROM_LOOKAHEAD,
        datetime.now(UTC),
    )
    try:
        samples = _prom_samples(cluster.prometheus, job.job_id, job.start_time, hi)
    except Exception as e:
        if job.cluster_name in PROM_FAILED:
            logger.debug(f"{label(job)}: prometheus still unreadable: {e}")
        else:
            PROM_FAILED.add(job.cluster_name)
            logger.warning(
                f"[prometheus][{job.cluster_name}] cannot read the job series, "
                f"e.g. on {label(job)}: {type(e).__name__}: {e}"
            )
        return None
    # One step of tolerance before the start covers the scrape taken a moment
    # before slurm stamped it; later data is bounded by the window itself.
    samples = {
        (t, node) for t, node in samples if lo_ts - PROM_STEP <= t <= hi.timestamp()
    }
    if not samples:
        logger.debug(f"{label(job)}: prometheus has no samples for the id")
        return None

    seen = _run_end_in_samples(job, samples, lo_ts, hi.timestamp())
    if seen is None:
        return None
    last, seed, resumed = seen
    duration = last - lo_ts
    if duration <= 0:
        return None

    nodes = ", ".join(sorted(seed))
    ended = datetime.fromtimestamp(last, UTC).isoformat()
    note = (
        f"prometheus samples on the run's nodes ({nodes}) fall silent after "
        f"{ended}, {duration:.0f}s after the start"
    )
    if resumed - seed:
        note += f"; the id reports again on {', '.join(sorted(resumed - seed))}"
    elif resumed:
        note += (
            "; the id keeps reporting from the same nodes, a requeue that did not move"
        )
    return Estimate(duration=duration, note=note)


# Asked in order for every stuck job, the first estimate wins. Add heuristics
# here, most trustworthy first. None means "cannot tell" and asks the next one.
HEURISTICS: list[Heuristic] = [closed_state_in_cache, stopped_data_in_prometheus]


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
                nodes=tuple(row.nodes or ()),
            )
        )
    return jobs


@dataclass(frozen=True)
class _Window:
    """One scraped period of the jobs cache: whose it is, which, and when."""

    # The key the value is stored under, `<cluster>_<start>_<end>`.
    key: str
    cluster: str
    start: datetime
    end: datetime
    # When it was fetched, which is when slurm was asked about the run.
    fetched_at: datetime


def read_cache_state(cache: Cache, job: StuckJob) -> CacheState | None:
    """What the jobs cache says ended one run, read over its own span of it.

    The span goes from the job's `start_time` to `CACHE_LOOKAHEAD` past its time
    limit: a run that really ended was reported closed within a scrape or two of
    its limit, so a cache that never said so has nothing left to say about it.

    The cache holds some six years, so what a job costs is all in how much of it
    gets walked. Only the entries from the start of the span on are, only the
    values of the job's cluster among them are read, and only those scraped over
    the span are decompressed: the entry's own date says when it was fetched,
    while the period it scraped on the cluster is in its keys. A one-hour job
    reads a couple of days of cache, not six years.
    """
    # Every job this script finds has a time limit; one without it would be
    # looked for for the lookahead alone, which is a sane enough span.
    deadline = job.start_time + timedelta(seconds=job.time_limit or 0) + CACHE_LOOKAHEAD
    # `Cache._paths_from` and not `read_from`, which would read every entry of
    # the span whole into memory: this walks the paths and opens a zip only to
    # look at its keys.
    for path, fetched_at in cache._paths_from(job.start_time):
        if fetched_at > deadline + CACHE_FETCH_SLACK:
            # The backstop, not the bound: the scan ends on its own when the
            # periods scraped go past the deadline, below. Without this, a
            # cluster the cache does not hold would be looked for to the end of
            # it -- once per job.
            break
        closed: CacheState | None = None
        scraped_past = False
        with ZipFile(path) as zf:
            for info in zf.infolist():
                window = _window_of_key(info.filename, fetched_at)
                if window is None or window.cluster != job.cluster_name:
                    continue
                if window.end <= job.start_time:
                    continue  # the run had not started over this period
                if window.start > deadline:
                    scraped_past = True
                    continue
                closed = _read_value(zf.read(info), job, window)
                if closed is not None:
                    break
        if closed is not None:
            logger.debug(
                f"{label(job)}: closed as {closed.state.value} in {closed.key}"
            )
            return closed
        if scraped_past:
            # Scraping only goes forward: no later entry covers this span again.
            break
    logger.debug(f"{label(job)}: no closed state in the span the cache holds")
    return None


def _read_value(raw: bytes, job: StuckJob, window: _Window) -> CacheState | None:
    """What one scraped value says ended the run, if it says anything."""
    try:
        for record in parse_raw(raw, window.cluster, window.start, window.end):
            closed = _read_record(record, job, window)
            if closed is not None:
                return closed
    except Exception as e:
        # A corrupt value only loses its own period: the next one, or a later
        # entry, says as much about this run. Every job whose span covers it
        # reads it, so it is only worth complaining about once.
        if window.key in UNPARSABLE:
            logger.debug(f"cannot parse the cache value of {window.key}: {e}")
        else:
            UNPARSABLE.add(window.key)
            logger.warning(f"cannot parse the cache value of {window.key}: {e}")
    return None


def _read_record(
    record: dict | None, job: StuckJob, window: _Window
) -> CacheState | None:
    """What one record of a scraped value says ended the run, if anything.

    None for a record that is not this run, or is it but still running, or found
    it closed without saying when: a later value of the span may know better.
    """
    if record is None:  # `parse_raw` skips the records it cannot convert
        return None
    if record["job_id"] != job.job_id or record["submit_time"] != job.submit_time:
        return None  # another run of this id, a requeued one
    state = _closed_state(record)
    duration = _duration(job, record)
    if state is None or duration is None:
        return None
    return CacheState(
        state=state,
        duration=duration,
        end_time=record["end_time"],
        key=window.key,
        seen_at=window.fetched_at,
    )


def _window_of_key(key: str, fetched_at: datetime) -> _Window | None:
    """What a cache key says its value holds, None if it is not a job value."""
    parts = key.split("_")
    if len(parts) != 3:
        return None
    try:
        return _Window(
            key, parts[0], parse_date(parts[1]), parse_date(parts[2]), fetched_at
        )
    except ValueError:
        return None


def _closed_state(record: dict) -> SlurmState | None:
    """The state of a cache record, when it says the run stopped running."""
    state = SlurmState.__members__.get(str(record["job_state"]))
    return state if state in CLOSED_STATES else None


def _duration(job: StuckJob, record: dict) -> float | None:
    """How long the run lasted as one cache record says, None if it cannot say.

    The record's end time less the *row's* start time, since the fix file
    closes that row with `start_time + duration` and lands on this end time; the
    record's own elapsed time when it has none.
    """
    end_time = record["end_time"]
    if end_time is not None:
        duration = (end_time - job.start_time).total_seconds()
        return duration if duration > 0 else None
    elapsed_time = record["elapsed_time"]
    return float(elapsed_time) if elapsed_time > 0 else None


def estimate_durations(
    jobs: Sequence[StuckJob], default_state: SlurmState
) -> tuple[list[dict], list[StuckJob]]:
    """Ask the heuristics about each job: (fix file entries, jobs left)."""
    entries: list[dict] = []
    no_estimate: list[StuckJob] = []
    for job in jobs:
        logger.info(
            f"Checking for job ({job.cluster_name, job.job_id, job.submit_time})"
        )
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
    took: timedelta,
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
    if UNPARSABLE:
        examples = ", ".join(sorted(UNPARSABLE)[:NB_EXAMPLES])
        logger.warning(f"{len(UNPARSABLE)} cache values unparsable, e.g. {examples}")
    if PROM_FAILED:
        examples = ", ".join(sorted(PROM_FAILED)[:NB_EXAMPLES])
        logger.warning(
            f"prometheus unreadable on {len(PROM_FAILED)} clusters, e.g. {examples}"
        )
    verb = "without an estimate" if no_estimate else "all estimated"
    logger.info(
        f"{len(jobs)} stuck rows, {len(entries)} estimated ({verb}), "
        f"fix file {path}, took {took}"
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
        default=1.0,
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

    started = time.monotonic()
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
    else:
        logger.info(f"Found {len(jobs)} stuck jobs")
    entries, no_estimate = estimate_durations(jobs, args.state)
    took = timedelta(seconds=round(time.monotonic() - started))
    write_fix_file(args.output, entries, args.state)
    log_report(jobs, entries, no_estimate, args.output, took)


if __name__ == "__main__":
    main()
