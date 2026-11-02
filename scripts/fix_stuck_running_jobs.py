"""
Close runs stuck in RUNNING, from a fix file.

`estimate_stuck_running_jobs.py` finds the rows a corrupted slurm db left
stuck in RUNNING and writes a fix file saying how long each job actually ran
and the state to close it with. This applies that file: for each entry, it
finds the row by the fix-file identity (cluster_name, job_id, submit_time)
-- the database's own unique key, so one entry can match no more than one
row -- and writes `job_state`, `end_time` = the row's start_time + duration
and `elapsed_time` = duration. It writes nothing else; the `job_series`
trigger mirrors the three fields.

An entry whose row is not in the database is skipped, and a row that is no
longer RUNNING -- the slurm db healed, or somebody fixed it by hand -- is
left alone: only RUNNING rows are closed, so re-applying a fix file is a
no-op on everything it already fixed. Both cases are only counted.

Needs the configuration `sarc parse jobs` runs with (the database).
Dry run by default:

    SARC_CONFIG=<file> uv run python scripts/fix_stuck_running_jobs.py fix.json

Then apply with:

    SARC_CONFIG=<file> uv run python scripts/fix_stuck_running_jobs.py fix.json --no-dry-run
"""

import argparse
import json
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from pydantic import (
    AwareDatetime,
    BaseModel,
    PositiveFloat,
    ValidationError,
    field_validator,
)
from sqlmodel import Session, col, select

from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB
from sarc.models.job import SlurmState

logger = logging.getLogger("fix_stuck_running_jobs")

NB_EXAMPLES = 10

# The fix file format this script reads, written by the matching version of
# `estimate_stuck_running_jobs.py`.
FILE_VERSION = 1

STATUS_FIXED = "fixed"
STATUS_NO_ROW = "no_row"
STATUS_UNKNOWN_CLUSTER = "unknown_cluster"
STATUS_NOT_RUNNING = "not_running"


class FixEntry(BaseModel):
    """One job of the fix file: the identity, and how to close it."""

    cluster_name: str
    job_id: int
    submit_time: AwareDatetime
    duration: PositiveFloat
    state: SlurmState
    # How the estimate was made, only for the log.
    source: str = ""
    note: str = ""

    @field_validator("state")
    @classmethod
    def _not_running(cls, state: SlurmState) -> SlurmState:
        if state is SlurmState.RUNNING:
            raise ValueError(
                "RUNNING is the state being fixed, not one to close a job with"
            )
        return state


@dataclass(frozen=True)
class Result:
    entry: FixEntry
    status: str
    # What the row's state was, when it was not RUNNING.
    state: str | None = None
    # The row this entry closed, when it closed one.
    row_id: int | None = None


def entry_label(entry: FixEntry) -> str:
    return f"{entry.cluster_name}:{entry.job_id}@{entry.submit_time.isoformat()}"


def load_fix_file(path: Path) -> list[FixEntry]:
    """The entries of a fix file, refusing anything else."""
    try:
        document = json.loads(path.read_text())
    except (OSError, ValueError) as e:
        raise SystemExit(f"Cannot read {path}: {e}") from e
    if not isinstance(document, dict) or document.get("version") != FILE_VERSION:
        version = document.get("version") if isinstance(document, dict) else None
        raise SystemExit(
            f"{path}: fix file version {version!r}, this script applies {FILE_VERSION}"
        )
    try:
        return [FixEntry.model_validate(entry) for entry in document.get("jobs", [])]
    except ValidationError as e:
        raise SystemExit(f"{path}: invalid fix file entry: {e}") from e


def fix_stuck_running_jobs(
    sess: Session, entries: list[FixEntry], *, dry_run: bool
) -> list[Result]:
    """Close the rows of each entry, and report what happened to each.

    The whole file is one transaction, committed at the end: the corruption
    hits about a thousand jobs once every four years, batching would cost
    more in code than it saves in time.
    """
    results = [_fix_one(sess, entry, dry_run=dry_run) for entry in entries]
    if dry_run:
        sess.rollback()  # nothing was written; this only ends the transaction
    else:
        sess.commit()
    return results


def _fix_one(sess: Session, entry: FixEntry, *, dry_run: bool) -> Result:
    """Close the RUNNING row of one entry, if there is one."""
    cluster_id = SlurmClusterDB.id_by_name(sess, entry.cluster_name)
    if cluster_id is None:
        return Result(entry, STATUS_UNKNOWN_CLUSTER)
    row = sess.exec(
        select(SlurmJobDB).where(
            col(SlurmJobDB.cluster_id) == cluster_id,
            col(SlurmJobDB.job_id) == entry.job_id,
            col(SlurmJobDB.submit_time) == entry.submit_time,
        )
    ).one_or_none()  # the identity is the database's unique key
    if row is None:
        return Result(entry, STATUS_NO_ROW)
    if row.job_state != SlurmState.RUNNING:
        return Result(entry, STATUS_NOT_RUNNING, row.job_state.value)
    _close(row, entry, dry_run=dry_run)
    return Result(entry, STATUS_FIXED, row_id=row.id)


def _close(row: SlurmJobDB, entry: FixEntry, *, dry_run: bool) -> None:
    """Store the estimated end of the run on the row."""
    if dry_run:
        return
    assert row.start_time is not None  # a row stuck in RUNNING has started
    row.job_state = entry.state
    row.end_time = row.start_time + timedelta(seconds=entry.duration)
    row.elapsed_time = entry.duration


def log_report(results: list[Result], dry_run: bool) -> None:
    by_cluster: dict[str, Counter] = defaultdict(Counter)
    examples: dict[str, list[str]] = defaultdict(list)
    for result in results:
        label = result.status
        if result.status == STATUS_NOT_RUNNING:
            label = f"{result.status}:{result.state}"
        by_cluster[result.entry.cluster_name][label] += 1
        if result.status != STATUS_FIXED and len(examples[label]) < NB_EXAMPLES:
            examples[label].append(entry_label(result.entry))

    for cluster in sorted(by_cluster):
        counts = by_cluster[cluster]
        details = ", ".join(f"{label}={n}" for label, n in sorted(counts.items()))
        logger.info(f"[{cluster}] {counts.total()} fix file entries: {details}")
    for status in sorted(examples):
        logger.warning(f"{status}, e.g. {', '.join(examples[status])}")

    nb_fixed = sum(result.status == STATUS_FIXED for result in results)
    verb = "would be fixed" if dry_run else "fixed"
    logger.info(f"{len(results)} entries, {nb_fixed} {verb}")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Close runs stuck in RUNNING, from a fix file written by "
            "estimate_stuck_running_jobs.py."
        )
    )
    parser.add_argument("file", type=Path, help="the fix file to read and apply.")
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Do not write anything to the database.",
    )
    args = parser.parse_args()
    # force=True: importing sarc modules already put a root handler in place at
    # WARNING level, which would silently swallow this script's INFO report.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s::%(levelname)s::%(name)s::%(message)s",
        force=True,
    )

    entries = load_fix_file(args.file)
    logger.info(f"{len(entries)} entries in {args.file}")

    with config.db.session() as sess:
        results = fix_stuck_running_jobs(sess, entries, dry_run=args.dry_run)
    log_report(results, args.dry_run)


if __name__ == "__main__":
    main()
