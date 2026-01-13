from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from simple_parsing import field

from sarc.config import config, UTC, TZLOCAL
from sarc.errors import ClusterNotFound

logger = logging.getLogger(__name__)


DATE_FORMAT_HOUR = "%Y-%m-%dT%H:%M"


def _str_to_dt(dt_str: str) -> datetime:
    return datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=UTC)


def _str_to_extended_dt(dt_str: str) -> datetime:
    """Parse date up to minute, with format %Y-%m-%dT%H:%M"""
    return datetime.strptime(dt_str, DATE_FORMAT_HOUR).replace(tzinfo=UTC)


def _time_auto_first_date(cluster_name: str, end_field: str) -> datetime:
    # get the last valid date in the database for the cluster
    # pylint: disable=broad-exception-raised
    db = config().mongo.database_instance
    db_collection = db.clusters
    cluster = db_collection.find_one({"cluster_name": cluster_name})
    if cluster is None:
        raise ClusterNotFound(f"Cluster {cluster_name} not found in database")
    start_date = cluster["start_date"]
    logger.info(f"start_date={start_date}")
    end_time = cluster[end_field]
    logger.info(f"{end_field}={end_time}")
    if end_time is None:
        # Use cluster start date
        # NB: Cluster start date is a day, like YYYY-MM-DD
        return _str_to_dt(start_date)
    # Use cluster end time for sacct
    # Cluster end time is an hour, like YYYY-MM-DDTHH:mm
    return _str_to_extended_dt(end_time)


def parse_intervals(intervals: list[str]) -> list[tuple[datetime, datetime]]:
    regex_interval = re.compile(
        r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})-(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})$", re.ASCII
    )
    parsed_intervals = []
    for interval in intervals:
        match = regex_interval.fullmatch(interval)
        if match is None:
            raise ValueError(f"Invalid interval {interval}")
        date_from = _str_to_extended_dt(match.group(1))
        date_to = _str_to_extended_dt(match.group(2))
        if date_from > date_to:
            raise ValueError(f"Interval: {date_from} > {date_to}")
        parsed_intervals.append((date_from, date_to))
    return parsed_intervals


def parse_auto_intervals(
    cluster_name: str, end_field: str, minutes: int, end: datetime | None = None
) -> list[tuple[datetime, datetime]]:
    intervals = []
    start = _time_auto_first_date(cluster_name, end_field)
    end = end or datetime.now(tz=TZLOCAL).astimezone(UTC)
    if start > end:
        raise ValueError(f"auto intervals: start date {start} > end date {end}")
    if minutes <= 0:
        # Invalid minutes. Let's just create a unique interval.
        intervals.append((start, end))
    else:
        # Valid minutes. Generate many intervals to cover [start, end].
        delta = timedelta(minutes=minutes)
        curr = start
        while curr + delta <= end:
            next_time = curr + delta
            intervals.append((curr, next_time))
            curr = next_time
    return intervals


def set_auto_end_time(cluster_name: str, end_field: str, date: datetime) -> None:
    # set the last valid date in the database for the cluster
    logger.info(f"set last successful date for cluster {cluster_name} to {date}")
    db = config().mongo.database_instance
    db_collection = db.clusters
    db_collection.update_one(
        {"cluster_name": cluster_name},
        {"$set": {end_field: date.strftime(DATE_FORMAT_HOUR)}},
        upsert=True,
    )


# pylint: disable=logging-not-lazy,too-many-branches
@dataclass
class AcquireJobs:
    cluster_names: list[str] = field(alias=["-c"], default_factory=list)

    intervals: list[str] | None = field(
        alias=["-i"],
        default=None,
        help=(
            "Acquire jobs in these intervals. "
            "Expected format for each interval: <date-from>-<date-to>, "
            "with <date-from> and <date-to> in format: YYYY-MM-DDTHH:mm "
            "(e.g.: 2020-01-01T17:05-2020-01-01T18:00). "
            "Dates will be interpreted as UTC. "
            "Mutually exclusive with --auto_interval."
        ),
    )

    auto_interval: int | None = field(
        alias=["-a"],
        type=int,
        default=None,
        help=(
            "Acquire jobs every <auto_interval> minutes "
            "since latest scraping date until now. "
            "If <= 0, use only one interval since latest scraping date until now. "
            "Mutually exclusive with --intervals."
        ),
    )

    def execute(self) -> int:
        print("ERROR: 'sarc acquire jobs' has been deprecated.")
        print()
        print("Please use the new two-step commands:")
        print()
        print("  1. Fetch jobs data:")
        print("     sarc fetch jobs -c <cluster> -a <minutes>")
        print("     sarc fetch jobs -c <cluster> -i <interval>")
        print()
        print("  2. Parse and store to database:")
        print("     sarc parse jobs --since <iso_datetime>")
        print()
        print("Example:")
        print("  sarc fetch jobs -c mila -a 60")
        print("  sarc parse jobs --since '2024-01-15T00:00:00'")
        print()
        print("For multiple clusters, use multiple -c flags:")
        print("  sarc fetch jobs -c mila -c narval -a 60")
        return 1
