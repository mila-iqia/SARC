from datetime import datetime, timedelta
from hostlist import expand_hostlist
from invoke.runners import Result
import json
import logging
import re
import subprocess
from typing import Iterator, Optional

from sarc.cache import with_cache
from sarc.client.job import SlurmJob
from sarc.config import ClusterConfig, config, UTC, TZLOCAL
from sarc.core.models.validators import UTCOFFSET
from sarc.errors import ClusterNotFound
from sarc.jobs.node_gpu_mapping import get_node_to_gpu
from sarc.traces import trace_decorator, using_trace


logger = logging.getLogger(__name__)


DATE_FORMAT_HOUR = "%Y-%m-%dT%H:%M"


class JobConversionError(Exception):
    """Exception raised when there's an error converting a job entry from sacct."""


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
        while curr < end:
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


def parse_in_timezone(timestamp: int | None) -> datetime | None:
    if timestamp is None or timestamp == 0:
        return None
    # Slurm returns timestamps in UTC
    return datetime.fromtimestamp(timestamp, UTC)


def _date_is_utc(value: datetime) -> bool:
    """Return True if given date is in UTC timezone."""
    return value.tzinfo is not None and value.utcoffset() == UTCOFFSET


class SacctScraper:
    """Scrape info from Slurm using the sacct command."""

    def __init__(
        self,
        cluster: ClusterConfig,
        start: datetime,
        end: datetime,
    ):
        """Initialize a SacctScraper.

        Arguments:
            cluster: The cluster on which to scrape the data.
            start: the UTC datetime from which we wish to scrape.
                Should be precise up to minute.
            end: the UTC datetime until which we wish to scrape.
                Should be precise up to minute.
        """
        if not _date_is_utc(start):
            raise ValueError(f"sacct scraper: start date not in UTC: {start}")
        if not _date_is_utc(end):
            raise ValueError(f"sacct scraper: end date not in UTC: {end}")

        self.cluster = cluster
        self.start = start
        self.end = end

    @trace_decorator()
    def fetch_raw(self) -> dict:
        """Fetch the raw sacct data as a dict via SSH, or run sacct locally."""
        fmt = "%Y-%m-%dT%H:%M"
        start = self.start.strftime(fmt)
        end = self.end.strftime(fmt)
        accounts = ",".join(self.cluster.accounts) if self.cluster.accounts else None
        accounts_option = f"-A {accounts} " if accounts else ""
        cmd = f"{self.cluster.sacct_bin} {accounts_option}-X -S {start} -E {end} --allusers --json"
        logger.debug(f"{self.cluster.name} $ {cmd}")
        if self.cluster.host == "localhost":
            results: subprocess.CompletedProcess[str] | Result = subprocess.run(
                cmd,
                shell=True,
                text=True,
                capture_output=True,
                check=False,
                env={"TZ": "UTC"} if not self.cluster.ignore_tz_utc else {},
            )
        else:
            ssh = self.cluster.ssh
            ssh.config.run.env = {"TZ": "UTC"} if not self.cluster.ignore_tz_utc else {}
            results = ssh.run(cmd, hide=True)
            logger.debug(results.stdout)
        return json.loads(results.stdout[results.stdout.find("{") :])

    def _cache_key(self) -> str | None:
        now = datetime.now(tz=UTC)
        if self.start < self.end <= now:
            fmt = "%Y-%m-%dT%H:%M"
            startstr = self.start.strftime(fmt)
            endstr = self.end.strftime(fmt)
            return f"{self.cluster.name}.{startstr}.{endstr}.json"
        else:
            # Not cachable
            return None

    @with_cache(subdirectory="sacct", key=_cache_key, live=True)  # type: ignore[arg-type] # mypy has some trouble with methods
    def get_raw(self) -> dict:
        return self.fetch_raw()

    def __len__(self) -> int:
        return len(self.get_raw()["jobs"])

    def __iter__(self) -> Iterator[SlurmJob | None]:
        """Fetch and iterate on all jobs as SlurmJob objects."""
        version: dict = (
            self.get_raw().get("meta", {}).get("Slurm", None)
            or self.get_raw().get("meta", {}).get("slurm", {})
        ).get("version", None)
        for entry in self.get_raw()["jobs"]:
            with using_trace(
                "sarc.core.scraping.jobs_utils",
                "SacctScraper.__iter__",
                exception_types=(),
            ) as span:
                span.set_attribute("entry", json.dumps(entry))
                converted = self.convert(entry, version)
                yield converted

    def convert(self, entry: dict, version: dict | None = None) -> SlurmJob | None:
        """Convert a single job entry from sacct to a SlurmJob."""
        resources: dict[str, dict] = {"requested": {}, "allocated": {}}
        tracked_resources = ["cpu", "mem", "gres", "node", "billing"]

        if entry["group"] is None:
            # These seem to correspond to very old jobs that shouldn't still exist,
            # likely a configuration blunder.
            logger.debug('Skipping job with group "None": %s', entry["job_id"])
            return None

        for grp, vals in resources.items():
            for alloc in entry["tres"][grp]:
                if (key := alloc["type"]) not in tracked_resources:
                    continue
                if aname := alloc["name"]:
                    key += f"_{aname}"

                if key.startswith("gres_gpu:"):
                    value = key.split(":")[1]
                    key = "gpu_type"
                else:
                    value = alloc["count"]

                vals[key] = value

        nodes = entry["nodes"]

        tracked_flags = [
            "CLEAR_SCHEDULING",
            "STARTED_ON_SUBMIT",
            "STARTED_ON_SCHEDULE",
            "STARTED_ON_BACKFILL",
        ]
        flags = {k: True for k in entry["flags"] if k in tracked_flags}

        submit_time = parse_in_timezone(entry["time"]["submission"])
        assert submit_time is not None
        start_time = parse_in_timezone(entry["time"]["start"])
        end_time = parse_in_timezone(entry["time"]["end"])
        elapsed_time: int = entry["time"]["elapsed"]

        if end_time:
            # The start_time is not set properly in the json output of sacct, but
            # it can be calculated from end_time and elapsed_time. We leave the
            # inaccurate value in for RUNNING jobs.
            start_time = end_time - timedelta(seconds=elapsed_time)

        # Here we add supplementary SlurmJob attributes
        # which should be common to all slurm versions.
        extra = {
            # Save scraping period in job
            # We save these dates with timezone UTC
            # Note: If date is naive (as actually parsed from `acquire jobs`),
            # then astimezone() assumes date is in local timezone.
            "latest_scraped_start": self.start.astimezone(UTC),
            "latest_scraped_end": self.end.astimezone(UTC),
        }

        assert self.cluster.name is not None

        if self.cluster.name != entry["cluster"]:
            logger.warning(
                'Job %s from cluster "%s" has a different cluster name: "%s". Using "%s".',
                entry["job_id"],
                self.cluster.name,
                entry["cluster"],
                self.cluster.name,
            )
        if version is None or int(version["major"]) < 23:
            return SlurmJob(
                cluster_name=self.cluster.name,
                job_id=entry["job_id"],
                array_job_id=entry["array"]["job_id"] or None,
                task_id=entry["array"]["task_id"],
                name=entry["name"],
                user=entry["user"],
                group=entry["group"],
                account=entry["account"],
                job_state=entry["state"]["current"],
                exit_code=entry["exit_code"]["return_code"],
                signal=entry["exit_code"].get("signal", {}).get("signal_id", None),
                time_limit=(tlimit := entry["time"]["limit"]) and tlimit * 60,
                submit_time=submit_time,
                start_time=start_time,
                end_time=end_time,
                elapsed_time=elapsed_time,
                partition=entry["partition"],
                nodes=(
                    sorted(expand_hostlist(nodes)) if nodes != "None assigned" else []
                ),
                constraints=entry["constraints"],
                priority=entry["priority"],
                qos=entry["qos"],
                work_dir=entry["working_directory"],
                **resources,  # type: ignore[arg-type]
                **flags,  # type: ignore[arg-type]
                **extra,  # type: ignore[arg-type]
            )
        if int(version["major"]) == 23:
            if int(version["minor"]) == 11:
                return SlurmJob(
                    cluster_name=self.cluster.name,
                    job_id=entry["job_id"],
                    array_job_id=entry["array"]["job_id"] or None,
                    task_id=entry["array"]["task_id"]["number"],
                    name=entry["name"],
                    user=entry["user"],
                    group=entry["group"],
                    account=entry["account"],
                    job_state=entry["state"]["current"][0],
                    exit_code=entry["exit_code"]["return_code"]["number"],
                    signal=entry["exit_code"]
                    .get("signal", {})
                    .get("id", {})
                    .get("number", None),
                    time_limit=(tlimit := entry["time"]["limit"]["number"])
                    and tlimit * 60,
                    submit_time=submit_time,
                    start_time=start_time,
                    end_time=end_time,
                    elapsed_time=elapsed_time,
                    partition=entry["partition"],
                    nodes=(
                        sorted(expand_hostlist(nodes))
                        if nodes != "None assigned"
                        else []
                    ),
                    constraints=entry["constraints"],
                    priority=entry["priority"]["number"],
                    qos=entry["qos"],
                    work_dir=entry["working_directory"],
                    **resources,  # type: ignore[arg-type]
                    **flags,  # type: ignore[arg-type]
                    **extra,  # type: ignore[arg-type]
                )

            return SlurmJob(
                cluster_name=self.cluster.name,
                job_id=entry["job_id"],
                array_job_id=entry["array"]["job_id"] or None,
                task_id=entry["array"]["task_id"]["number"],
                name=entry["name"],
                user=entry["user"],
                group=entry["group"],
                account=entry["account"],
                job_state=entry["state"]["current"],
                exit_code=entry["exit_code"]["return_code"],
                signal=entry["exit_code"].get("signal", {}).get("signal_id", None),
                time_limit=(tlimit := entry["time"]["limit"]["number"]) and tlimit * 60,
                submit_time=submit_time,
                start_time=start_time,
                end_time=end_time,
                elapsed_time=elapsed_time,
                partition=entry["partition"],
                nodes=(
                    sorted(expand_hostlist(nodes)) if nodes != "None assigned" else []
                ),
                constraints=entry["constraints"],
                priority=entry["priority"]["number"],
                qos=entry["qos"],
                work_dir=entry["working_directory"],
                **resources,  # type: ignore[arg-type]
                **flags,  # type: ignore[arg-type]
                **extra,  # type: ignore[arg-type]
            )

        if int(version["major"]) in [24, 25]:
            return SlurmJob(
                cluster_name=self.cluster.name,
                job_id=entry["job_id"],
                array_job_id=entry["array"]["job_id"] or None,
                task_id=entry["array"]["task_id"]["number"],
                name=entry["name"],
                user=entry["user"],
                group=entry["group"],
                account=entry["account"],
                job_state=entry["state"]["current"][0],
                exit_code=entry["exit_code"]["return_code"]["number"],
                signal=entry["exit_code"]
                .get("signal", {})
                .get("id", {})
                .get("number", None),
                time_limit=(tlimit := entry["time"]["limit"]["number"]) and tlimit * 60,
                submit_time=submit_time,
                start_time=start_time,
                end_time=end_time,
                elapsed_time=elapsed_time,
                partition=entry["partition"],
                nodes=(
                    sorted(expand_hostlist(nodes)) if nodes != "None assigned" else []
                ),
                constraints=entry["constraints"],
                priority=entry["priority"]["number"],
                qos=entry["qos"],
                work_dir=entry["working_directory"],
                **resources,  # type: ignore[arg-type]
                **flags,  # type: ignore[arg-type]
                **extra,  # type: ignore[arg-type]
            )

        # if we arrive here, it means that the version is not supported :-(
        raise JobConversionError(f"Unsupported slurm version: {version}")


@trace_decorator()
def update_allocated_gpu_type_from_nodes(
    cluster: ClusterConfig, entry: SlurmJob
) -> Optional[str]:
    """
    Try to infer job GPU type from entry nodes

    Parameters
    ----------
    cluster: ClusterConfig
        Cluster configuration for the current job.
    entry: SlurmJob
        Slurm job for which to infer the gpu type.

    Returns
    -------
    str
        String representing the gpu type.
    None
        Unable to infer gpu type.
    """
    gpu_type = None

    # Try to get GPU type from entry nodes.
    assert cluster.name is not None
    node_gpu_mapping = get_node_to_gpu(cluster.name, entry.start_time)
    if node_gpu_mapping:
        node_to_gpu = node_gpu_mapping.node_to_gpu
        gpu_types = {
            gpu for nodename in entry.nodes for gpu in node_to_gpu.get(nodename, ())
        }
        # We infer gpu_type only if we found 1 GPU for this job.
        if len(gpu_types) == 1:
            gpu_type = gpu_types.pop()

    if gpu_type is None:
        # No gpu_type from neither prometheus nor entry nodes.
        # Just take current value in entry.allocated.gpu_type.
        # If value is not None, it could be harmonized below.
        gpu_type = entry.allocated.gpu_type

    # If we found a GPU type, try to infer descriptive GPU name
    if gpu_type is not None:
        entry.allocated.gpu_type = (
            cluster.harmonize_gpu_from_nodes(entry.nodes, gpu_type) or gpu_type
        )

    return entry.allocated.gpu_type
