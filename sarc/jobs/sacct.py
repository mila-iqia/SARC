import json
import logging
import subprocess
from datetime import date, datetime, time, timedelta
from typing import Iterator, Optional

from hostlist import expand_hostlist
from invoke.runners import Result
from tqdm import tqdm

from sarc.cache import with_cache
from sarc.client.job import SlurmJob, _jobs_collection
from sarc.config import UTC, ClusterConfig
from sarc.jobs.node_gpu_mapping import get_node_to_gpu
from sarc.traces import trace_decorator, using_trace

logger = logging.getLogger(__name__)


class JobConversionError(Exception):
    """Exception raised when there's an error converting a job entry from sacct."""


def parse_in_timezone(timestamp: int | None) -> datetime | None:
    if timestamp is None or timestamp == 0:
        return None
    # Slurm returns timestamps in UTC
    return datetime.fromtimestamp(timestamp, UTC)


class SAcctScraper:
    """Scrape info from Slurm using the sacct command.

    The scraper is currently hard-coded to fetch data for a day.
    """

    def __init__(
        self,
        cluster: ClusterConfig,
        day: datetime | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ):
        """Initialize a SAcctScraper.

        Arguments:
            cluster: The cluster on which to scrape the data.
            day: The day we wish to scrape, as a datetime object. The time
                does not matter: we will fetch from 00:00 on that day to
                00:00 on the next day.
            start: the datetime from which we wish to scrape.
                Used only if `day` is None. May be precise
                up to time. To be used with `end`.
            end: the datetime until which we wish to scrape.
                Used only if `day` is None. May be precise
                up to time. To be used with `start`.
        """
        self.cluster = cluster
        self.day = day
        if day is not None:
            assert start is None
            assert end is None
            self.start = datetime.combine(day, time.min)
            self.end = self.start + timedelta(days=1)
        else:
            assert start is not None
            assert end is not None
            self.start = start
            self.end = end

    @trace_decorator()
    def fetch_raw(self) -> dict:
        """Fetch the raw sacct data as a dict via SSH, or run sacct locally."""
        fmt = "%Y-%m-%dT%H:%M"
        start = self.start.strftime(fmt)
        end = self.end.strftime(fmt)
        accounts = ",".join(self.cluster.accounts) if self.cluster.accounts else None
        accounts_option = f"-A {accounts}" if accounts else ""
        cmd = f"{self.cluster.sacct_bin} {accounts_option} -X -S {start} -E {end} --allusers --json"
        logger.info(f"{self.cluster.name} $ {cmd}")
        if self.cluster.host == "localhost":
            results: subprocess.CompletedProcess[str] | Result = subprocess.run(
                cmd,
                shell=True,
                text=True,
                capture_output=True,
                check=False,
                env={"TZ": "UTC"},
            )
        else:
            ssh = self.cluster.ssh
            ssh.config.run.env = {"TZ": "UTC"}
            results = ssh.run(cmd, hide=True)
        return json.loads(results.stdout[results.stdout.find("{") :])

    def _cache_key(self) -> str | None:
        today = datetime.combine(date.today(), datetime.min.time())
        if self.day is None:
            fmt = "%Y-%m-%dT%H:%M"
            startstr = self.start.strftime(fmt)
            endstr = self.end.strftime(fmt)
            return f"{self.cluster.name}.{startstr}.{endstr}.json"
        elif self.day < today:
            daystr = self.day.strftime("%Y-%m-%d")
            return f"{self.cluster.name}.{daystr}.json"
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
                "sarc.jobs.sacct", "SAcctScraper.__iter__", exception_types=()
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
            )

        if int(version["major"]) == 24:
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
            )

        # if we arrive here, it means that the version is not supported :-(
        raise JobConversionError(f"Unsupported slurm version: {version}")


@trace_decorator()
def sacct_mongodb_import(
    cluster: ClusterConfig,
    day: datetime | None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> None:
    """Fetch sacct data and store it in MongoDB.

    Parameters
    ----------
    cluster: ClusterConfig
        The configuration of the cluster on which to fetch the data.
    day: datetime
        The day for which to fetch the data. The time does not matter.
    start: datetime
        The datetime from which to fetch the data. Time matters.
        Used with `end`, and only if `day` is None.
    end: datetime
        The datetime up to which we fetch the data. Time matters.
        Use with `start`, and only if `day` is None.
    """
    collection = _jobs_collection()
    scraper = SAcctScraper(cluster, day, start, end)

    timestr = f"date {day}" if day is not None else f"time {start} to {end}"
    logger.info(f"Getting the sacct data for cluster {cluster.name}, {timestr}...")

    scraper.get_raw()
    logger.info(
        f"Saving into mongodb collection '{collection.Meta.collection_name}'..."
    )
    nb_entries = 0
    for entry in tqdm(scraper):
        if entry is not None:
            nb_entries += 1
            update_allocated_gpu_type_from_nodes(cluster, entry)
            collection.save_job(entry)
    logger.info(f"Saved {nb_entries}/{len(scraper)} entries.")


@trace_decorator()
def update_allocated_gpu_type_from_nodes(
    cluster: ClusterConfig, entry: SlurmJob
) -> Optional[str]:
    """
    Try to infer job GPU type from entry nodes
    if cluster has not configured a Prometheusn connection.

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

    if not cluster.prometheus_url:
        # No prometheus config. Try to get GPU type from entry nodes.
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
