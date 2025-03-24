import json
import logging
import math
import subprocess
import time as profiling
from datetime import date, datetime, time, timedelta
from typing import Iterator, List, Optional

from hostlist import expand_hostlist
from tqdm import tqdm

from sarc.cache import with_cache
from sarc.client.job import SlurmJob, _jobs_collection
from sarc.config import MTL, UTC, ClusterConfig
from sarc.jobs.node_gpu_mapping import get_node_to_gpu
from sarc.jobs.series import get_job_time_series
from sarc.traces import trace_decorator, using_trace

logger = logging.getLogger(__name__)


def parse_in_timezone(timestamp):
    if timestamp is None or timestamp == 0:
        return None
    # Slurm returns timestamps in UTC
    date_naive = datetime.utcfromtimestamp(timestamp)
    return date_naive.replace(tzinfo=UTC)


class SAcctScraper:
    """Scrape info from Slurm using the sacct command.

    The scraper is currently hard-coded to fetch data for a day.
    """

    def __init__(
        self,
        cluster: ClusterConfig,
        day: datetime = None,
        start: datetime = None,
        end: datetime = None,
        user: str = None,
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
            user: user for which we wish to scrape.
        """
        self.cluster = cluster
        if day is not None:
            assert start is None
            assert end is None
            self.day = day
            self.start = datetime.combine(day, time.min)
            self.end = self.start + timedelta(days=1)
        else:
            assert start is not None
            assert end is not None
            self.day = None
            self.start = start
            self.end = end
        self.user = user

    @trace_decorator()
    def fetch_raw(self) -> dict:
        """Fetch the raw sacct data as a dict via SSH, or run sacct locally."""
        fmt = "%Y-%m-%dT%H:%M"
        start = self.start.strftime(fmt)
        end = self.end.strftime(fmt)
        accounts = self.cluster.accounts and ",".join(self.cluster.accounts)
        accounts_option = f"-A {accounts}" if accounts else ""
        user_option = "--allusers" if self.user is None else f"--user {self.user}"
        cmd = f"{self.cluster.sacct_bin} {accounts_option} -X -S {start} -E {end} {user_option} --json"
        logger.info(f"{self.cluster.name} $ {cmd}")
        if self.cluster.host == "localhost":
            results = subprocess.run(
                cmd, shell=True, text=True, capture_output=True, check=False
            )
        else:
            results = self.cluster.ssh.run(cmd, hide=True)
        return json.loads(results.stdout[results.stdout.find("{") :])

    def _cache_key(self):
        today = datetime.combine(date.today(), datetime.min.time())
        userstr = "" if self.user is None else f".user_{self.user}"
        if self.day is None:
            fmt = "%Y-%m-%dT%H:%M"
            startstr = self.start.strftime(fmt)
            endstr = self.end.strftime(fmt)
            return f"{self.cluster.name}.{startstr}.{endstr}{userstr}.json"
        elif self.day < today:
            daystr = self.day.strftime("%Y-%m-%d")
            return f"{self.cluster.name}.{daystr}{userstr}.json"
        else:
            # Not cachable
            return None

    @with_cache(subdirectory="sacct", key=_cache_key, live=True)
    def get_raw(self) -> dict:
        return self.fetch_raw()

    def __len__(self) -> int:
        return len(self.get_raw()["jobs"])

    def __iter__(self) -> Iterator[SlurmJob]:
        """Fetch and iterate on all jobs as SlurmJob objects."""
        version = (
            self.get_raw().get("meta", {}).get("Slurm", None)
            or self.get_raw().get("meta", {}).get("slurm", {})
        ).get("version", None)
        for entry in self.get_raw()["jobs"]:
            with using_trace("sarc.jobs.sacct", "SAcctScraper.__iter__") as span:
                span.set_attribute("entry", json.dumps(entry))
                converted = self.convert(entry, version)
                if converted is not None:
                    yield converted

    def convert(self, entry: dict, version: dict = None) -> Optional[SlurmJob]:
        """Convert a single job entry from sacct to a SlurmJob."""
        resources = {"requested": {}, "allocated": {}}
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
        start_time = parse_in_timezone(entry["time"]["start"])
        end_time = parse_in_timezone(entry["time"]["end"])
        elapsed_time = entry["time"]["elapsed"]

        if end_time:
            # The start_time is not set properly in the json output of sacct, but
            # it can be calculated from end_time and elapsed_time. We leave the
            # inaccurate value in for RUNNING jobs.
            start_time = end_time - timedelta(seconds=elapsed_time)

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
                **resources,
                **flags,
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
                    **resources,
                    **flags,
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
                **resources,
                **flags,
            )

        raise ValueError(f"Unsupported slurm version: {version}")


@trace_decorator()
def sacct_mongodb_import(
    cluster: ClusterConfig,
    day: Optional[datetime],
    no_prometheus: bool,
    start: datetime = None,
    end: datetime = None,
    user: str = None,
) -> None:
    """Fetch sacct data and store it in MongoDB.

    Arguments:
    Parameters
    ----------
    cluster: ClusterConfig
        The configuration of the cluster on which to fetch the data.
    day: datetime
        The day for which to fetch the data. The time does not matter.
    no_prometheus: bool
        If True, avoid any scraping requiring prometheus connection.
    start: datetime
        The datetime from which to fetch the data. Time matters.
        Used with `end`, and only if `day` is None.
    end: date
        The datetime up to which we fetch the data. Time matters.
        Use with `start`, and only if `day` is None.
    user: str
        Optional user for which to fetch the data.
    """
    collection = _jobs_collection()
    scraper = SAcctScraper(cluster, day, start, end, user)
    logger.info("Getting the sacct data...")
    scraper.get_raw()
    logger.info(
        f"Saving into mongodb collection '{collection.Meta.collection_name}'..."
    )
    entries = list(tqdm(scraper, desc="scrap entries"))
    job_id_to_gpu_type = _check_prometheus(cluster, entries)
    for entry in tqdm(entries, desc="save entries"):
        saved = False
        if not no_prometheus:
            update_allocated_gpu_type(cluster, entry, job_id_to_gpu_type)
            saved = entry.statistics(recompute=True, save=True) is not None

        if not saved:
            collection.save_job(entry)
    logger.info(f"Saved {len(scraper)} entries.")


def _check_prometheus(cluster: ClusterConfig, entries: List[SlurmJob]):
    job_id_to_gpu_type = {}
    a = profiling.perf_counter()
    if cluster.prometheus_url:
        now = datetime.now(tz=UTC).astimezone(MTL)
        oldest_start = min(
            (entry.start_time for entry in entries if entry.start_time is not None),
            default=now,
        )
        oldest_debut = min(
            (
                entry.end_time - timedelta(seconds=entry.elapsed_time)
                for entry in entries
                if entry.end_time is not None and entry.elapsed_time >= 0
            ),
            default=now,
        )
        newest_end_time = max(
            (entry.end_time for entry in entries if entry.end_time is not None),
            default=now,
        )
        times = [oldest_start, oldest_debut]
        greatest_duration = now - min(times)
        duration_seconds = math.ceil(greatest_duration.total_seconds())
        selector = f'slurm_job_utilization_gpu_memory{{cluster="{cluster.name}"}}'
        if newest_end_time < now:
            offset = now - newest_end_time
            offset_seconds = math.ceil(offset.total_seconds())
            offset_string = f"offset {offset_seconds}s"
            query = (
                f"present_over_time({selector}[{duration_seconds}s] {offset_string})"
            )
        else:
            query = f"present_over_time({selector}[{duration_seconds}s])"
        print(
            cluster.name,
            *times,
            oldest_start == oldest_debut,
            query,
        )
        results = cluster.prometheus.custom_query(query)
        for result in results:
            job_id = int(result["metric"]["slurmjobid"])
            gpu_type = result["metric"]["gpu_type"]
            if job_id in job_id_to_gpu_type:
                assert job_id_to_gpu_type[job_id] == gpu_type, (
                    job_id,
                    job_id_to_gpu_type[job_id],
                    gpu_type,
                )
            else:
                job_id_to_gpu_type[job_id] = gpu_type
    b = profiling.perf_counter()
    print("_check_prometheus", repr(timedelta(seconds=b - a)), len(job_id_to_gpu_type))
    return job_id_to_gpu_type


@trace_decorator()
def update_allocated_gpu_type(
    cluster: ClusterConfig, entry: SlurmJob, jtg: dict, use_job_time_series=False
) -> Optional[str]:
    """Try to infer job GPU type.

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

    if cluster.prometheus_url:
        # Cluster does have prometheus config.
        if use_job_time_series:
            output = get_job_time_series(
                job=entry,
                metric="slurm_job_utilization_gpu_memory",
                max_points=1,
                dataframe=False,
            )
            if output:
                gpu_type = output[0]["metric"]["gpu_type"]
                assert entry.job_id in jtg, (
                    "should be found",
                    entry.job_id,
                    gpu_type,
                    list(jtg.keys()),
                )
                assert jtg[entry.job_id] == gpu_type, (
                    "should be same gpu",
                    jtg[entry.job_id],
                    gpu_type,
                )
            else:
                if jtg.get(entry.job_id):
                    print(
                        "should NOT be found",
                        entry.job_id,
                        jtg.get(entry.job_id),
                        entry.allocated.gpu_type,
                    )
        else:
            gpu_type = jtg.get(entry.job_id)
    else:
        # No prometheus config. Try to get GPU type from entry nodes.
        node_gpu_mapping = get_node_to_gpu(cluster.name, entry.start_time)
        if node_gpu_mapping:
            node_to_gpu = node_gpu_mapping.node_to_gpu
            gpu_types = {
                node_to_gpu[nodename]
                for nodename in entry.nodes
                if nodename in node_to_gpu
            }
            # We should not have more than 1 GPU type per job.
            assert len(gpu_types) <= 1
            if gpu_types:
                gpu_type = gpu_types.pop()

    # If we found a GPU type, try to infer descriptive GPU name
    if gpu_type is not None:
        harmonized_gpu_names = {
            cluster.harmonize_gpu(nodename, gpu_type) for nodename in entry.nodes
        }
        # If present, remove None from GPU names
        harmonized_gpu_names.discard(None)
        # If we got 1 GPU name, use it.
        # Otherwise, keep default found gpu_type.
        if len(harmonized_gpu_names) == 1:
            gpu_type = harmonized_gpu_names.pop()
        # Finally, save gpu_type into job object.
        entry.allocated.gpu_type = gpu_type

    return entry.allocated.gpu_type
