import json
import warnings
from datetime import datetime, time, timedelta
from enum import Enum
from typing import Iterator, Optional

from hostlist import expand_hostlist

from ..cluster import Cluster
from ..config import BaseModel, config


class SlurmState(str, Enum):
    """Possible Slurm job states.

    Reference: https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES
    """

    BOOT_FAIL = "BOOT_FAIL"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    CONFIGURING = "CONFIGURING"
    COMPLETING = "COMPLETING"
    DEADLINE = "DEADLINE"
    FAILED = "FAILED"
    NODE_FAIL = "NODE_FAIL"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    PENDING = "PENDING"
    PREEMPTED = "PREEMPTED"
    RUNNING = "RUNNING"
    RESV_DEL_HOLD = "RESV_DEL_HOLD"
    REQUEUE_FED = "REQUEUE_FED"
    REQUEUE_HOLD = "REQUEUE_HOLD"
    REQUEUED = "REQUEUED"
    RESIZING = "RESIZING"
    REVOKED = "REVOKED"
    SIGNALING = "SIGNALING"
    SPECIAL_EXIT = "SPECIAL_EXIT"
    STAGE_OUT = "STAGE_OUT"
    STOPPED = "STOPPED"
    SUSPENDED = "SUSPENDED"
    TIMEOUT = "TIMEOUT"


class SlurmResources(BaseModel):
    """Counts for various resources."""

    cpu: Optional[int]
    mem: Optional[int]
    node: Optional[int]
    billing: Optional[int]
    gres_gpu: Optional[int]


class SlurmJob(BaseModel):
    """Holds data for a Slurm job."""

    # Database ID (cluster_name:job_id)
    id: str

    # job identification
    cluster_name: str
    account: str
    job_id: int
    array_job_id: Optional[int]
    task_id: Optional[int]
    name: str
    username: str

    # status
    job_state: SlurmState
    exit_code: Optional[int]
    signal: Optional[int]

    # allocation information
    partition: str
    nodes: list[str]
    work_dir: str

    # temporal fields
    time_limit: Optional[int]
    submit_time: datetime
    start_time: datetime
    end_time: Optional[datetime]
    elapsed_time: int

    # tres
    requested: SlurmResources
    allocated: SlurmResources


class SAcctScraper:
    """Scrape info from Slurm using the sacct command.

    The scraper is currently hard-coded to fetch data for a day.
    """

    def __init__(self, cluster: Cluster, day: datetime):
        """Initialize a SAcctScraper.

        Arguments:
            cluster: The cluster on which to scrape the data.
            day: The day we wish to scrape, as a datetime object. The time
                does not matter: we will fetch from 00:00 on that day to
                00:00 on the next day.
        """
        self.cluster = cluster
        self.day = day
        self.start = datetime.combine(day, time.min)
        self.end = self.start + timedelta(days=1)
        self.results = None

        cachedir = config().cache
        if cachedir:
            cachedir = cachedir / "sacct"
            cachedir.mkdir(parents=True, exist_ok=True)
            daystr = day.strftime("%Y-%m-%d")
            self.cachefile = cachedir / f"{cluster.name}.{daystr}.json"
        else:
            self.cachefile = None

    def fetch_raw(self) -> dict:
        """Fetch the raw sacct data as a dict via SSH."""
        fmt = "%Y-%m-%dT%H:%M"
        start = self.start.strftime(fmt)
        end = self.end.strftime(fmt)
        cmd = f"sacct -X -S '{start}' -E '{end}' --json"
        print(f"{self.cluster.name} $ {cmd}")
        results = self.cluster.ssh.run(cmd, hide=True)
        return json.loads(results.stdout)

    def get_raw(self) -> dict:
        """Return the raw sacct data as a dict.

        If the data had been fetched before and cached in Config.cache, the contents
        of the cache file are returned. Otherwise, the data is fetched via SSH and
        cached if Config.cache is set.
        """
        if self.results is not None:
            return self.results

        if self.cachefile and self.cachefile.exists():
            try:
                return json.load(open(self.cachefile, "r", encoding="utf8"))
            except json.JSONDecodeError:
                warnings.warn("Need to re-fetch because cache has malformed JSON.")

        self.results = self.fetch_raw()
        if self.cachefile:
            json.dump(fp=open(self.cachefile, "w", encoding="utf8"), obj=self.results)
        return self.results

    def __len__(self) -> int:
        return len(self.get_raw()["jobs"])

    def __iter__(self) -> Iterator[SlurmJob]:
        """Fetch and iterate on all jobs as SlurmJob objects."""
        for entry in self.get_raw()["jobs"]:
            yield self.convert(entry)

    def convert(self, entry: dict) -> SlurmJob:
        """Convert a single job entry from sacct to a SlurmJob."""
        resources = {"requested": {}, "allocated": {}}
        tracked_resources = ["cpu", "mem", "gres", "node", "billing"]

        for grp, vals in resources.items():
            for alloc in entry["tres"][grp]:
                if (key := alloc["type"]) not in tracked_resources:
                    continue
                if aname := alloc["name"]:
                    key += f"_{aname}"
                vals[key] = alloc["count"]

        nodes = entry["nodes"]

        return SlurmJob(
            id=f'{self.cluster.name}:{entry["job_id"]}',
            cluster_name=self.cluster.name,
            job_id=entry["job_id"],
            array_job_id=entry["array"]["job_id"] or None,
            task_id=entry["array"]["task_id"],
            name=entry["name"],
            username=entry["user"],
            account=entry["account"],
            job_state=entry["state"]["current"],
            exit_code=entry["exit_code"]["return_code"],
            signal=entry["exit_code"].get("signal", {}).get("signal_id", None),
            time_limit=(tlimit := entry["time"]["limit"]) and tlimit * 60,
            submit_time=(entry["time"]["submission"] or None),
            start_time=(entry["time"]["start"] or None),
            end_time=(entry["time"]["end"] or None),
            elapsed_time=entry["time"]["elapsed"],
            partition=entry["partition"],
            nodes=expand_hostlist(nodes) if nodes != "None assigned" else [],
            work_dir=entry["working_directory"],
            **resources,
        )
