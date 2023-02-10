import json
import sys
import traceback
import warnings
import zoneinfo
from datetime import datetime, time, timedelta
from enum import Enum
from pprint import pprint
from typing import Iterator, Optional

from hostlist import expand_hostlist
from pydantic import validator
from pydantic_mongo import ObjectIdField

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

    # Database ID
    id: ObjectIdField = None

    # job identification
    cluster_name: str
    account: str
    job_id: int
    array_job_id: Optional[int]
    task_id: Optional[int]
    name: str
    user: str
    group: str

    # status
    job_state: SlurmState
    exit_code: Optional[int]
    signal: Optional[int]

    # allocation information
    partition: str
    nodes: list[str]
    work_dir: str

    # Miscellaneous
    constraints: Optional[str]
    priority: int
    qos: Optional[str]

    # Flags
    CLEAR_SCHEDULING: bool = False
    STARTED_ON_SUBMIT: bool = False
    STARTED_ON_SCHEDULE: bool = False
    STARTED_ON_BACKFILL: bool = False

    # temporal fields
    time_limit: Optional[int]
    submit_time: datetime
    start_time: datetime
    end_time: Optional[datetime]
    elapsed_time: int

    # tres
    requested: SlurmResources
    allocated: SlurmResources

    @validator("submit_time", "start_time", "end_time")
    def _ensure_timezone(cls, v):
        # We'll store in MTL timezone because why not
        return v and v.replace(tzinfo=UTC).astimezone(MTL)


MTL = zoneinfo.ZoneInfo("America/Montreal")
UTC = zoneinfo.ZoneInfo("UTC")


def parse_in_timezone(cluster, timestamp):
    if timestamp is None or timestamp == 0:
        return None
    date_naive = datetime.fromtimestamp(timestamp)
    date_aware = date_naive.replace(tzinfo=cluster.timezone)
    return date_aware.astimezone(UTC)


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
        accounts = self.cluster.accounts and ",".join(self.cluster.accounts)
        accounts_option = f"-A {accounts}" if accounts else ""
        cmd = f"{self.cluster.sacct_bin} {accounts_option} -X -S '{start}' -E '{end}' --json"
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
            try:
                converted = self.convert(entry)
                if converted is not None:
                    yield converted
            except Exception:
                traceback.print_exc()
                print("There was a problem with this entry:", file=sys.stderr)
                print("====================================", file=sys.stderr)
                pprint(entry)
                print("====================================", file=sys.stderr)

    def convert(self, entry: dict) -> SlurmJob:
        """Convert a single job entry from sacct to a SlurmJob."""
        resources = {"requested": {}, "allocated": {}}
        tracked_resources = ["cpu", "mem", "gres", "node", "billing"]

        if entry["group"] is None:
            # These seem to correspond to very old jobs that shouldn't still exist,
            # likely a configuration blunder.
            return None

        for grp, vals in resources.items():
            for alloc in entry["tres"][grp]:
                if (key := alloc["type"]) not in tracked_resources:
                    continue
                if aname := alloc["name"]:
                    key += f"_{aname}"
                vals[key] = alloc["count"]

        nodes = entry["nodes"]

        flags = {k: True for k in entry["flags"]}

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
            submit_time=parse_in_timezone(self.cluster, entry["time"]["submission"]),
            start_time=parse_in_timezone(self.cluster, entry["time"]["start"]),
            end_time=parse_in_timezone(self.cluster, entry["time"]["end"]),
            elapsed_time=entry["time"]["elapsed"],
            partition=entry["partition"],
            nodes=expand_hostlist(nodes) if nodes != "None assigned" else [],
            constraints=entry["constraints"],
            priority=entry["priority"],
            qos=entry["qos"],
            work_dir=entry["working_directory"],
            **resources,
            **flags,
        )
