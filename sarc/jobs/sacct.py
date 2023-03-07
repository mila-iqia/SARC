import json
import logging
import sys
import traceback
from datetime import datetime, time, timedelta
from pprint import pformat
from typing import Iterator, Optional

from hostlist import expand_hostlist
from tqdm import tqdm

from ..config import UTC, ClusterConfig, config
from .job import SlurmJob, jobs_collection

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

    def __init__(self, cluster: ClusterConfig, day: datetime):
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
                logger.warning("Need to re-fetch because cache has malformed JSON.")

        self.results = self.fetch_raw()
        if self.cachefile:
            # pylint: disable=consider-using-with
            json.dump(
                fp=open(self.cachefile, "w", encoding="utf8"),
                obj=self.results,
            )
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
            except Exception:  # pylint: disable=broad-exception-caught
                traceback.print_exc()
                print("There was a problem with this entry:", file=sys.stderr)
                print("====================================", file=sys.stderr)
                print(pformat(entry), file=sys.stderr)
                print("====================================", file=sys.stderr)

    def convert(self, entry: dict) -> Optional[SlurmJob]:
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

        flags = {k: True for k in entry["flags"]}

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

        job = SlurmJob(
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
            nodes=sorted(expand_hostlist(nodes)) if nodes != "None assigned" else [],
            constraints=entry["constraints"],
            priority=entry["priority"],
            qos=entry["qos"],
            work_dir=entry["working_directory"],
            **resources,
            **flags,
        )

        return job


def sacct_mongodb_import(cluster, day) -> None:
    """Fetch sacct data and store it in MongoDB.

    Arguments:
        cluster: The cluster on which to fetch the data.
        day: The day for which to fetch the data. The time does not matter.
    """
    collection = jobs_collection()
    scraper = SAcctScraper(cluster, day)
    print("Getting the sacct data...")
    scraper.get_raw()
    print(f"Saving into mongodb collection '{collection.Meta.collection_name}'...")
    for entry in tqdm(scraper):
        collection.save_job(entry)
    print(f"Saved {len(scraper)} entries.")
