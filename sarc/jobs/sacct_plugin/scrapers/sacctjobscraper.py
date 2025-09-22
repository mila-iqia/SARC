from datetime import date, datetime, time, timedelta
from invoke.runners import Result
import json
import logging
import subprocess

from sarc.config import ClusterConfig
from sarc.core.scraping.jobs import JobScraper
from sarc.traces import trace_decorator


logger = logging.getLogger(__name__)


class SacctJobScraper(JobScraper):
    """
    Scrape jobs info from Slurm using the sacct command
    """

    name = "Sacct Scraper"
    config_type = ClusterConfig

    def __init__(self, cluster: ClusterConfig):
        """
        Initialize a SacctScraper.

        Parameters:
            cluster: The cluster on which to scrape the data
        """
        self.cluster = cluster

    @trace_decorator()
    def fetch_raw(self, day:datetime) -> dict:
        """
        Fetch the raw sacct data as a dict via SSH, or run sacct locally

        This is done by launching a sacct command requesting the jobs for the given day
        (from 00:00 the day to 00:00 the next day) and return the
        unchanged results into a JSON dictionary.

        Parameters:
            day             The day for which we want to get the jobs
                            (this is done from 00:00 the morning to
                            00:00 the following night)

        Returns:
            A JSON dictionary containing the unchanged command result
        """
        
        # Set the day
        start = datetime.combina(day, time.min) # We scrape the day from OO:OO...
        end = start + timedelta(days=1) # ... to 00:00 the next day

        # Format sacct command

        # - times
        format = "%Y-%m-%dT%H:%M"
        start = start.strftime(format)
        end = end.strftime(format)

        # - accouts
        accounts = ",".join(self.cluster.accounts) if self.cluster.accounts else None
        accounts_option = f"-A {accounts}" if accounts else ""

        # Final command
        cmd = f"{self.cluster.sacct_bin} {accounts_option} -X -S {start} -E {end} --allusers --json"

        # Logger
        logger.debug(f"{self.cluster.name} $ {cmd}")


        if self.cluster.host == "localhost":
            results: subprocess.CompletedProcess[str] | Result = subprocess.run(
                cmd, shell=True, text=True, capture_output=True, check=False
            )
        else:
            results = self.cluster.ssh.run(cmd, hide=True)
        return json.loads(results.stdout[results.stdout.find("{") :])


    def _cache_key(self) -> str | None:
        """
        Define the name of the file in which we will store the results
        when calling get_raw.

        Returns:
            The name of the file as a string if the requested day is
            in the past, None otherwise
        """
        today = datetime.combine(date.today(), datetime.min.time())
        if self.day < today:
            daystr = self.day.strftime("%Y-%m-%d")
            return f"{self.cluster.name}.{daystr}.json"
        else:
            # Not cachable
            return None

    @with_cache(subdirectory="sacct", key=_cache_key, live=True)  # type: ignore[arg-type] # mypy has some trouble with methods
    def get_raw(self, day: datetime) -> dict:
        """
        Fetch the raw sacct data as a dict via SSH, or run sacct locally
        by using a cache

        Parameters:
            day             The day for which we want to get the jobs
                            (this is done from 00:00 the morning to
                            00:00 the following night)
        """
        return self.fetch_raw(day)

