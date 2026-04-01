import logging
from collections import defaultdict
from datetime import datetime

from sarc.cache import Cache
from sarc.client.job import SlurmJob, _jobs_collection
from sarc.config import UTC, ClusterConfig, config
from sarc.core.models.runstate import get_parsed_date, set_parsed_date
from sarc.core.models.users import UserData
from sarc.core.models.validators import DateMatchError
from sarc.core.scraping.jobs_utils import (
    DATE_FORMAT_HOUR,
    fetch_raw,
    parse_auto_intervals,
    parse_intervals,
    parse_raw,
    set_auto_end_time,
    update_allocated_gpu_type_from_nodes,
)
from sarc.traces import using_trace
from sarc.users.db import get_users

logger = logging.getLogger(__name__)


def fetch_jobs(
    cluster_names: list[str],
    clusters: dict[str, ClusterConfig],
    unparsed_intervals: list[str] | None,
    auto_interval: int | None,
    max_intervals: int | None = None,
) -> None:
    """
    Fetch jobs and place the results in cache.

    Parameters:
        cluster_names           List of the names of the clusters on which we want
                                to fetch the jobs

        clusters                Dictionary linking a cluster name to the
                                associated cluster config

        unparsed_intervals      Intervals during which we want to fetch the jobs.
                                Expected format for each interval: <date-from>-<date-to>,
                                with <date-from> and <date-to> in format: YYYY-MM-DDTHH:mm
                                (e.g.: 2020-01-01T17:05-2020-01-01T18:00).
                                Dates will be interpreted as UTC.
                                Mutually exclusive with --auto_interval.

        auto_interval           Acquire jobs every <auto_interval> minutes since latest scraping date until now.
                                If <= 0, use only one interval since latest scraping date until now. Mutually
                                exclusive with --intervals.

        max_intervals
                               Only fetch that many intervals at maximum when using auto_intervals.
                               The number fetched can be lower.
    """

    auto_end_field = "end_time_sacct"  # Used to parse the intervals MODIFIER CECI en end_time_sacct_fetch
    cluster_endtime: dict[str, datetime] = {}

    def _fetch_jobs(
        cluster_name: str,
        cluster_configs: dict[str, ClusterConfig],
        intervals: list[tuple[datetime, datetime]],
        auto_interval: int | None,
    ):
        assert cluster_name in cluster_configs
        cluster_config = cluster_configs[cluster_name]
        # Fetch the jobs for each time interval
        for time_from, time_to in intervals:
            with using_trace(
                "FetchJobs",
                "acquire_cluster_data_from_time_interval",
                exception_types=(),
            ) as span:
                span.set_attribute("cluster_name", cluster_name)
                span.set_attribute("time_from", str(time_from))
                span.set_attribute("time_to", str(time_to))
                try:
                    logger.info(
                        f"Fetching the sacct data for cluster {cluster_config.name}, time {time_from} to {time_to}..."
                    )
                    key = f"{cluster_name}_{time_from.strftime(DATE_FORMAT_HOUR)}_{time_to.strftime(DATE_FORMAT_HOUR)}"

                    raw_data = fetch_raw(cluster_config, time_from, time_to)
                    yield (key, raw_data)

                    if auto_interval is not None:
                        cluster_endtime[cluster_name] = time_to

                # pylint: disable=broad-exception-caught
                except Exception as e:
                    logger.error(
                        f"Failed to fetch data on {cluster_name} for interval: "
                        f"{time_from} to {time_to}: {type(e).__name__}: {e}"
                    )
                    raise e

    # Define cache directory
    cache = Cache(subdirectory="jobs")

    try:
        with cache.create_entry(datetime.now(UTC)) as cache_entry:
            for cluster_name in cluster_names:
                # Define the time intervals on which we want to retrieve the jobs
                intervals: list[tuple[datetime, datetime]] = []

                try:
                    if unparsed_intervals is not None:
                        intervals = parse_intervals(unparsed_intervals)
                    elif auto_interval is not None:
                        intervals = parse_auto_intervals(
                            cluster_name, auto_end_field, auto_interval, max_intervals
                        )
                    if not intervals:
                        logger.warning(
                            "No --intervals or --auto_interval parsed, nothing to do."
                        )
                        continue

                    for cache_key, raw_data in _fetch_jobs(
                        cluster_name, clusters, intervals, auto_interval
                    ):
                        cache_entry.add_value(
                            key=cache_key,  # f"{cluster_name}_{time_from.strftime(DATE_FORMAT_HOUR)}_{time_to.strftime(DATE_FORMAT_HOUR)}"
                            value=raw_data,  # sortie stdout de sacct : bytes
                        )

                # pylint: disable=broad-exception-caught
                except Exception as e:
                    logger.error(
                        f"Error while fetching data on {cluster_name}: {type(e).__name__}: {e} ; skipping cluster."
                    )
                # Continue to next cluster.
                continue
    finally:
        for cluster_name, time_to in cluster_endtime.items():
            set_auto_end_time(cluster_name, auto_end_field, time_to)


def parse_jobs(
    clusters_cfg: dict[str, ClusterConfig],
    since: datetime | None,
    update_parsed_date: bool,
) -> None:
    user_map = UserMap()
    collection = _jobs_collection()
    db = config().mongo.database_instance

    if since is None:
        since = get_parsed_date(db, "jobs")

    # Retrieve from the cache
    cache = Cache(subdirectory="jobs")
    for cache_entry in cache.read_from(from_time=since):
        logger.info(
            f"Parsing slurm jobs from cache entry: {cache_entry.get_entry_datetime()}"
        )
        nb_entries = 0
        nb_linked_to_users = 0

        # Retrieve all jobs associated to the time intervals
        # The cache entry is designed to yield the jobs intervals
        # in the same order they were added, i.e. in chronological order.
        for key, value in cache_entry.items():
            logger.info(f"Parsing slurm jobs identified by: {key}...")

            cluster_name = key.split("_")[0]
            scraped_start = datetime.fromisoformat(key.split("_")[1]).replace(
                tzinfo=UTC
            )
            scraped_end = datetime.fromisoformat(key.split("_")[2]).replace(tzinfo=UTC)

            for entry in parse_raw(value, cluster_name, scraped_start, scraped_end):
                if entry is not None:
                    nb_entries += 1
                    update_allocated_gpu_type_from_nodes(
                        clusters_cfg[cluster_name], entry
                    )
                    nb_linked_to_users += user_map.solve_user(entry)
                    collection.save_job(entry)

        # Update the parsed date
        if update_parsed_date:
            logger.info(
                f"Set parsed_dates for jobs to {cache_entry.get_entry_datetime()}."
            )
            set_parsed_date(db, "jobs", cache_entry.get_entry_datetime())

        logger.info(f"Saved {nb_entries} entries.")
        logger.info(f"Linked {nb_linked_to_users} / {nb_entries} entries to users.")


class UserMap:
    """
    Helper class mapping users to credentials.
    Used to find UserData object associated to a job.
    """

    def __init__(self) -> None:
        # Map cluster name to account domain
        # (e.g. "mila" => "mila", "narval" => "drac")
        self._cluster_domain: dict[str, str] = {}
        # Map user credential (domain, username) to user object
        self.__users: dict[tuple[str, str], list[UserData]] = {}

        for cluster_config in config("scraping").clusters.values():
            assert cluster_config.name is not None
            self._cluster_domain[cluster_config.name] = cluster_config.user_domain

        users = get_users()

        # Map all users including duplicates, using all historical usernames
        indexed_users = defaultdict(list)
        for user in users:
            for domain, cred in user.associated_accounts.items():
                for tag in cred.values:
                    username = tag.value
                    user_key = (domain, username)
                    indexed_users[user_key].append(user)

        self.__users = indexed_users

        logger.info(f"{len(users)} user(s), {len(self.__users)} credential(s)")

    def solve_user(self, entry: SlurmJob) -> bool:
        """
        Main method to link a job to a user.
        Update SlurmJob.user_uuid if not already set.
        Return True if user_uuid updated, False otherwise.

        We match credentials by (domain, username) and verify that the
        credential was valid at the job's submit time. This temporal check
        is conceptually correct but currently produces false negatives
        because user scraping plugins do not provide reliable validity dates:

        - MILA LDAP (sarc/users/mila_ldap.py) sets start=scrape_time,
          so a job submitted before the first scraping time won't be matched
          to the user, even if the user credential was already valid.
          Fix: use the account creation date as start instead of scrape_time.

        - DRAC (sarc/users/drac.py) sets start=member_since and end=scrape_time,
          so a job submitted after the last scraping time (and before next
          scraping time) won't be matched to the user, even if the user
          credential was still valid.
          Fix: leave end open (None) or use an explicit expiration date.
        """
        if entry.user_uuid is None:
            domain = self._cluster_domain.get(entry.cluster_name)
            if domain is not None:
                user_key = (domain, entry.user)
                if user_key in self.__users:
                    valid_users = []
                    for candidate_user in self.__users[user_key]:
                        try:
                            username_at_submit = candidate_user.associated_accounts[
                                domain
                            ].get_value(entry.submit_time)
                            if entry.user == username_at_submit:
                                valid_users.append(candidate_user)
                        except DateMatchError:
                            pass
                    if valid_users:
                        if len(valid_users) > 1:
                            logger.warning(
                                f"Job {entry.cluster_name}/{entry.job_id}/{entry.submit_time}: "
                                f"expected 1 matching user, found {len(valid_users)}: "
                                + ", ".join(
                                    f"{u.email} ({u.uuid})" for u in valid_users
                                )
                            )
                        else:
                            (user,) = valid_users
                            entry.user_uuid = user.uuid
                            return True
        return False
