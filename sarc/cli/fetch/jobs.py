from dataclasses import dataclass
import logging
from simple_parsing import field

from sarc.config import config
from sarc.core.scraping.jobs import fetch_jobs


logger = logging.getLogger(__name__)


@dataclass
class FetchJobs:
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

    force: bool = field(
        action="store_true",
        help="Force recalculating the data rather than use the cache",
    )

    def execute(self) -> int:
        if self.intervals is not None and self.auto_interval is not None:
            logger.error(
                "Parameters mutually exclusive: either --intervals or --auto_interval, not both"
            )
            return -1

        clusters_cfg = config("scraping").clusters
        assert clusters_cfg is not None

        # Define if the cache is used or not
        with_cache = (config().cache is not None) and (not self.force)
        if with_cache:
            logger.info("Using the cache while fetching jobs")
        else:
            logger.info("Not using the cache while fetching jobs")

        fetch_jobs(
            self.cluster_names,
            clusters_cfg,
            self.intervals,
            self.auto_interval,
            with_cache,
        )
        return 0
