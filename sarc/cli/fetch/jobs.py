from __future__ import annotations

import logging
from dataclasses import dataclass

from simple_parsing import field

from sarc.config import config
from sarc.core.scraping.jobs import fetch_jobs
from sarc.cli.acquire.jobs import parse_intervals, parse_auto_intervals

logger = logging.getLogger(__name__)


@dataclass
class FetchJobs:
    cluster_names: list[str] = field(alias=["-c"], default_factory=list)

    intervals: list[str] | None = field(
        alias=["-i"],
        default=None,
        help=(
            "Fetch jobs in these intervals. "
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
            "Fetch jobs every <auto_interval> minutes "
            "since latest scraping date until now. "
            "If <= 0, use only one interval since latest scraping date until now. "
            "Mutually exclusive with --intervals."
        ),
    )

    def execute(self) -> int:
        if self.intervals is not None and self.auto_interval is not None:
            logger.error(
                "Parameters mutually exclusive: either --intervals or --auto_interval, not both"
            )
            return -1

        cfg = config("scraping")
        clusters_configs = cfg.clusters
        auto_end_field = "end_time_sacct"

        # Collect all (cluster, start, end) tuples to fetch
        clusters_and_intervals = []

        for cluster_name in self.cluster_names:
            try:
                if cluster_name not in clusters_configs:
                    logger.error(f"Unknown cluster: {cluster_name}")
                    continue

                cluster = clusters_configs[cluster_name]

                # Parse intervals
                intervals = []
                if self.intervals is not None:
                    intervals = parse_intervals(self.intervals)
                elif self.auto_interval is not None:
                    intervals = parse_auto_intervals(
                        cluster_name, auto_end_field, self.auto_interval
                    )

                if not intervals:
                    logger.warning(
                        f"No --intervals or --auto_interval parsed for {cluster_name}, nothing to do."
                    )
                    continue

                # Add to batch
                for time_from, time_to in intervals:
                    clusters_and_intervals.append((cluster, time_from, time_to))

            except Exception as e:
                logger.error(
                    f"Error preparing intervals for {cluster_name}: {type(e).__name__}: {e}"
                )
                continue

        if not clusters_and_intervals:
            logger.warning("No intervals to fetch")
            return 0

        # Fetch all jobs in a single batch (creates one cache entry)
        fetch_jobs(clusters_and_intervals)

        return 0
