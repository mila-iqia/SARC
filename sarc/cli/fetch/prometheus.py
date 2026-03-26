import logging
from dataclasses import dataclass
from datetime import datetime

from simple_parsing import field

from sarc.config import config
from sarc.core.scraping.jobs_utils import parse_intervals, set_auto_end_time
from sarc.jobs.prometheus_scraping import (
    AUTO_END_FIELD,
    fetch_prometheus,
    parse_prometheus_auto_intervals,
)
from sarc.traces import using_trace

logger = logging.getLogger(__name__)


@dataclass
class FetchPrometheus:
    cluster_names: list[str] = field(alias=["-c"], default_factory=list)

    intervals: list[str] | None = field(
        alias=["-i"],
        default=None,
        help=(
            "Acquire Prometheus metrics in these intervals. "
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
            "Acquire Prometheus metrics every <auto_interval> minutes "
            "since latest scraping date until now. "
            "If <= 0, use only one interval since latest scraping date until now. "
            "Mutually exclusive with --intervals."
        ),
    )

    max_intervals: int | None = field(
        type=int, default=None, help="Max number of intervals to fetch"
    )

    def execute(self) -> int:
        if self.intervals is not None and self.auto_interval is not None:
            logger.error(
                "Parameters mutually exclusive: either --intervals or --auto_interval, not both"
            )
            return -1

        cfg = config("scraping")
        clusters_configs = cfg.clusters

        for cluster_name in self.cluster_names:
            cluster = clusters_configs[cluster_name]
            if not cluster.prometheus_url:
                logger.error(
                    f"No prometheus URL for cluster: {cluster_name}, cannot get Prometheus metrics."
                )
                continue
            try:
                intervals: list[tuple[datetime, datetime]] = []
                if self.intervals is not None:
                    intervals = parse_intervals(self.intervals)
                elif self.auto_interval is not None:
                    intervals = parse_prometheus_auto_intervals(
                        cluster_name, self.auto_interval, self.max_intervals
                    )
                if not intervals:
                    logger.warning(
                        "No --intervals or --auto_interval parsed, nothing to do."
                    )
                    continue

                for time_from, time_to in intervals:
                    with using_trace(
                        "FetchPrometheus",
                        "fetch_prometheus_metrics_from_time_interval",
                        exception_types=(),
                    ) as span:
                        span.set_attribute("cluster_name", cluster_name)
                        span.set_attribute("time_from", str(time_from))
                        span.set_attribute("time_to", str(time_to))
                        interval_minutes = (time_to - time_from).total_seconds() / 60
                        try:
                            logger.info(
                                f"Acquire Prometheus metrics on {cluster_name} for jobs from "
                                f"{time_from} to {time_to} ({interval_minutes} min)"
                            )

                            fetch_prometheus(cluster, time_from, time_to)

                            if self.auto_interval is not None:
                                set_auto_end_time(cluster_name, AUTO_END_FIELD, time_to)
                        # pylint: disable=broad-exception-caught
                        except Exception as e:
                            logger.error(
                                f"Failed to fetch Prometheus metrics on {cluster_name} for interval: "
                                f"{time_from} to {time_to} ({interval_minutes} min): {type(e).__name__}: {e}"
                            )
                            raise e
            except Exception as e:
                logger.error(
                    f"Error while acquiring Prometheus metrics on {cluster_name}: "
                    f"{type(e).__name__}: {e} ; skipping cluster."
                )
                # Continue to next cluster.
                continue
        return 0
