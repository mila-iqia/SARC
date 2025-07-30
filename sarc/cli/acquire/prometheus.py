"""
Script to acquire prometheus metrics.
NB: Dates are parsed in UTC timezone.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Generator

from simple_parsing import field

from sarc.config import config, UTC
from sarc.jobs.prometheus_scraping import scrap_prometheus
from sarc.traces import using_trace

logger = logging.getLogger(__name__)


def _str_to_dt(dt_str: str) -> datetime:
    return datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=UTC)


def _str_to_extended_dt(dt_str: str) -> datetime:
    """Parse date up to minute, with format %Y-%m-%dT%H:%M"""
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M").replace(tzinfo=UTC)


def parse_dates(dates: list[str]) -> list[datetime]:
    parsed_dates: list[datetime] = []
    for date in dates:
        if date.count("-") == 5:
            start = _str_to_dt("-".join(date.split("-")[:3]))
            end = _str_to_dt("-".join(date.split("-")[3:]))
            parsed_dates.extend(date for date in _daterange(start, end))
        else:
            parsed_dates.append(_str_to_dt(date))

    return parsed_dates


def _daterange(start_date: datetime, end_date: datetime) -> Generator[datetime]:
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


# pylint: disable=logging-not-lazy,too-many-branches
@dataclass
class AcquirePrometheus:
    cluster_names: list[str] = field(alias=["-c"], default_factory=list)

    dates: list[str] = field(alias=["-d"], default_factory=list)

    time_from: str | None = field(
        alias=["-a"],
        default=None,
        help=(
            "Acquire Prometheus metrics for jobs from this datetime. "
            "Expected format: %%Y-%%m-%%dT%%H:%%M (e.g.: 2020-01-01T17:00). "
            "Should be used along with --time_to. "
            "Mutually exclusive with --dates."
        ),
    )

    time_to: str | None = field(
        alias=["-b"],
        default=None,
        help=(
            "Acquire Prometheus metrics for jobs until this datetime. "
            "Expected format: %%Y-%%m-%%dT%%H:%%M  (e.g.: 2020-01-01T17:05). "
            "Should be used along with --time_from. "
            "Mutually exclusive with --dates."
        ),
    )

    def execute(self) -> int:
        time_intervals = []
        if self.dates:
            if self.time_from or self.time_to:
                logger.error(
                    "Parameters mutually exclusive: either --date "
                    "or (--time_from ... --time_to ...), not both."
                )
                return -1
            # Parse dates and convert each date to a time interval
            # of 1 day length (from date:00h00 to (date+1day):00h00)
            for date in parse_dates(self.dates):
                start = date
                end = start + timedelta(days=1)
                time_intervals.append((start, end))
        elif self.time_from or self.time_to:
            if not self.time_from or not self.time_to:
                logger.error("Both parameters needed: --time_from, --time_to")
                return -1
            time_from = _str_to_extended_dt(self.time_from)
            time_to = _str_to_extended_dt(self.time_to)
            if time_from >= time_to:
                logger.error(
                    f"Expected time_from < time_to, instead got time_from: {time_from}, time_to: {time_to}"
                )
                return -1
            time_intervals.append((time_from, time_to))

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
                for start, end in time_intervals:
                    with using_trace(
                        "AcquirePrometheus",
                        "acquire_prometheus_metrics_from_time_interval",
                        exception_types=(),
                    ) as span:
                        span.set_attribute("cluster_name", cluster_name)
                        span.set_attribute("start", str(start))
                        span.set_attribute("end", str(end))
                        interval_minutes = (end - start).total_seconds() / 60
                        try:
                            logger.info(
                                f"Acquire Prometheus metrics on {cluster_name} for jobs from "
                                f"{start} to {end} ({interval_minutes} min)"
                            )

                            scrap_prometheus(cluster, start, end)
                        # pylint: disable=broad-exception-caught
                        except Exception as e:
                            logger.error(
                                f"Failed to acquire Prometheus metrics on {cluster_name} for interval: "
                                f"{start} to {end} ({interval_minutes} min): {type(e).__name__}: {e}"
                            )
                            raise e
            # pylint: disable=broad-exception-caught
            except Exception as e:
                logger.error(
                    f"Error while acquiring Prometheus metrics on {cluster_name}: "
                    f"{type(e).__name__}: {e} ; skipping cluster."
                )
                # Continue to next cluster.
                continue
        return 0
