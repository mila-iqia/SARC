from __future__ import annotations

import itertools
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Generator

from simple_parsing import field

from sarc.cli.utils import clusters
from sarc.config import config
from sarc.jobs.sacct import sacct_mongodb_import


def _str_to_dt(dt_str: str) -> datetime:
    return datetime.strptime(dt_str, "%Y-%m-%d")


def parse_dates(dates: list[str]) -> list[datetime]:
    parsed_dates = []
    for date in dates:
        if date.count("-") == 5:
            start = _str_to_dt("-".join(date.split("-")[:3]))
            end = _str_to_dt("-".join(date.split("-")[3:]))
            parsed_dates += list(_daterange(start, end))
        else:
            parsed_dates.append(_str_to_dt(date))

    return parsed_dates


def _daterange(
    start_date: datetime, end_date: datetime
) -> Generator[datetime, None, None]:
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


@dataclass
class AcquireJobs:
    cluster_names: list[str] = field(
        alias=["-c"], default_factory=list, choices=clusters
    )

    dates: list[str] = field(alias=["-d"], default_factory=list)

    def execute(self) -> int:
        cfg = config()
        clusters_configs = cfg.clusters

        for cluster_name, date in itertools.product(
            self.cluster_names, parse_dates(self.dates)
        ):
            print(f"Acquire data on {cluster_name} for date: {date}")
            sacct_mongodb_import(clusters_configs[cluster_name], date)

        return 0
