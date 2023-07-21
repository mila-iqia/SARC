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


def parse_dates(dates: list[str], cluster_name: str) -> list[(datetime,bool)]:
    parsed_dates = []  # return values are tuples (date, is_auto)
    for date in dates:
        if date=="auto":
            # TODO: remember this auto settings and construct dates from the last valid date+1 in the database
            dates_auto = _dates_auto(cluster_name)
            parsed_dates.extend([(date,True) for date in dates_auto])          
            pass
        elif date.count("-") == 5:
            start = _str_to_dt("-".join(date.split("-")[:3]))
            end = _str_to_dt("-".join(date.split("-")[3:]))
            parsed_dates.extend([(date,False) for date in _daterange(start, end)])  
        else:
            parsed_dates.append((_str_to_dt(date),False))

    return parsed_dates


def _daterange(
    start_date: datetime, end_date: datetime
) -> Generator[datetime, None, None]:
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def _dates_auto(cluster_name: str) -> list[datetime]:
    # we want to get the list of dates from the last valid date+1 in the database, until yesterday
    dates = []
    start = _dates_auto_first_date(cluster_name)
    end = datetime.today()
    return _daterange(start, end)

def _dates_auto_first_date(cluster_name: str) -> datetime:
    # get the last valid date in the database for the cluster
    # TEMPORARY !!!!!!
    return _str_to_dt("2023-07-01")



@dataclass
class AcquireJobs:
    cluster_names: list[str] = field(
        alias=["-c"], default_factory=list, choices=clusters
    )

    dates: list[str] = field(alias=["-d"], default_factory=list)

    ignore_statistics: bool = field(
        alias=["-s"],
        action="store_true",
        help="Ignore statistics, avoiding connection to prometheus (default: False)",
    )

    def execute(self) -> int:
        cfg = config()
        clusters_configs = cfg.clusters

        for cluster_name in self.cluster_names:
            for date,is_auto in parse_dates(self.dates,cluster_name):
                
                print(f"Acquire data on {cluster_name} for date: {date} (is_auto={is_auto})")

                sacct_mongodb_import(
                    clusters_configs[cluster_name], date, self.ignore_statistics
                )

        return 0
