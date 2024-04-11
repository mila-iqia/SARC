from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Generator

from simple_parsing import field

from sarc.config import config
from sarc.errors import ClusterNotFound
from sarc.jobs.sacct import sacct_mongodb_import
from sarc.traces import using_trace


def _str_to_dt(dt_str: str) -> datetime:
    return datetime.strptime(dt_str, "%Y-%m-%d")


def parse_dates(dates: list[str], cluster_name: str) -> list[(datetime, bool)]:
    parsed_dates = []  # return values are tuples (date, is_auto)
    for date in dates:
        if date == "auto":
            # is_auto is set to True to indicate that the database collection `clusters`
            # should be updated if scraping successful
            dates_auto = _dates_auto(cluster_name)
            parsed_dates.extend([(date, True) for date in dates_auto])
        elif date.count("-") == 5:
            start = _str_to_dt("-".join(date.split("-")[:3]))
            end = _str_to_dt("-".join(date.split("-")[3:]))
            parsed_dates.extend([(date, False) for date in _daterange(start, end)])
        else:
            parsed_dates.append((_str_to_dt(date), False))

    return parsed_dates


def _daterange(
    start_date: datetime, end_date: datetime
) -> Generator[datetime, None, None]:
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def _dates_auto(cluster_name: str) -> list[datetime]:
    # we want to get the list of dates from the last valid date+1 in the database, until yesterday
    start = _dates_auto_first_date(cluster_name)
    end = datetime.today()
    return _daterange(start, end)


def _dates_auto_first_date(cluster_name: str) -> datetime:
    # get the last valid date in the database for the cluster
    # pylint: disable=broad-exception-raised
    db = config().mongo.database_instance
    db_collection = db.clusters
    cluster = db_collection.find_one({"cluster_name": cluster_name})
    if cluster is None:
        raise ClusterNotFound(f"Cluster {cluster_name} not found in database")
    start_date = cluster["start_date"]
    logging.info(f"start_date={start_date}")
    end_date = cluster["end_date"]
    logging.info(f"end_date={end_date}")
    if end_date is None:
        return _str_to_dt(start_date)
    return _str_to_dt(end_date) + timedelta(days=1)


def _dates_set_last_date(cluster_name: str, date: datetime) -> None:
    # set the last valid date in the database for the cluster
    logging.info(f"set last successful date for cluster {cluster_name} to {date}")
    db = config().mongo.database_instance
    db_collection = db.clusters
    db_collection.update_one(
        {"cluster_name": cluster_name},
        {"$set": {"end_date": date.strftime("%Y-%m-%d")}},
        upsert=True,
    )


@dataclass
class AcquireJobs:
    cluster_names: list[str] = field(alias=["-c"], default_factory=list)

    dates: list[str] = field(alias=["-d"], default_factory=list)

    no_prometheus: bool = field(
        alias=["-s"],
        action="store_true",
        help="Avoid any scraping requiring connection to prometheus (default: False)",
    )

    def execute(self) -> int:
        cfg = config()
        clusters_configs = cfg.clusters

        for cluster_name in self.cluster_names:
            try:
                for date, is_auto in parse_dates(self.dates, cluster_name):
                    with using_trace(
                        "AcquireJobs", "acquire_cluster_data", exception_types=()
                    ) as span:
                        span.set_attribute("cluster_name", cluster_name)
                        span.set_attribute("date", str(date))
                        span.set_attribute("is_auto", is_auto)
                        try:
                            logging.info(
                                f"Acquire data on {cluster_name} for date: {date} (is_auto={is_auto})"
                            )

                            sacct_mongodb_import(
                                clusters_configs[cluster_name], date, self.no_prometheus
                            )
                            if is_auto:
                                _dates_set_last_date(cluster_name, date)
                        # pylint: disable=broad-exception-caught
                        except Exception as e:
                            logging.error(
                                f"Failed to acquire data for {cluster_name} on {date}: {e}"
                            )
                            raise e
            # pylint: disable=broad-exception-caught
            except Exception:
                # Error while acquiring data on a cluster from given dates.
                # Continue to next cluster.
                continue
        return 0
