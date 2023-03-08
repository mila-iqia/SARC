import itertools
from dataclasses import dataclass
from datetime import datetime

from simple_parsing import field

from sarc.cli.utils import clusters
from sarc.config import config
from sarc.jobs.sacct import sacct_mongodb_import


@dataclass
class AcquireJobs:
    cluster_names: list[str] = field(
        alias=["-c"], default_factory=list, choices=clusters
    )

    dates: list[str] = field(alias=["-d"], default_factory=list)

    def execute(self) -> int:
        cfg = config()
        clusters_configs = cfg.clusters

        for cluster_name, date_string in itertools.product(
            self.cluster_names, self.dates
        ):
            date = datetime.strptime(date_string, "%Y-%m-%d")
            print(f"Acquire data on {cluster_name} for date: {date}")
            sacct_mongodb_import(clusters_configs[cluster_name], date)

        return 0
