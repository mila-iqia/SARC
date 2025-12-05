from dataclasses import dataclass
from datetime import datetime
from simple_parsing import field

from sarc.config import config
from sarc.core.scraping.jobs import parse_jobs


@dataclass
class ParseJobs:
    from_: datetime = field(help="Start parsing the cache from the specified date")

    cluster_names: list[str] = field(alias=["-c"], default_factory=list)

    def execute(self) -> int:
        clusters_cfg = config("scraping").clusters
        assert clusters_cfg is not None

        parse_jobs(self.cluster_names, clusters_cfg, self.from_)

        return 0
