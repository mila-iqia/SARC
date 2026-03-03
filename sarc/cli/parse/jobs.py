from dataclasses import dataclass

from sarc.config import config
from sarc.core.scraping.jobs import parse_jobs

@dataclass
class ParseJobs:
    def execute(self) -> int:
        clusters_cfg = config("scraping").clusters
        assert clusters_cfg is not None

        parse_jobs(clusters_cfg)

        return 0
