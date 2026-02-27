from dataclasses import dataclass
from datetime import datetime, UTC
from simple_parsing import field

from sarc.config import config
from sarc.core.scraping.jobs import parse_jobs


@dataclass
class ParseJobs:
    since: str = field(help="Start parsing the cache from the specified date, otherwise use the last parsed date from the database")

    def execute(self) -> int:
        clusters_cfg = config("scraping").clusters
        assert clusters_cfg is not None

        # TODO: get the last parsed date from the database if self.since is not provided

        ts = datetime.fromisoformat(self.since)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        ts = ts.astimezone(UTC)

        parse_jobs(clusters_cfg, ts)

        # TODO: update the last parsed date in the database if self.since was not provided

        return 0
