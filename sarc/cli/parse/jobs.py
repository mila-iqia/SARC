from dataclasses import dataclass
from sarc.core.scraping.jobs import parse_jobs
from simple_parsing import field
from datetime import datetime
from sarc.cache import UTC


@dataclass
class ParseJobs:
    since: str | None = field(
        default=None,
        help="Start parsing the cache from the specified date, otherwise use the last parsed date from the database",
    )
    update_parsed_date: bool = field(
        default=True, help="Update the last parsed date in the database"
    )

    def execute(self) -> int:
        _since = None
        if self.since is not None:
            _since = datetime.fromisoformat(self.since).astimezone(UTC)
        parse_jobs(_since, self.update_parsed_date)
        return 0
