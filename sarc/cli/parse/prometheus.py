from dataclasses import dataclass
from datetime import UTC, datetime

from simple_parsing import field

from sarc.jobs.prometheus_scraping import parse_prometheus


@dataclass
class ParsePrometheus:
    since: str | None = field(
        default=None,
        help="Start parsing the cache from the specified date, otherwise use the last parsed date from the database. "
        "NB: Naive date will be interpreted as in local timezone.",
    )
    update_parsed_date: bool = field(
        default=True, help="Update the last parsed date in the database"
    )

    def execute(self) -> int:
        _since = None
        if self.since is not None:
            _since = datetime.fromisoformat(self.since).astimezone(UTC)
        parse_prometheus(_since, self.update_parsed_date)
        return 0
