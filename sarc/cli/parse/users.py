from dataclasses import dataclass
from datetime import UTC, datetime

from simple_parsing import field

from sarc.core.scraping.users import parse_users
from sarc.users.db import get_user_collection


@dataclass
class ParseUsers:
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

        coll = get_user_collection()
        for um in parse_users(from_=_since, update_parsed_date=self.update_parsed_date):
            coll.update_user(um)

        return 0
