from dataclasses import dataclass
from datetime import UTC, datetime

from simple_parsing import field

from sarc.core.scraping.users import parse_users
from sarc.users.db import get_user_collection


@dataclass
class ParseUsers:
    since: str = field(help="Start parsing the cache from the specified date")

    def execute(self) -> int:
        ts = datetime.fromisoformat(self.since)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        ts = ts.astimezone(UTC)

        coll = get_user_collection()
        for um in parse_users(from_=ts):
            coll.update_user(um)

        return 0
