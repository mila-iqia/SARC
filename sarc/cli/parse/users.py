from dataclasses import dataclass
from datetime import datetime

from simple_parsing import field

from sarc.core.scraping.users import parse_users
from sarc.users.db import get_user_collection


@dataclass
class ParseUsers:
    force: bool = field(
        action="store_true",
        help="Re-parse from cached data even if the database contains the data",
    )

    from_: datetime = field(help="Start parsing the cache from the specified date")

    def execute(self) -> int:
        coll = get_user_collection()
        for um in parse_users(from_=self.from_):
            coll.update_user(um)

        return 0
