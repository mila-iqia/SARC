from dataclasses import dataclass
from datetime import datetime

from simple_parsing import field

from sarc.config import config
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
        users_cfg = config("scraping").users
        assert users_cfg is not None

        coll = get_user_collection()
        for um in parse_users(
            list(users_cfg.scrapers.items()), from_=self.from_, force=self.force
        ):
            coll.update_user(um)

        return 0
