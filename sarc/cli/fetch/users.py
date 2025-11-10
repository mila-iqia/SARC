from dataclasses import dataclass

from simple_parsing import field

from sarc.config import config
from sarc.core.scraping.users import fetch_users


@dataclass
class FetchUsers:
    force: bool = field(
        action="store_true",
        help="Force recalculating the data rather than use the cache",
    )

    def execute(self) -> int:
        users_cfg = config("scraping").users
        assert users_cfg is not None

        fetch_users(list(users_cfg.scrapers.items()))
        return 0
