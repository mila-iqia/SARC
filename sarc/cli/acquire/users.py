from __future__ import annotations

from dataclasses import dataclass

from simple_parsing import field

from sarc.cache import CachePolicy
from sarc.config import config
from sarc.core.scraping.users import scrape_users
from sarc.users.db import get_user_collection


@dataclass
class AcquireUsers:
    force: bool = field(
        action="store_true",
        help="Force recalculating the data rather than use the cache",
    )

    no_fetch: bool = field(
        action="store_true",
        help="Use cached data",
    )

    def execute(self) -> int:
        if self.force:
            assert not self.no_fetch
            cache_policy = CachePolicy.refresh
        elif self.no_fetch:
            cache_policy = CachePolicy.always
        else:
            cache_policy = CachePolicy.use

        users_cfg = config("scraping").users
        assert users_cfg is not None

        coll = get_user_collection()
        for um in scrape_users(list(users_cfg.scrapers.items()), cache_policy):
            coll.update_user(um)

        return 0
