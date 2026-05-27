from dataclasses import dataclass

from sarc.config import config
from sarc.scraping.users import fetch_users


@dataclass
class FetchUsers:
    def execute(self) -> int:
        users_cfg = config.users
        assert users_cfg is not None

        fetch_users(list(users_cfg.scrapers.items()))
        return 0
