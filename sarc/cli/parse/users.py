import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from simple_parsing import field

from sarc.cache import Cache
from sarc.config import config
from sarc.db.runstate import get_parsed_date, set_parsed_date
from sarc.scraping.users import parse_ce, update_user

logger = logging.getLogger(__name__)


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
        cache = Cache(subdirectory="users")
        _since = None
        if self.since is not None:
            _since = datetime.fromisoformat(self.since).astimezone(UTC)

        with config.db.session() as sess:
            if _since is None:
                _since = get_parsed_date(sess, "users")
                if _since is None:
                    _since = cache.oldest_year()

            for ce in cache.read_from(from_time=_since):
                for um in parse_ce(ce):
                    update_user(sess, um)
                    sess.flush()
                if self.update_parsed_date:
                    logger.info(
                        f"Set parsed_dates for users to {ce.get_entry_datetime()}."
                    )
                    set_parsed_date(sess, "users", ce.get_entry_datetime())
                    sess.flush()
                sess.commit()

        return 0
