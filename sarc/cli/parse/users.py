import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from simple_parsing import field

from sarc.config import config
from sarc.db.runstate import get_parsed_date, set_parsed_date
from sarc.db.users import deduplicate_users
from sarc.scraping.users import parse_ce, parse_users, update_user

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

        _since = None
        if self.since is not None:
            _since = datetime.fromisoformat(self.since).astimezone(UTC)

        with config("scraping").db.session() as sess:
            if _since is None:
                _since = get_parsed_date(sess, "users")
                sess.commit()
            for ce in parse_users(from_=_since):
                with sess.begin():
                    for um in parse_ce(ce):
                        update_user(sess, um)
                        sess.flush()
                    if self.update_parsed_date:
                        logger.info(
                            f"Set parsed_dates for users to {ce.get_entry_datetime()}."
                        )
                        set_parsed_date(sess, "users", ce.get_entry_datetime())
                        sess.flush()
                    deduplicate_users(sess)
                    sess.commit()

        return 0
