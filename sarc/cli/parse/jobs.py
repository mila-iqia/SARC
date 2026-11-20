from dataclasses import dataclass
from datetime import datetime

from simple_parsing import field
from sqlmodel import Session

from sarc.cache import UTC
from sarc.config import config
from sarc.patch import declare_patch
from sarc.scraping.jobs import parse_jobs


@declare_patch
def patch_jobs(sess: Session) -> None:
    pass


@dataclass
class ParseJobs:
    since: str | None = field(
        default=None,
        help="Start parsing the cache from the specified date, otherwise use the last parsed date from the database. "
        "NB: Naive date will be interpreted as in local timezone.",
    )
    update_parsed_date: bool = field(
        default=True, help="Update the last parsed date in the database"
    )

    def execute(self) -> int:
        clusters_cfg = config.clusters
        assert clusters_cfg is not None
        _since = None
        if self.since is not None:
            _since = datetime.fromisoformat(self.since).astimezone(UTC)
        parse_jobs(clusters_cfg, _since, self.update_parsed_date)
        # Reapply the manual corrections the cache contradicts
        with config.db.session() as sess:
            patch_jobs(sess)
            sess.commit()
        return 0
