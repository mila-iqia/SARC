import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from simple_parsing import field

from sarc.cache import Cache
from sarc.config import config
from sarc.db.diskusage import DiskUsageDB
from sarc.scraping.diskusage import get_diskusage_scraper

logger = logging.getLogger(__name__)


@dataclass
class ParseDiskUsage:
    from_: str = field(
        alias="--from",
        help="Start parsing the cache from the specified date. "
        "NB: Naive date will be interpreted as UTC.",
    )

    def execute(self) -> int:
        ts = datetime.fromisoformat(self.from_)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        ts = ts.astimezone(UTC)

        cache = Cache("disk_usage")
        with config().db.session() as sess:
            for ce in cache.read_from(ts):
                for item in ce.items():
                    scraper = get_diskusage_scraper(item[0])
                    sess.add(
                        DiskUsageDB.model_validate(
                            scraper.parse_diskusage_report(item[1])
                        )
                    )
                sess.flush()
            sess.commit()

        return 0
