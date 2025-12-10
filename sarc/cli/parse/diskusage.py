import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from simple_parsing import field

from sarc.cache import Cache
from sarc.core.scraping.diskusage import get_diskusage_scraper
from sarc.storage.diskusage import get_diskusage_collection

logger = logging.getLogger(__name__)


@dataclass
class ParseDiskUsage:
    from_: str = field(
        alias="--from", help="Start parsing the cache from the specified date"
    )

    def execute(self) -> int:
        ts = datetime.fromisoformat(self.from_)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        ts = ts.astimezone(UTC)

        cache = Cache("disk_usage")
        collection = get_diskusage_collection()
        for ce in cache.read_from(ts):
            for item in ce.items():
                scraper = get_diskusage_scraper(item[0])
                collection.add(scraper.parse_diskusage_report(item[1]))

        return 0
