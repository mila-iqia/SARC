import logging
from dataclasses import dataclass
from datetime import datetime

from simple_parsing import field

from sarc.cache import Cache
from sarc.core.scraping.diskusage import get_diskusage_scraper
from sarc.storage.diskusage import get_diskusage_collection

logger = logging.getLogger(__name__)


@dataclass
class ParseDiskUsage:
    from_: datetime = field(help="Start parsing the cache from the specified date")

    def execute(self) -> int:
        cache = Cache("disk_usage")
        collection = get_diskusage_collection()
        for ce in cache.read_from(self.from_):
            for item in ce.items():
                scraper = get_diskusage_scraper(item[0])
                collection.add(scraper.parse_diskusage_report(item[1]))

        return 0
