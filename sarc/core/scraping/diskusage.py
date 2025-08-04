from importlib.metadata import entry_points
from typing import Any, Protocol, Type

from fabric import Connection
from serieux import deserialize

from sarc.core.models.diskusage import DiskUsage


class DiskUsageScraper[T](Protocol):
    config_type: Type[T]

    def validate_config(self, config_data: Any) -> T:
        """Validate the configuration"""
        return deserialize(self.config_type, config_data)

    def get_diskusage_report(self, ssh: Connection, config: T) -> bytes:
        """Get the raw disk usage report for caching and archiving purposes"""
        ...

    def parse_diskusage_report(
        self, config: T, cluster_name: str, data: bytes
    ) -> DiskUsage:
        """Parse previously fetched report into a DiskUsage"""
        ...


_builtin_scrapers: dict[str, DiskUsageScraper] = dict()
_diskusage_scrapers = entry_points(group="sarc.diskusage_scraper")


def get_diskusage_scraper(name: str) -> DiskUsageScraper:
    """Raises KeyError if the name is not found"""
    try:
        return _builtin_scrapers[name]
    except KeyError:
        pass
    val = _diskusage_scrapers[name]
    return val.load()
