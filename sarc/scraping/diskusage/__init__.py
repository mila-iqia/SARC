from importlib.metadata import entry_points
from typing import Any, Protocol, Type

from fabric import Connection
from pydantic import BaseModel, ByteSize
from serieux import deserialize

from sarc.core.models.validators import datetime_utc


class DiskUsageUser(BaseModel):
    user: str
    nbr_files: int
    size: ByteSize


class DiskUsageGroup(BaseModel):
    group_name: str
    users: list[DiskUsageUser]


class DiskUsage(BaseModel):
    """
    Disk usage on a given cluster
    """

    cluster_name: str
    groups: list[DiskUsageGroup]
    timestamp: datetime_utc


class DiskUsageScraper[T](Protocol):
    config_type: Type[T]

    def validate_config(self, config_data: Any) -> T:
        """Validate the configuration"""
        return deserialize(self.config_type, config_data)

    def get_diskusage_report(
        self, ssh: Connection, cluster_name: str, config: T
    ) -> bytes:
        """Get the raw disk usage report for caching and archiving purposes"""
        ...

    def parse_diskusage_report(self, data: bytes) -> DiskUsage:
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


from . import drac  # noqa: E402, F401
