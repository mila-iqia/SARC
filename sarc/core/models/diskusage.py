from pydantic import BaseModel, ByteSize

from .validators import datetime_utc


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
