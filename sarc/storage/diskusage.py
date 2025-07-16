from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from pydantic_mongo import AbstractRepository, PydanticObjectId

from sarc.config import config
from sarc.core.models.diskusage import DiskUsage


class DiskUsageDB(DiskUsage):
    """
    Disk usage on a given cluster
    """

    # Database ID
    id: PydanticObjectId | None = None


class ClusterDiskUsageRepository(AbstractRepository[DiskUsageDB]):
    class Meta:
        collection_name = "diskusage"

    def add(self, disk_usage: DiskUsage):
        # we only keep the date part of the timestamp
        # this way we keep only one report per day and per cluster
        # mongo does not support the date format, we have to stick to datetime format
        disk_usage.timestamp.date
        day_at_midnight = datetime(
            year=disk_usage.timestamp.year,
            month=disk_usage.timestamp.month,
            day=disk_usage.timestamp.day,
        )
        disk_usage.timestamp = day_at_midnight
        document = self.to_document(DiskUsageDB(**disk_usage.model_dump()))
        query_attrs = ["cluster_name", "timestamp"]
        query = {key: document[key] for key in query_attrs}
        self.get_collection().update_one(query, {"$set": document}, upsert=True)


def get_diskusage_collection() -> ClusterDiskUsageRepository:
    db = config().mongo.database_instance
    return ClusterDiskUsageRepository(database=db)


def _convert_date_to_iso(date_value: date) -> datetime:
    return datetime(date_value.year, date_value.month, date_value.day, tzinfo=UTC)


# pylint: disable=duplicate-code
def get_diskusages(
    cluster_name: str | list[str],
    start: None | date = None,
    end: None | date = None,
) -> list[DiskUsage]:
    collection = get_diskusage_collection()

    query: dict[str, Any] = {}
    if isinstance(cluster_name, str):
        query["cluster_name"] = cluster_name
    else:
        query["cluster_name"] = {"$in": cluster_name}

    if start is not None:
        query["timestamp"] = {"$gte": _convert_date_to_iso(start)}

    if end is not None:
        query["timestamp"] = {"$lte": _convert_date_to_iso(end)}

    return list(collection.find_by(query, sort=[("timestamp", 1)]))
