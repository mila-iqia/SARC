from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import ByteSize, validator
from pydantic_mongo import AbstractRepository, ObjectIdField

from sarc.config import BaseModel, config, validate_date


class AllocationCompute(BaseModel):
    gpu_year: Optional[int]
    cpu_year: Optional[int]
    vcpu_year: Optional[int]
    vgpu_year: Optional[int]


class AllocationStorage(BaseModel):
    project_size: Optional[ByteSize]
    project_inodes: Optional[float]
    nearline: Optional[ByteSize]
    dCache: Optional[ByteSize]
    object: Optional[ByteSize]
    cloud_volume: Optional[ByteSize]
    cloud_shared: Optional[ByteSize]


class AllocationRessources(BaseModel):
    compute: AllocationCompute
    storage: AllocationStorage


def convert_date_to_iso(date_value: date) -> str:
    return datetime(date_value.year, date_value.month, date_value.day).isoformat()


class Allocation(BaseModel):
    # Database ID
    id: ObjectIdField = None

    cluster_name: str
    resource_name: str
    group_name: str
    timestamp: datetime
    start: date
    end: date
    resources: AllocationRessources

    _validate_start = validator("start", pre=True, always=True, allow_reuse=True)(
        validate_date
    )
    _validate_end = validator("end", pre=True, always=True, allow_reuse=True)(
        validate_date
    )


class AllocationsRepository(AbstractRepository[Allocation]):
    class Meta:
        collection_name = "allocations"

    def add(self, allocation: Allocation):
        document = self.to_document(allocation)
        print(document)
        query_attrs = ["cluster_name", "resource_name", "group_name", "start", "end"]
        query = {key: document[key] for key in query_attrs}
        return self.get_collection().update_one(query, {"$set": document}, upsert=True)


def get_allocations_collection():
    db = config().mongo.instance

    return AllocationsRepository(database=db)


def get_allocations(
    cluster_name: str | list[str],
    start: None | date = None,
    end: None | date = None,
) -> list[Allocation]:
    collection = get_allocations_collection()

    query = {}
    if isinstance(cluster_name, str):
        query["cluster_name"] = cluster_name
    else:
        query["cluster_name"] = {"$in": cluster_name}

    if start is not None:
        query["start"] = {"$gte": convert_date_to_iso(start)}

    if end is not None:
        query["end"] = {"$lte": convert_date_to_iso(end)}

    return list(collection.find_by(query, sort=[("start", 1)]))
