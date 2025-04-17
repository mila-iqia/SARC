from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Optional

import pandas as pd
from flatten_dict import flatten
from pydantic import BeforeValidator, ByteSize, field_serializer
from pydantic_mongo import AbstractRepository, PydanticObjectId

from sarc.config import BaseModel, config, validate_date


class AllocationCompute(BaseModel):
    gpu_year: Optional[int] = 0
    cpu_year: Optional[int] = 0
    vcpu_year: Optional[int] = 0
    vgpu_year: Optional[int] = 0


class AllocationStorage(BaseModel):
    project_size: Optional[ByteSize] = 0
    project_inodes: Optional[float] = 0
    nearline: Optional[ByteSize] = 0
    dCache: Optional[ByteSize] = 0
    object: Optional[ByteSize] = 0
    cloud_volume: Optional[ByteSize] = 0
    cloud_shared: Optional[ByteSize] = 0


class AllocationRessources(BaseModel):
    compute: AllocationCompute
    storage: AllocationStorage


def _convert_date_to_iso(date_value: date) -> datetime:
    return datetime(date_value.year, date_value.month, date_value.day)


class Allocation(BaseModel):
    # Database ID
    id: PydanticObjectId = None

    cluster_name: str
    resource_name: str
    group_name: str
    timestamp: datetime
    start: Annotated[date, BeforeValidator(validate_date)]
    end: Annotated[date, BeforeValidator(validate_date)]
    resources: AllocationRessources

    # pylint: disable=unused-argument
    @field_serializer("start", "end")
    def save_as_datetime(self, value, info):
        return datetime(year=value.year, month=value.month, day=value.day)


class AllocationsRepository(AbstractRepository[Allocation]):
    class Meta:
        collection_name = "allocations"

    def add(self, allocation: Allocation):
        document = self.to_document(allocation)
        query_attrs = ["cluster_name", "resource_name", "group_name", "start", "end"]
        query = {key: document[key] for key in query_attrs}
        return self.get_collection().update_one(query, {"$set": document}, upsert=True)


def get_allocations_collection():
    db = config().mongo.database_instance

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
        query["start"] = {"$gte": _convert_date_to_iso(start)}

    if end is not None:
        query["end"] = {"$lte": _convert_date_to_iso(end)}

    return list(collection.find_by(query, sort=[("start", 1)]))


def increment(a, b):
    if a is None:
        return b or 0

    if b is None:
        return a

    return a + b


def get_allocation_summaries(
    cluster_name: str | list[str],
    start: None | date = None,
    end: None | date = None,
) -> pd.DataFrame:
    allocations = get_allocations(cluster_name, start=start, end=end)

    def allocation_key(allocation: Allocation):
        return (allocation.cluster_name, allocation.start, allocation.end)

    summaries = {}
    for allocation in allocations:
        key = allocation_key(allocation)
        if key in summaries:
            for field in ["cpu_year", "gpu_year", "vcpu_year", "vgpu_year"]:
                setattr(
                    summaries[key].resources.compute,
                    field,
                    increment(
                        getattr(summaries[key].resources.compute, field),
                        getattr(allocation.resources.compute, field),
                    ),
                )

            for field in [
                "project_size",
                "project_inodes",
                "nearline",
                "dCache",
                "object",
                "cloud_volume",
                "cloud_shared",
            ]:
                setattr(
                    summaries[key].resources.storage,
                    field,
                    increment(
                        getattr(summaries[key].resources.storage, field),
                        getattr(allocation.resources.storage, field),
                    ),
                )
        else:
            summaries[key] = allocation

    summaries = list(summaries.values())

    return pd.DataFrame(
        [
            flatten(
                summary.model_dump(
                    exclude={
                        "id",
                        "resource_name",
                    }
                ),
                reducer="dot",
            )
            for summary in summaries
        ]
    )
