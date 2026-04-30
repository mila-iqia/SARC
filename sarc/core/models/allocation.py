from datetime import date, datetime
from typing import Annotated, cast

from pydantic import BeforeValidator, ByteSize
from sqlmodel import Field

from sarc.db.sqlmodel import SQLModel


def validate_date(value: str | date | datetime) -> date:
    if isinstance(value, str):
        if "T" in value:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S").date()

        return datetime.strptime(value, "%Y-%m-%d").date()

    if isinstance(value, datetime):
        return value.date()

    return value


class Allocation(SQLModel):
    cluster_id: int = Field(foreign_key="clusters.id")
    resource_name: str
    group_name: str
    timestamp: datetime
    start: Annotated[date, BeforeValidator(validate_date)]
    end: Annotated[date, BeforeValidator(validate_date)]
    gpu_year: int | None = 0
    cpu_year: int | None = 0
    rgu_year: int | None = 0
    vcpu_year: int | None = 0
    vgpu_year: int | None = 0
    project_size: ByteSize | None = cast(ByteSize, 0)
    project_inodes: float | None = 0
    nearline: ByteSize | None = cast(ByteSize, 0)
    dCache: ByteSize | None = cast(ByteSize, 0)
    object: ByteSize | None = cast(ByteSize, 0)
    cloud_volume: ByteSize | None = cast(ByteSize, 0)
    cloud_shared: ByteSize | None = cast(ByteSize, 0)
