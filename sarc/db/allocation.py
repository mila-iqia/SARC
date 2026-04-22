from datetime import date, datetime
from typing import cast

from pydantic import ByteSize
from sqlmodel import Field, SQLModel


class Allocation(SQLModel, table=True):
    # Database ID
    id: int | None = Field(default=None, primary_key=True)

    cluster_id: int = Field(foreign_key="clusters.id")
    resource_name: str
    group_name: str
    timestamp: datetime
    start: date
    end: date

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
