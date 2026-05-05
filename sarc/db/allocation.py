from datetime import date, datetime
from typing import Annotated

from pydantic import BeforeValidator, ByteSize
from sqlalchemy import BigInteger
from sqlmodel import Field, Relationship, Session, select

from sarc.core.models.validators import datetime_utc

from .cluster import SlurmClusterDB
from .sqlmodel import SQLModel, datetime_utc_field


def validate_date(value: str | date | datetime) -> date:
    if isinstance(value, str):
        if "T" in value:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S").date()

        return datetime.strptime(value, "%Y-%m-%d").date()

    if isinstance(value, datetime):
        return value.date()

    return value


class AllocationDB(SQLModel, table=True):
    # Database ID
    id: int | None = Field(default=None, primary_key=True)
    cluster_id: int = Field(foreign_key="clusters.id")
    cluster: SlurmClusterDB = Relationship()

    resource_name: str
    group_name: str
    timestamp: datetime_utc = datetime_utc_field()
    start: Annotated[date, BeforeValidator(validate_date)]
    end: Annotated[date, BeforeValidator(validate_date)]
    gpu_year: int | None = None
    cpu_year: int | None = None
    rgu_year: int | None = None
    vcpu_year: int | None = None
    vgpu_year: int | None = None
    project_inodes: float | None = None
    cloud_volume: ByteSize | None = Field(default=None, sa_type=BigInteger)
    cloud_shared: ByteSize | None = Field(default=None, sa_type=BigInteger)
    project_size: ByteSize | None = Field(default=None, sa_type=BigInteger)
    nearline_size: ByteSize | None = Field(default=None, sa_type=BigInteger)
    dCache: ByteSize | None = Field(default=None, sa_type=BigInteger)
    object: ByteSize | None = Field(default=None, sa_type=BigInteger)

    @classmethod
    def get_or_create(cls, sess: Session, **kwargs) -> AllocationDB:
        res = AllocationDB.model_validate(kwargs)
        res.id = sess.exec(
            select(AllocationDB.id).where(
                AllocationDB.cluster_id == res.cluster_id,
                AllocationDB.resource_name == res.resource_name,
                AllocationDB.group_name == res.group_name,
                AllocationDB.start == res.start,
                AllocationDB.end == res.end,
            )
        ).one_or_none()
        return sess.merge(res)
