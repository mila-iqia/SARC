from collections.abc import Callable
from typing import Any

from pydantic import ByteSize, model_serializer
from sqlalchemy import BigInteger
from sqlmodel import Field, Index, Relationship

from sarc.core.models.validators import datetime_utc

from .sqlmodel import SQLModel, datetime_utc_field


class DiskUsageUserDB(SQLModel, table=True):
    __tablename__ = "diskusage_users"
    id: int | None = Field(default=None, primary_key=True)
    group_id: int | None = Field(
        foreign_key="diskusage_groups.id", index=True, default=None, nullable=False
    )
    user: str
    nbr_files: int
    size: ByteSize = Field(sa_type=BigInteger)


class DiskUsageGroupDB(SQLModel, table=True):
    __tablename__ = "diskusage_groups"
    id: int | None = Field(default=None, primary_key=True)
    report_id: int | None = Field(
        foreign_key="diskusage_reports.id", index=True, default=None, nullable=False
    )
    group_name: str
    users: list[DiskUsageUserDB] = Relationship()

    @model_serializer(mode="wrap")
    def serialize_model(self, handler: Callable[[Any], dict]):
        data = handler(self)
        data["users"] = [
            user.model_dump(exclude={"id", "group_id"}) for user in self.users
        ]
        return data


class DiskUsageDB(SQLModel, table=True):
    __tablename__ = "diskusage_reports"
    __table_args__ = (Index("idx_cluster_time", "cluster_id", "timestamp"),)

    id: int | None = Field(default=None, primary_key=True)
    cluster_id: int = Field(foreign_key="clusters.id")
    groups: list[DiskUsageGroupDB] = Relationship()
    timestamp: datetime_utc = datetime_utc_field()

    @model_serializer(mode="wrap")
    def serialize_model(self, handler: Callable[[Any], dict]):
        data = handler(self)
        data["groups"] = [
            group.model_dump(exclude={"id", "report_id"}) for group in self.groups
        ]
        return data
