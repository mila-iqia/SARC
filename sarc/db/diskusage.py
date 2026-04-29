from datetime import date

from pydantic import ByteSize
from sqlalchemy import BigInteger
from sqlmodel import Field, Index, Relationship

from .sqlmodel import SQLModel


class DiskUsageUserDB(SQLModel, table=True):
    __tablename__ = "diskusage_users"
    id: int | None = Field(default=None, primary_key=True)
    group_id: int = Field(foreign_key="diskusage_groups.id", index=True)
    user: str
    nbr_files: int
    size: ByteSize = Field(sa_type=BigInteger)


class DiskUsageGroupDB(SQLModel, table=True):
    __tablename__ = "diskusage_groups"
    id: int | None = Field(default=None, primary_key=True)
    report_id: int = Field(foreign_key="diskusage_reports.id", index=True)
    group_name: str
    users: list[DiskUsageUserDB] = Relationship()


class DiskUsageDB(SQLModel, table=True):
    __tablename__ = "diskusage_reports"
    __table_args__ = (Index("idx_cluster_time", "cluster_name", "timestamp"),)

    id: int | None = Field(default=None, primary_key=True)
    cluster_name: str
    groups: list[DiskUsageGroupDB] = Relationship()
    timestamp: date
