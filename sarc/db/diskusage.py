from pydantic import ByteSize, computed_field
from sqlalchemy import BigInteger
from sqlmodel import Field, Index, Relationship

from sarc.core.models.validators import datetime_utc
from sarc.db.cluster import SlurmClusterDB

from .sqlmodel import SQLModel, datetime_utc_field


class DiskUsageUserDB(SQLModel, table=True):
    __tablename__ = "diskusage_users"
    id: int | None = Field(default=None, primary_key=True, exclude=True)
    group_id: int | None = Field(
        foreign_key="diskusage_groups.id",
        index=True,
        default=None,
        nullable=False,
        exclude=True,
    )
    user: str
    nbr_files: int
    size: ByteSize = Field(sa_type=BigInteger)


class DiskUsageGroupDB(SQLModel, table=True):
    __tablename__ = "diskusage_groups"
    id: int | None = Field(default=None, primary_key=True, exclude=True)
    report_id: int | None = Field(
        foreign_key="diskusage_reports.id",
        index=True,
        default=None,
        nullable=False,
        exclude=True,
    )
    group_name: str
    _users: list[DiskUsageUserDB] = Relationship()

    @computed_field
    @property
    def users(self) -> list[DiskUsageUserDB]:
        return self._users


class DiskUsageDB(SQLModel, table=True):
    __tablename__ = "diskusage_reports"
    __table_args__ = (Index("idx_cluster_time", "cluster_id", "timestamp"),)

    id: int | None = Field(default=None, primary_key=True)
    cluster_id: int = Field(foreign_key="clusters.id")
    cluster: SlurmClusterDB = Relationship()
    timestamp: datetime_utc = datetime_utc_field()
    _groups: list[DiskUsageGroupDB] = Relationship()

    @computed_field
    @property
    def groups(self) -> list[DiskUsageGroupDB]:
        return self._groups
