from sqlmodel import Field, UniqueConstraint

from sarc.db.sqlmodel import SQLModel, datetime_utc_field
from sarc.validators import datetime_utc


class UserPeriods(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("user_id", "cluster_id"),)
    id: int | None = Field(primary_key=True, nullable=False)
    user_id: int = Field(foreign_key="users.id")
    cluster_id: int = Field(foreign_key="cluster.id")
    start_date: datetime_utc = datetime_utc_field()
    end_date: datetime_utc = datetime_utc_field()
    sm_occ_mean: float
    unused_rguh: int
    isunderuser: bool
    flagged: bool
    elevated: bool
