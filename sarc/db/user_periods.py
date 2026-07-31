from sqlmodel import Field, UniqueConstraint

from sarc.db.sqlmodel import SQLModel, datetime_utc_field
from sarc.validators import datetime_utc


class UserPeriods(SQLModel, table=True):
    __tablename__ = "user_periods"
    __table_args__ = (UniqueConstraint("user_id", "cluster_id", "end_date"),)

    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="users.id", index=True)
    cluster_id: int = Field(foreign_key="clusters.id", index=True)
    start_date: datetime_utc = datetime_utc_field()
    end_date: datetime_utc = datetime_utc_field(index=True)
    sm_occ_mean: float
    rgu_hours: float
    unused_rguh: float
    isunderuser: bool
    flagged: bool
    elevated: bool
