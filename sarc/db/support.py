from sqlmodel import Field

from .sqlmodel import SQLModel


class GpuRguDB(SQLModel, table=True):
    name: str = Field(primary_key=True)
    rgu: float
