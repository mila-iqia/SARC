from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel

from sarc.core.models.validators import datetime_utc


class GPUBillingDB(SQLModel, table=True):
    """Holds data for a GPU Billing."""

    id: int | None = Field(default=None, primary_key=True)

    cluster_id: int = Field(foreign_key="clusters.id")
    since: datetime_utc
    gpu_to_billing: dict[str, float] = Field(sa_type=JSONB)


class NodeGPUMappingDB(SQLModel, table=True):
    """Holds data for a mapping <node name> -> <GPU type>."""

    # # Database ID
    id: int | None = None

    cluster_id: int = Field(foreign_key="clusters.id")
    since: datetime_utc
    node_to_gpu: dict[str, list[str]] = Field(sa_type=JSONB)

    def __lt__(self, other):
        return self.since < other.since


class SlurmCluster(SQLModel, table=True):
    """Hold data for a Slurm cluster."""

    __tablename__ = "clusters"

    # Database ID
    id: int | None = Field(default=None, primary_key=True)

    cluster_name: str = Field(unique=True)
    start_date: datetime_utc
    end_time_sacct: datetime_utc | None = None
    end_time_prometheus: datetime_utc | None = None
    billing_is_gpu: bool = False
