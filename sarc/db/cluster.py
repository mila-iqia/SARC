import bisect
from datetime import datetime

from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, Index, Relationship, Session, SQLModel, select

from sarc.core.models.validators import datetime_utc


class GPUBillingDB(SQLModel, table=True):
    """Holds data for a GPU Billing."""

    __table_args__ = (Index("idx_cluster_since_gpu_billing", "cluster_id", "since"),)

    id: int | None = Field(default=None, primary_key=True)

    cluster_id: int = Field(foreign_key="clusters.id")
    since: datetime_utc
    gpu_to_billing: dict[str, float] = Field(sa_type=JSONB)


class NodeGPUMappingDB(SQLModel, table=True):
    """Holds data for a mapping <node name> -> <GPU type>."""

    __table_args__ = (
        Index("idx_cluster_since_node_gpu_mapping", "cluster_id", "since"),
    )

    # # Database ID
    id: int | None = Field(default=None, primary_key=True)

    cluster_id: int = Field(foreign_key="clusters.id")
    since: datetime_utc
    node_to_gpu: dict[str, list[str]] = Field(sa_type=JSONB)


class SlurmClusterDB(SQLModel, table=True):
    """Hold data for a Slurm cluster."""

    __tablename__ = "clusters"

    # Database ID
    id: int | None = Field(default=None, primary_key=True)

    cluster_name: str = Field(unique=True)
    start_date: datetime_utc
    end_time_sacct: datetime_utc | None = None
    end_time_prometheus: datetime_utc | None = None
    billing_is_gpu: bool = False

    gpu_billing: list[GPUBillingDB] = Relationship(
        sa_relationship_kwargs={"order_by": GPUBillingDB.since}
    )
    node_gpu_mapping: list[NodeGPUMappingDB] = Relationship(
        sa_relationship_kwargs={"order_by": NodeGPUMappingDB.since}
    )

    def get_node_to_gpu(
        self, required_date: datetime | None = None
    ) -> NodeGPUMappingDB | None:
        if required_date is None:
            return self.node_gpu_mapping[-1]

        index_mapping = max(
            0,
            bisect.bisect_right(
                [mapping.since for mapping in self.node_gpu_mapping], required_date
            )
            - 1,
        )
        return self.node_gpu_mapping[index_mapping]

    @classmethod
    def id_by_name(cls, cluster_name: str, sess: Session) -> int | None:
        return sess.exec(
            select(cls.id).where(cls.cluster_name == cluster_name)
        ).one_or_none()
