import bisect
from collections.abc import Sequence
from datetime import date, datetime
from typing import Self

from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, Index, Relationship, Session, select

from sarc.core.models.validators import datetime_utc

from .sqlmodel import SQLModel


class GPUBillingDB(SQLModel, table=True):
    """Holds data for a GPU Billing."""

    __table_args__ = (Index("idx_cluster_since_gpu_billing", "cluster_id", "since"),)

    id: int | None = Field(default=None, primary_key=True)

    cluster_id: int = Field(foreign_key="clusters.id")
    since: datetime_utc
    gpu_to_billing: dict[str, float] = Field(sa_type=JSONB)

    @classmethod
    def get_or_create(cls, sess: Session, **kwargs) -> Self:
        res = cls.model_validate(kwargs)
        res.id = sess.exec(
            select(cls.id).where(
                cls.cluster_id == res.cluster_id, cls.since == res.since
            )
        ).one_or_none()
        return sess.merge(res)


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

    @classmethod
    def get_or_create(cls, sess: Session, **kwargs) -> Self:
        res = cls.model_validate(kwargs)
        res.id = sess.exec(
            select(cls.id).where(
                cls.cluster_id == res.cluster_id, cls.since == res.since
            )
        ).one_or_none()
        return sess.merge(res)


class SlurmClusterDB(SQLModel, table=True):
    """Hold data for a Slurm cluster."""

    __tablename__ = "clusters"

    # Database ID
    id: int | None = Field(default=None, primary_key=True)

    cluster_name: str = Field(unique=True)
    domain: str
    start_date: date
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

        index_mapping = (
            bisect.bisect_right(
                [mapping.since for mapping in self.node_gpu_mapping], required_date
            )
            - 1
        )
        if index_mapping < 0:
            return None
        else:
            return self.node_gpu_mapping[index_mapping]

    def get_gpu_billing(
        self, required_date: datetime | None = None
    ) -> GPUBillingDB | None:
        if required_date is None:
            return self.gpu_billing[-1]
        index_mapping = (
            bisect.bisect_right(
                [mapping.since for mapping in self.gpu_billing], required_date
            )
            - 1
        )
        if index_mapping < 0:
            return None
        else:
            return self.gpu_billing[index_mapping]

    @classmethod
    def id_by_name(cls, sess: Session, cluster_name: str) -> int | None:
        return sess.exec(
            select(cls.id).where(cls.cluster_name == cluster_name)
        ).one_or_none()

    @classmethod
    def by_name(cls, sess: Session, cluster_name: str) -> Self | None:
        return sess.exec(
            select(cls).where(cls.cluster_name == cluster_name)
        ).one_or_none()


def get_available_clusters(sess: Session) -> Sequence[SlurmClusterDB]:
    """Get clusters available in database."""
    return sess.exec(select(SlurmClusterDB)).all()
