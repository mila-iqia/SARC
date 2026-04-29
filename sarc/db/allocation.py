from collections.abc import Sequence
from datetime import date
from typing import cast

from pydantic import ByteSize
from sqlalchemy import BigInteger
from sqlmodel import Field, Relationship, Session, col, select

from sarc.core.models.allocation import Allocation
from sarc.db.cluster import SlurmClusterDB


class AllocationDB(Allocation, table=True):
    # Database ID
    id: int | None = Field(default=None, primary_key=True)

    cluster: SlurmClusterDB = Relationship()

    project_size: ByteSize | None = Field(default=cast(ByteSize, 0), sa_type=BigInteger)
    nearline: ByteSize | None = Field(default=cast(ByteSize, 0), sa_type=BigInteger)
    dCache: ByteSize | None = Field(default=cast(ByteSize, 0), sa_type=BigInteger)
    object: ByteSize | None = Field(default=cast(ByteSize, 0), sa_type=BigInteger)
    cloud_volume: ByteSize | None = Field(default=cast(ByteSize, 0), sa_type=BigInteger)
    cloud_shared: ByteSize | None = Field(default=cast(ByteSize, 0), sa_type=BigInteger)

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


def get_allocations(
    sess: Session,
    cluster_name: str | list[str],
    start: None | date = None,
    end: None | date = None,
) -> Sequence[AllocationDB]:
    from .cluster import SlurmClusterDB

    query = select(AllocationDB)

    if isinstance(cluster_name, str):
        query = query.where(
            AllocationDB.cluster_id == SlurmClusterDB.id_by_name(sess, cluster_name)
        )
    else:
        cluster_ids = [SlurmClusterDB.id_by_name(sess, name) for name in cluster_name]
        query = query.where(col(AllocationDB.cluster_id).in_(cluster_ids))

    if start is not None:
        query = query.where(AllocationDB.start >= start)

    if end is not None:
        query = query.where(AllocationDB.end <= end)

    return sess.exec(query.order_by(col(AllocationDB.start))).all()
