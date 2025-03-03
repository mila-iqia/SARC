from __future__ import annotations

import bisect
import logging
from datetime import datetime, time
from typing import Dict, List, Optional

from pydantic import field_validator
from pydantic_mongo import AbstractRepository, PydanticObjectId

from sarc.config import MTL, UTC, config, scraping_mode_required
from sarc.model import BaseModel

logger = logging.getLogger(__name__)


class NodeGPUMapping(BaseModel):
    """Holds data for a mapping <node name> -> <GPU type>."""

    # # Database ID
    id: PydanticObjectId = None

    cluster_name: str
    since: datetime
    node_to_gpu: Dict[str, List[str]]

    @field_validator("since", mode="before")
    @classmethod
    def _ensure_since(cls, value):
        """Parse `since` from stored string to Python datetime."""
        if isinstance(value, str):
            return datetime.combine(datetime.fromisoformat(value), time.min).replace(
                tzinfo=MTL
            )
        else:
            assert isinstance(value, datetime)
            return value.replace(tzinfo=UTC).astimezone(MTL)

    def __lt__(self, other):
        return self.since < other.since


class NodeGPUMappingRepository(AbstractRepository[NodeGPUMapping]):
    class Meta:
        collection_name = "node_gpu_mapping"

    @scraping_mode_required
    def save_node_gpu_mapping(
        self, cluster_name: str, since: str, node_to_gpu: Dict[str, List[str]]
    ):
        mapping = NodeGPUMapping(
            cluster_name=cluster_name, since=since, node_to_gpu=node_to_gpu
        )
        # Check if a node->GPU mapping was already registered
        # for given cluster and date.
        exists = list(
            self.find_by(
                {
                    "cluster_name": mapping.cluster_name,
                    "since": mapping.since,
                }
            )
        )
        if exists:
            # If a record was found, update it if changed.
            (prev_mapping,) = exists
            if prev_mapping.node_to_gpu != mapping.node_to_gpu:
                self.get_collection().update_one(
                    {
                        "cluster_name": mapping.cluster_name,
                        "since": mapping.since,
                    },
                    {"$set": self.to_document(mapping)},
                )
                logger.info(
                    f"[{mapping.cluster_name}] node<->GPU updated for: {mapping.since}"
                )
        else:
            # If no record found, create a new one.
            self.save(mapping)
            logger.info(
                f"[{mapping.cluster_name}] node<->GPU saved for: {mapping.since}"
            )


def _node_gpu_mapping_collection():
    """Return the node_gpu_mapping collection in the current MongoDB."""
    db = config().mongo.database_instance
    return NodeGPUMappingRepository(database=db)


def get_node_to_gpu(
    cluster_name: str, required_date: Optional[datetime] = None
) -> Optional[NodeGPUMapping]:
    mappings = sorted(
        _node_gpu_mapping_collection().find_by({"cluster_name": cluster_name}),
        key=lambda b: b.since,
    )
    if not mappings:
        return None

    if required_date is None:
        return mappings[-1]

    index_mapping = max(
        0,
        bisect.bisect_right([mapping.since for mapping in mappings], required_date) - 1,
    )
    return mappings[index_mapping]
