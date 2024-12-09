from __future__ import annotations

import logging
from datetime import datetime, time
from typing import Dict, List

from pydantic import validator
from pydantic_mongo import AbstractRepository, ObjectIdField

from sarc.config import MTL, UTC, BaseModel, config, scraping_mode_required

logger = logging.getLogger(__name__)


class GPUBilling(BaseModel):
    """Holds data for a GPU Billing."""

    # # Database ID
    id: ObjectIdField = None

    cluster_name: str
    since: datetime
    gpu_to_billing: Dict[str, float]

    @validator("since", pre=True)
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


class GPUBillingRepository(AbstractRepository[GPUBilling]):
    class Meta:
        collection_name = "gpu_billing"

    @scraping_mode_required
    def save_gpu_billing(
        self,
        cluster_name: str,
        since: str,
        gpu_to_billing: Dict[str, float],
    ):
        """Save GPU->billing mapping into database."""

        billing = GPUBilling(
            cluster_name=cluster_name,
            since=since,
            gpu_to_billing=gpu_to_billing,
        )
        # Check if a GPU->billing mapping was already registered
        # for given cluster and date.
        exists = list(
            self.find_by(
                {
                    "cluster_name": billing.cluster_name,
                    "since": billing.since,
                }
            )
        )
        if exists:
            # If a record was found, update it if changed.
            (prev_billing,) = exists
            if prev_billing.gpu_to_billing != billing.gpu_to_billing:
                self.get_collection().update_one(
                    {
                        "cluster_name": billing.cluster_name,
                        "since": billing.since,
                    },
                    {"$set": self.to_document(billing)},
                )
                logger.info(
                    f"[{billing.cluster_name}] GPU<->billing updated for: {billing.since}"
                )
        else:
            # If no record found, create a new one.
            self.save(billing)
            logger.info(
                f"[{billing.cluster_name}] GPU<->billing saved for: {billing.since}"
            )


def _gpu_billing_collection():
    """Return the gpu_billing collection in the current MongoDB."""
    db = config().mongo.database_instance
    return GPUBillingRepository(database=db)


def get_cluster_gpu_billings(cluster_name: str) -> List[GPUBilling]:
    """Return GPU->billing mapping records for a cluster, sorted by ascending `since`."""
    return sorted(
        _gpu_billing_collection().find_by({"cluster_name": cluster_name}),
        key=lambda b: b.since,
    )
