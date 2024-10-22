from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List

from pydantic import validator
from pydantic_mongo import AbstractRepository, ObjectIdField

from sarc.config import MTL, UTC, BaseModel, config, scraping_mode_required

logger = logging.getLogger(__name__)


class RGUBilling(BaseModel):
    """Holds data for a RGU Billing."""

    # # Database ID
    id: ObjectIdField = None

    cluster_name: str
    rgu_start_date: datetime
    gpu_to_rgu: Dict[str, float]

    @validator("rgu_start_date", pre=True)
    def _ensure_rgu_start_date(cls, value):
        """Parse rgu_start_date from stored string to Python datetime."""
        if isinstance(value, str):
            return datetime.fromisoformat(value).astimezone(MTL)
        else:
            assert isinstance(value, datetime)
            return value.replace(tzinfo=UTC).astimezone(MTL)


class RGUBillingRepository(AbstractRepository[RGUBilling]):
    class Meta:
        collection_name = "rgu_billing"

    @scraping_mode_required
    def save_rgu_billing(
        self,
        cluster_name: str,
        rgu_start_date: str,
        gpu_to_rgu: Dict[str, float],
    ):
        """Save RGU mapping into database."""

        billing = RGUBilling(
            cluster_name=cluster_name,
            rgu_start_date=rgu_start_date,
            gpu_to_rgu=gpu_to_rgu,
        )
        # Check if an RGU mapping was already registered
        # for given cluster and date.
        exists = list(
            self.find_by(
                {
                    "cluster_name": billing.cluster_name,
                    "rgu_start_date": billing.rgu_start_date,
                }
            )
        )
        if exists:
            # If a record was found, update it
            # if RGU mapping changed.
            (prev_billing,) = exists
            if prev_billing.gpu_to_rgu != billing.gpu_to_rgu:
                self.get_collection().update_one(
                    {
                        "cluster_name": billing.cluster_name,
                        "rgu_start_date": billing.rgu_start_date,
                    },
                    {"$set": self.to_document(billing)},
                )
                logger.info(
                    f"[{billing.cluster_name}] GPU<->RGU mapping updated for: {billing.rgu_start_date}"
                )
        else:
            # If no record found, create a new one.
            self.save(billing)
            logger.info(
                f"[{billing.cluster_name}] GPU<->RGU mapping saved for: {billing.rgu_start_date}"
            )


def _rgu_billing_collection():
    """Return the rgu_billing collection in the current MongoDB."""
    db = config().mongo.database_instance
    return RGUBillingRepository(database=db)


def get_cluster_rgus(cluster_name: str) -> List[RGUBilling]:
    """Return RGU mapping records for a cluster, sorted by ascending rgu_start_date."""
    return sorted(
        _rgu_billing_collection().find_by({"cluster_name": cluster_name}),
        key=lambda b: b.rgu_start_date,
    )
