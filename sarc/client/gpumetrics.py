from __future__ import annotations

import logging
from datetime import datetime
from types import SimpleNamespace
from typing import Dict, List

import iguane
from pydantic import validator
from pydantic_mongo import AbstractRepository, ObjectIdField

from sarc.config import MTL, UTC, BaseModel, config, scraping_mode_required

logger = logging.getLogger(__name__)


class GPUBilling(BaseModel):
    """Holds data for a GPU Billing."""

    # # Database ID
    id: ObjectIdField = None

    cluster_name: str
    billing_start_date: datetime
    gpu_to_billing: Dict[str, float]

    @validator("billing_start_date", pre=True)
    @classmethod
    def _ensure_billing_start_date(cls, value):
        """Parse billing_start_date from stored string to Python datetime."""
        if isinstance(value, str):
            return datetime.fromisoformat(value).astimezone(MTL)
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
        billing_start_date: str,
        gpu_to_billing: Dict[str, float],
    ):
        """Save GPU->billing mapping into database."""

        billing = GPUBilling(
            cluster_name=cluster_name,
            billing_start_date=billing_start_date,
            gpu_to_billing=gpu_to_billing,
        )
        # Check if a GPU->billing mapping was already registered
        # for given cluster and date.
        exists = list(
            self.find_by(
                {
                    "cluster_name": billing.cluster_name,
                    "billing_start_date": billing.billing_start_date,
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
                        "billing_start_date": billing.billing_start_date,
                    },
                    {"$set": self.to_document(billing)},
                )
                logger.info(
                    f"[{billing.cluster_name}] GPU<->billing updated for: {billing.billing_start_date}"
                )
        else:
            # If no record found, create a new one.
            self.save(billing)
            logger.info(
                f"[{billing.cluster_name}] GPU<->billing saved for: {billing.billing_start_date}"
            )


def _gpu_billing_collection():
    """Return the gpu_billing collection in the current MongoDB."""
    db = config().mongo.database_instance
    return GPUBillingRepository(database=db)


def get_cluster_gpu_billings(cluster_name: str) -> List[GPUBilling]:
    """Return GPU->billing mapping records for a cluster, sorted by ascending billing_start_date."""
    return sorted(
        _gpu_billing_collection().find_by({"cluster_name": cluster_name}),
        key=lambda b: b.billing_start_date,
    )


def get_rgus(rgu_version="1.0") -> Dict[str, float]:
    """Return GPU->RGU mapping for given RGU version."""
    fom = iguane.fom_ugr
    args = SimpleNamespace(ugr_version=rgu_version)
    gpus = sorted(iguane.RAWDATA.keys())
    return {gpu: fom(gpu, args=args) for gpu in gpus}
