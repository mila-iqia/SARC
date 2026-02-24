from __future__ import annotations

import logging
from types import SimpleNamespace

from iguane.fom import RAWDATA, fom_ugr
from pydantic import BaseModel
from pydantic_mongo import AbstractRepository, PydanticObjectId

from sarc.config import config, scraping_mode_required
from sarc.core.models.validators import datetime_utc

logger = logging.getLogger(__name__)


class GPUBilling(BaseModel):
    """Holds data for a GPU Billing."""

    # # Database ID
    id: PydanticObjectId | None = None

    cluster_name: str
    since: datetime_utc
    gpu_to_billing: dict[str, float]


class GPUBillingRepository(AbstractRepository[GPUBilling]):
    class Meta:
        collection_name = "gpu_billing"

    @scraping_mode_required
    def save_gpu_billing(
        self,
        cluster_name: str,
        since: datetime_utc,
        gpu_to_billing: dict[str, float],
    ) -> None:
        """Save GPU->billing mapping into database."""

        billing = GPUBilling(
            cluster_name=cluster_name,
            since=since,  # type: ignore[arg-type]
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


def _gpu_billing_collection() -> GPUBillingRepository:
    """Return the gpu_billing collection in the current MongoDB."""
    db = config().mongo.database_instance
    return GPUBillingRepository(database=db)


def get_cluster_gpu_billings(cluster_name: str) -> list[GPUBilling]:
    """Return GPU->billing mapping records for a cluster, sorted by ascending `since`."""
    return sorted(
        _gpu_billing_collection().find_by({"cluster_name": cluster_name}),
        key=lambda b: b.since,
    )


def get_rgus(rgu_version: str = "1.0") -> dict[str, float]:
    """
    Return GPU->RGU mapping for given RGU version.

    Get mapping from package IGUANE.
    """
    args = SimpleNamespace(fom_version=rgu_version, custom_weights=None, norm=False)
    gpus = sorted(RAWDATA.keys())
    return {gpu: fom_ugr(gpu, args=args) for gpu in gpus}
