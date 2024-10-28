from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List

from sarc.cache import CacheException, CachePolicy, with_cache
from sarc.client.gpumetrics import _gpu_billing_collection
from sarc.config import config

logger = logging.getLogger(__name__)


@dataclass
class AcquireGPUBillings:
    @classmethod
    def execute(cls) -> int:
        """
        Acquire `GPU -> billing` mappings from cached files.

        For more info about cached data format,
        see documentation for method `fetch_gpu_type_to_rgu()` below.

        About RGU and GPU billing
        -------------------------
        For a GPU, we consider 2 metrics:
        - RGU, which represents how many times 1 unit of this GPU
          is equivalent to a reference GPU.
          RGU is computed at runtime using project `IGUANE`.
        - GPU billing, which represents the billing for 1 unit of this GPU.
          GPU billing cannot yet be inferred, and may vary depending on time and cluster,
          so it must be manually described (in cached files)
          and then imported in SARC (with this script)
          to help compute further job metrics.

        For a job with a given GPU billing (`gres_gpu`)
        running on a specific GPU, we have the following relations:
        <job GPU billing> = <number of GPU units used by job> * <billing for this GPU>
        <job RGU Billing> = <number of GPU units used by job> * <RGU for this GPU>

        So, job RGU Billing can be computed using job GPU billing and GPU metrics.
        """
        collection = _gpu_billing_collection()
        for cluster_config in config().clusters.values():
            try:
                cluster_gpu_billings = fetch_gpu_type_to_billing(
                    cluster_config.name, cache_policy=CachePolicy.always
                )
                assert isinstance(cluster_gpu_billings, list)
                for cluster_gpu_billing in cluster_gpu_billings:
                    collection.save_gpu_billing(
                        cluster_config.name, **cluster_gpu_billing
                    )
            except CacheException as exc:
                logger.warning(str(exc))
        return 0


def _gpu_type_to_billing_cache_key(cluster_name: str):
    """
    Return cache key for `GPU -> billing` mapping file.

    We expect one file per cluster.
    """
    return f"gpu_type_to_billing.{cluster_name}.json"


@with_cache(subdirectory="gpu_billing", key=_gpu_type_to_billing_cache_key)
def fetch_gpu_type_to_billing(cluster_name: str) -> List[dict]:
    """
    Return a list of `GPU -> billing` mapping dicts for given cluster.

    Each dictionary must have the following format:
    {
      "billing_start_date" : <date: str, example format: "YYYY-MM-DD">
      "gpu_to_billing" : {
        <gpu type: str>: <billing: int or float>
      }
    }

    Dictionaries list is expected to be read from a cache file located at:
    {config().cache}/gpu_billing/gpu_type_to_billing.{cluster_name}.json
    """
    raise RuntimeError(
        f"Please add GPU->billing mappings JSON file into cache, at location: "
        f"{config().cache}/gpu_billing/{_gpu_type_to_billing_cache_key(cluster_name)}"
    )
