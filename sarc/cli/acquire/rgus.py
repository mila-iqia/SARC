from __future__ import annotations

import logging
from dataclasses import dataclass

from sarc.cache import CacheException, CachePolicy, with_cache
from sarc.client.rgu import _rgu_billing_collection
from sarc.config import config

logger = logging.getLogger(__name__)


@dataclass
class AcquireRGUs:
    @classmethod
    def execute(cls) -> int:
        """
        Acquire RGU mapping from cached files.

        For more info about cached data format,
        see documentation for method `fetch_gpu_type_to_rgu()` below.
        """
        collection = _rgu_billing_collection()
        for cluster_config in config().clusters.values():
            try:
                cluster_rgu_billing = fetch_gpu_type_to_rgu(
                    cluster_config.name, cache_policy=CachePolicy.always
                )
                assert isinstance(cluster_rgu_billing, dict)
                if cluster_rgu_billing:
                    collection.save_rgu_billing(
                        cluster_config.name, **cluster_rgu_billing
                    )
            except CacheException as exc:
                logger.warning(str(exc))
        return 0


def _gpu_type_to_rgu_cache_key(cluster_name: str):
    """
    Return cache key for GPU->RGU mapping file.

    We expect one file per cluster.
    """
    return f"gpu_type_to_rgu.{cluster_name}.json"


@with_cache(subdirectory="rgu", key=_gpu_type_to_rgu_cache_key)
def fetch_gpu_type_to_rgu(cluster_name: str) -> dict:
    """
    Return a GPU->RGU mapping JSON dict for given cluster.

    Dictionary must have the following format:
    {
      "rgu_start_date" : <date: str, example format: "YYYY-MM-DD">
      "gpu_to_rgu" : {
        <gpu type: str>: <RGU billing: int or float>
      }
    }

    Dictionary is expected to be read from a cache file located at:
    {config().cache}/rgu/gpu_type_to_rgu.{cluster_name}.json

    To add a new RGU mapping for given cluster,
    update cache file by changing `rgu_start_date` and `gpu_to_rgu`,
    then run `sarc acquire rgus` again.

    To fix a previous RGU mapping already registered in database,
    set cache file with registered mapping date in `rgu_start_date`
    and new mapping values in `gpu_to_rgu`,
    then run `sarc acquire rgus` again.
    """
    raise RuntimeError(
        f"Please add GPU-type-to-RGU JSON mapping file into cache, at location: "
        f"{config().cache}/rgu/{_gpu_type_to_rgu_cache_key(cluster_name)}"
    )
