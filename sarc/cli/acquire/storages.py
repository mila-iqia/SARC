import logging
from dataclasses import dataclass
from typing import Callable

from simple_parsing import field

from sarc.config import ClusterConfig, config
from sarc.storage.diskusage import DiskUsage, get_diskusage_collection
from sarc.storage.drac import fetch_diskusage_report as fetch_dirac_diskusage
from sarc.storage.mila import fetch_diskusage_report as fetch_mila_diskusage

logger = logging.getLogger(__name__)

methods: dict[str, Callable[[ClusterConfig], DiskUsage]] = {
    "default": fetch_dirac_diskusage,
    "mila": fetch_mila_diskusage,
}


@dataclass
class AcquireStorages:
    cluster_names: list[str] = field(alias=["-c"], default_factory=list)
    dry: bool = False

    def execute(self) -> int:
        cfg = config("scraping")

        for cluster_name in self.cluster_names:
            logger.info(f"Acquiring {cluster_name} storages report...")

            cluster = cfg.clusters[cluster_name]

            fetch_diskusage = methods.get(cluster_name, methods["default"])
            du = fetch_diskusage(cluster)

            if not self.dry:
                collection = get_diskusage_collection()
                collection.add(du)
            else:
                logger.info("Document:\n" + du.model_dump_json(indent=2))

        return 0
