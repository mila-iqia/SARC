import logging
from dataclasses import dataclass

from simple_parsing import field

from sarc.config import config
from sarc.core.scraping.diskusage import get_diskusage_scraper
from sarc.storage.diskusage import get_diskusage_collection

logger = logging.getLogger(__name__)


@dataclass
class AcquireStorages:
    cluster_names: list[str] = field(alias=["-c"], default_factory=list)
    dry: bool = False

    def execute(self) -> int:
        cfg = config("scraping")

        for cluster_name in self.cluster_names:
            logger.info(f"Acquiring {cluster_name} storages report...")

            cluster = cfg.clusters[cluster_name]
            diskusage_configs = cluster.diskusage
            if diskusage_configs is None:
                continue

            # Process each diskusage configuration
            for diskusage_config in diskusage_configs:
                try:
                    scraper = get_diskusage_scraper(diskusage_config.name)
                except KeyError as ke:
                    logger.exception(
                        "Invalid or absent diskusage scraper name: %s",
                        diskusage_config.name,
                        exc_info=ke,
                    )
                    continue

                disk_config = scraper.validate_config(diskusage_config.params)
                data = scraper.get_diskusage_report(
                    cluster.ssh, cluster_name, disk_config
                )
                du = scraper.parse_diskusage_report(data)

                if not self.dry:
                    collection = get_diskusage_collection()
                    collection.add(du)
                else:
                    logger.info("Document:\n" + du.model_dump_json(indent=2))

        return 0
