import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from simple_parsing import field

from sarc.cache import Cache
from sarc.config import config
from sarc.core.scraping.diskusage import get_diskusage_scraper

logger = logging.getLogger(__name__)


@dataclass
class FetchDiskUsage:
    cluster_names: list[str] = field(alias=["-c"], default_factory=list)

    def execute(self) -> int:
        cfg = config("scraping")
        cache = Cache("disk_usage")

        with cache.create_entry(datetime.now(UTC)) as ce:
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
                    try:
                        disk_config = scraper.validate_config(diskusage_config.params)
                    except Exception as e:
                        logger.exception(
                            "Could not parse config for: %s",
                            diskusage_config.name,
                            exc_info=e,
                        )
                        continue
                    try:
                        data = scraper.get_diskusage_report(
                            cluster.ssh, cluster_name, disk_config
                        )
                    except Exception as e:
                        logger.exception(
                            "Could not fetch data for: %s",
                            diskusage_config.name,
                            exc_info=e,
                        )
                    ce.add_value(key=f"{diskusage_config.name}", value=data)

        return 0
