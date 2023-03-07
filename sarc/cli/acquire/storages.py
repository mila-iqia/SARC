from dataclasses import dataclass
from pathlib import Path

from simple_parsing import field

from sarc.config import config
from sarc.storage.diskusage import get_diskusage_collection
from sarc.storage.drac import (
    convert_parsed_report_to_diskusage,
    fetch_diskusage_report,
    parse_diskusage_report,
)


@dataclass
class AcquireStorages:
    file: Path = field(default=None)
    cluster_names: list[str] = field(
        alias=["-c"], default_factory=list, choices=list(config().clusters.keys())
    )

    def execute(self) -> int:
        cfg = config()
        clusters_configs = cfg.clusters  # pylint: disable=unused-variable

        for cluster_name in self.cluster_names:
            print(f"Acquiring {cluster_name} storages report...")

            report = None

            if self.file:
                with open(self.file, "r", encoding="utf-8") as f:
                    report = f.readlines()
            else:
                cluster = config().clusters[cluster_name]
                report = fetch_diskusage_report(cluster)
            # pylint: disable=unused-variable
            header, body = parse_diskusage_report(report)

            du = convert_parsed_report_to_diskusage(cluster_name, body)

            collection = get_diskusage_collection()
            collection.add(du)

            # raise NotImplementedError()

        return 0
