from dataclasses import dataclass
from pathlib import Path

from simple_parsing import field

from sarc.config import config
from sarc.storage.diskusage import get_diskusage_collection
from sarc.storage.drac import convert_parsed_report_to_diskusage, parse_diskusage_report


@dataclass
class AcquireStorages:
    file: Path
    cluster_names: list[str] = field(
        alias=["-c"], default_factory=list, choices=list(config().clusters.keys())
    )

    def execute(self) -> int:
        cfg = config()
        clusters_configs = cfg.clusters  # pylint: disable=unused-variable

        for cluster_name in self.cluster_names:
            print(f"Acquiring {cluster_name} storages report...")

            f = open(self.file, "r")
            report = f.readlines()
            f.close()
            header, body = parse_diskusage_report(report)

            print(body)

            du = convert_parsed_report_to_diskusage(cluster_name, body)

            collection = get_diskusage_collection()
            collection.add(du)

            # raise NotImplementedError()

        return 0
