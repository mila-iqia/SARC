from dataclasses import dataclass

from simple_parsing import field

from sarc.config import config
from sarc.storage.diskusage import get_diskusage_collection
from sarc.storage.drac import fetch_diskusage_report as fetch_dirac_diskusage
from sarc.storage.mila import fetch_diskusage_report as fetch_mila_diskusage

methods = {
    "default": fetch_dirac_diskusage,
    "mila": fetch_mila_diskusage,
}


@dataclass
class AcquireStorages:
    cluster_names: list[str] = field(alias=["-c"], default_factory=list)
    dry: bool = False

    def execute(self) -> int:
        cfg = config()

        for cluster_name in self.cluster_names:
            print(f"Acquiring {cluster_name} storages report...")

            cluster = cfg.clusters[cluster_name]

            fetch_diskusage = methods.get(cluster_name, methods["default"])
            du = fetch_diskusage(cluster)

            if not self.dry:
                collection = get_diskusage_collection()
                collection.add(du)
            else:
                print("Document:")
                print(du.json(indent=2))

        return 0
