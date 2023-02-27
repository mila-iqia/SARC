from dataclasses import dataclass

from simple_parsing import field

from sarc.config import config


@dataclass
class AcquireStorages:
    cluster_names: list[str] = field(
        alias=["-c"], default_factory=list, choices=list(config().clusters.keys())
    )

    def execute(self) -> int:
        cfg = config()
        clusters_configs = cfg.clusters

        for cluster_name in self.cluster_names:
            clusters_configs[cluster_name]
            print(f"Acquire data on {cluster_name}.")
            # storage_mongodb_import(clusters_configs[cluster_name])
            raise NotImplementedError()

        return 0
