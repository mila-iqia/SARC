import logging
from dataclasses import dataclass
from datetime import datetime

from simple_parsing import field

from sarc.cache import Cache
from sarc.config import UTC, ClusterConfig, config

logger = logging.getLogger(__name__)


@dataclass
class FetchSlurmConfig:
    """Download slurm.conf file for given cluster at current time."""

    cluster_names: list[str] = field(alias=["-c"], default_factory=list)

    def execute(self) -> int:

        cache = Cache(subdirectory="slurm_conf")

        with cache.create_entry(datetime.now(UTC)) as ce:
            for cluster_name in self.cluster_names:
                try:
                    file_content = _download_slurm_conf_file(
                        config.clusters[cluster_name]
                    )
                    ce.add_value(cluster_name, file_content.encode("utf-8"))
                except Exception as e:
                    logger.exception("Skipping cluster %s", cluster_name, exc_info=e)

        return 0


def _download_slurm_conf_file(cluster: ClusterConfig) -> str:
    """Download slurm.conf file for given cluster."""
    cmd = f"cat {cluster.slurm_conf_host_path}"
    result = cluster.ssh.run(cmd, hide=True)
    return result.stdout
