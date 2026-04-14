import logging
from dataclasses import dataclass
from datetime import datetime

from simple_parsing import field

from sarc.cache import Cache
from sarc.config import UTC, ClusterConfig, config
from sarc.core.models.validators import datetime_utc

logger = logging.getLogger(__name__)


@dataclass
class FetchSlurmConfig:
    """Download slurm.conf file for given cluster at current time."""

    cluster_name: str = field(alias=["-c"])

    def execute(self) -> int:
        cfg = config("scraping")

        # Cache slurm.conf files in folder <sarc-cache>/slurm_conf/<cluster_name>
        cache = Cache(subdirectory=f"slurm_conf/{self.cluster_name}")

        # Now download slurm.conf file for current time
        file_content = _download_slurm_conf_file(cfg.clusters[self.cluster_name])
        now = datetime.now(UTC)

        # And save it into cache
        _save_into_cache(cache, file_content, now)
        return 0


def _save_into_cache(cache: Cache, content: str, date: datetime_utc) -> bool:
    """Save slurm.conf file content into cache at given date."""

    # We won't save content if identical to latest cached content.
    latest_cache_entry = cache.latest_entry()
    if latest_cache_entry is not None:
        ((key, blob),) = latest_cache_entry.items()
        prev_content = blob.decode(encoding="utf-8")
        if content == prev_content:
            logger.info(
                f"slurm.conf file at {date} have not changed since: {datetime.fromisoformat(key)}, skipping."
            )
            return False

    # Otherwise, save it into cache
    with cache.create_entry(date) as cache_entry:
        # key is date in isoformat
        # value is slurm.conf file content
        cache_entry.add_value(date.isoformat(), content.encode("utf-8"))
    return True


def _download_slurm_conf_file(cluster: ClusterConfig) -> str:
    """Download slurm.conf file for given cluster."""
    cmd = f"cat {cluster.slurm_conf_host_path}"
    result = cluster.ssh.run(cmd, hide=True)
    return result.stdout
