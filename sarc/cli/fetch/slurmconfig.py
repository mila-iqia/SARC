import glob
import logging
import re
from dataclasses import dataclass
from datetime import datetime

from simple_parsing import field

from sarc.cache import Cache
from sarc.config import config, ClusterConfig, UTC, Config, TZLOCAL
from sarc.core.models.validators import datetime_utc, UTCOFFSET

logger = logging.getLogger(__name__)


_CACHE_SUBDIRECTORY = "slurm_conf"


@dataclass
class FetchSlurmConfig:
    """Download slurm.conf file for given cluster at current time."""

    cluster_name: str = field(alias=["-c"])

    def execute(self) -> int:
        cfg = config("scraping")

        # Cache slurm.conf files in folder <sarc-cache>/slurm_conf/<cluster_name>
        cache = Cache(subdirectory=f"{_CACHE_SUBDIRECTORY}/{self.cluster_name}")

        # Make sure any legacy cached slurm.conf file is transferred to new cache system
        _fetch_legacy_cache_files(cfg, self.cluster_name, cache)

        # Now download slurm.conf file for current time
        file_content = _download_slurm_conf_file(cfg.clusters[self.cluster_name])
        now = datetime.now(UTC)

        # And save it into cache
        _save_into_cache(cache, file_content, now)
        return 0


def _fetch_legacy_cache_files(cfg: Config, cluster_name: str, cache: Cache):
    """
    Transfer old cached slurm.conf files into new cache system.

    After transfer, old cached file is renamed from `slurm.*.conf` to `.slurm.*.conf`,
    so that it's not parsed anymore if we run this command again.
    """
    assert cfg.cache is not None
    slurm_conf_dir = cfg.cache / _CACHE_SUBDIRECTORY

    prefix = f"slurm.{cluster_name}."
    suffix = ".conf"
    regex_day = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
    date_and_basename: list[tuple[datetime_utc, str]] = []
    for basename in glob.glob(f"slurm.{cluster_name}.*.conf", root_dir=slurm_conf_dir):
        assert basename.startswith(prefix)
        assert basename.endswith(suffix)
        date_string = basename[len(prefix) : -len(suffix)]
        if regex_day.match(date_string):
            cache_date = (
                datetime.strptime(date_string, "%Y-%m-%d")
                .replace(tzinfo=TZLOCAL)
                .astimezone(UTC)
            )
        else:
            cache_date = datetime.fromisoformat(date_string)
        assert cache_date.tzinfo is not None, "date is not tz-aware"
        assert cache_date.utcoffset() == UTCOFFSET, "date is not in UTC timezone"
        date_and_basename.append((cache_date, basename))

    for cache_date, basename in sorted(date_and_basename):
        logger.info(f"Legacy cache at {cache_date}: {basename}")
        file_path = slurm_conf_dir / basename
        # Read legacy cache file
        with open(file_path, mode="r", encoding="utf-8") as file:
            file_content = file.read()
        # Save legacy cache into new cache system
        _save_into_cache(cache, file_content, cache_date)
        # We can now try to deactivate legacy cache file
        deactivated_path = file_path.parent / f".{file_path.parts[-1]}"
        file_path.rename(deactivated_path)


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
        # value if slurm.conf file content
        cache_entry.add_value(date.isoformat(), content.encode("utf-8"))
    return True


def _download_slurm_conf_file(cluster: ClusterConfig) -> str:
    """Download slurm.conf file for given cluster."""
    cmd = f"cat {cluster.slurm_conf_host_path}"
    result = cluster.ssh.run(cmd, hide=True)
    return result.stdout
