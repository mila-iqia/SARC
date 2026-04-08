import glob
import logging
import re
import zoneinfo
from datetime import datetime

import tzlocal

from sarc.cache import Cache
from sarc.cli.fetch.slurmconfig import _save_into_cache
from sarc.config import UTC, Config, config
from sarc.core.models.validators import UTCOFFSET, datetime_utc

TZLOCAL = zoneinfo.ZoneInfo(tzlocal.get_localzone_name())

logger = logging.getLogger(__name__)


def main():
    cfg = config("scraping")

    for cluster in cfg.clusters.values():
        cluster_name = cluster.name
        assert cluster_name is not None
        # Cache slurm.conf files in folder <sarc-cache>/slurm_conf/<cluster_name>
        cache = Cache(subdirectory=f"slurm_conf/{cluster_name}")

        # Make sure any legacy cached slurm.conf file is transferred to new cache system
        _fetch_legacy_cache_files(cfg, cluster_name, cache)


def _fetch_legacy_cache_files(cfg: Config, cluster_name: str, cache: Cache):
    """
    Transfer old cached slurm.conf files into new cache system.

    After transfer, old cached file is renamed from `slurm.*.conf` to `.slurm.*.conf`,
    so that it's not parsed anymore if we run this command again.
    """
    assert cfg.cache is not None
    slurm_conf_dir = cfg.cache / "slurm_conf"

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


if __name__ == "__main__":
    main()
