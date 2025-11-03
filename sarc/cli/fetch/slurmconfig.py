from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import IO, Any

from simple_parsing import field

from sarc.cache import with_cache, FormatterProto, CachedFunction
from sarc.config import config, ClusterConfig, UTC, TZLOCAL
from sarc.core.models.validators import datetime_utc
from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)


@dataclass
class FetchSlurmConfig:
    """Download slurm.conf file for given cluster at current time."""

    cluster_name: str = field(alias=["-c"])

    def execute(self) -> int:
        SlurmConfigDownloader(
            cluster=config("scraping").clusters[self.cluster_name],
            date=datetime.now(tz=TZLOCAL).astimezone(UTC),
        ).get_slurm_config()
        return 0


class FileContent(FormatterProto[str]):
    """
    Formatter for slurm conf file cache.
    Just read and write entire text content from file.
    """

    read_flags = "r"
    write_flags = "w"

    @staticmethod
    def load(fp: IO[Any]) -> str:
        return fp.read()

    @staticmethod
    def dump(obj: str, fp: IO[Any]):
        fp.write(obj)


class SlurmConfigDownloader(BaseModel):
    model_config = ConfigDict(ignored_types=(CachedFunction,))

    cluster: ClusterConfig
    date: datetime_utc

    def _cache_key(self) -> str:
        # Use full date in ISO format
        return f"slurm.{self.cluster.name}.{self.date.isoformat()}.conf"

    @with_cache(subdirectory="slurm_conf", key=_cache_key, formatter=FileContent)
    def get_slurm_config(self) -> str:
        cmd = f"cat {self.cluster.slurm_conf_host_path}"
        result = self.cluster.ssh.run(cmd, hide=True)
        return result.stdout
