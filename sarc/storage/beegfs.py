"""
Fetching and parsing code specific to the mila cluster
"""

import csv
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO
from typing import cast

from fabric import Connection
from pydantic import ByteSize

from sarc.core.models.diskusage import DiskUsage, DiskUsageGroup, DiskUsageUser
from sarc.core.models.validators import DateMatchError
from sarc.core.scraping.diskusage import DiskUsageScraper, _builtin_scrapers
from sarc.core.utils import run_command
from sarc.users.db import get_users

logger = logging.getLogger(__name__)
beegfs_header = "name,id,size,hard,files,hard"


@dataclass
class BeeGFSDiskUsageConfig:
    config_files: dict[str, str]
    retries: int = 3
    beegfs_ctl_path: str = "beegfs-ctl"


class BeeGFSDiskUsage(DiskUsageScraper[BeeGFSDiskUsageConfig]):
    config_type = BeeGFSDiskUsageConfig

    def get_diskusage_report(
        self, ssh: Connection, cluster_name: str, config: BeeGFSDiskUsageConfig
    ) -> bytes:
        users = get_users()
        output: dict[str, list[str]] = {}

        for name, file in config.config_files.items():
            usage: list[str] = []

            for user in users:
                if "mila" not in user.associated_accounts:
                    continue

                creds = user.associated_accounts["mila"]
                try:
                    username = creds.get_value()
                except DateMatchError:
                    continue

                cmd = f"{config.beegfs_ctl_path} --cfgFile={file} --getquota --uid {username} --csv"
                result, errs = run_command(ssh, cmd, config.retries)

                for err in errs:
                    logger.exception("Error running BeeGFS command", exc_info=err)

                if result is None:
                    logger.error(
                        "Failed to get disk usage data for user %s(%s)",
                        username,
                        user.uuid,
                    )
                else:
                    usage.append(result)

            output[name] = usage

        return json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
                "cluster_name": cluster_name,
                "output": output,
            }
        ).encode()

    def parse_diskusage_report(self, data_raw: bytes) -> DiskUsage:
        groups: list[DiskUsageGroup] = []

        cached_data: dict = json.loads(data_raw.decode())
        timestamp = datetime.fromisoformat(cached_data["timestamp"])
        cluster_name = cached_data["cluster_name"]
        data = cached_data["output"]

        groups = [
            DiskUsageGroup(
                group_name=name,
                users=[
                    _parse_line(line)
                    for line in csv.reader(
                        StringIO(
                            "\n".join([_trim_beegfs_output(line) for line in lines])
                        )
                    )
                ],
            )
            for name, lines in data.items()
        ]

        return DiskUsage(
            cluster_name=cluster_name,
            groups=groups,
            timestamp=timestamp,
        )


# Register the scraper to make it available
_builtin_scrapers["beegfs"] = BeeGFSDiskUsage()


def _trim_beegfs_output(output: str) -> str:
    splitted = output.splitlines()
    header_index = splitted.index(beegfs_header)
    return "\n".join(splitted[header_index + 1 :])


def _parse_line(line: list[str]) -> DiskUsageUser:
    columns = {
        key.strip(): value.strip() for key, value in zip(beegfs_header.split(","), line)
    }
    return DiskUsageUser(
        user=columns["name"],
        nbr_files=int(columns["files"]),
        size=cast(ByteSize, columns["size"]),
    )
