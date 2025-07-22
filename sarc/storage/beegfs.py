"""
Fetching and parsing code specific to the mila cluster
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from fabric import Connection
from pydantic import ByteSize

from sarc.client.users.api import User, get_users
from sarc.core.models.diskusage import DiskUsage, DiskUsageGroup, DiskUsageUser
from sarc.core.scraping.diskusage import DiskUsageScraper, _builtin_scrapers
from sarc.core.utils import run_command

beegfs_header = "name,id,size,hard,files,hard"


@dataclass
class BeeGFSDiskUsageConfig:
    config_files: dict[str, str]
    retries: int = 3
    beegfs_ctl_path: str = "beegfs-ctl"


class BeeGFSDiskUsage(DiskUsageScraper[BeeGFSDiskUsageConfig]):
    config_type = BeeGFSDiskUsageConfig

    def get_diskusage_report(
        self, ssh: Connection, config: BeeGFSDiskUsageConfig
    ) -> str:
        users = get_users()
        assert len(users) > 0

        errors: list[Exception] = []
        failures: list[User] = []
        output: dict[str, list[str]] = {}

        for name, file in config.config_files.items():
            usage: list[str] = []

            for user in users:
                if not user.mila.active:
                    continue

                cmd = f"{config.beegfs_ctl_path} --cfgFile={file} --getquota --uid {user.mila.username} --csv"
                result, err = run_command(ssh, cmd, config.retries)

                if err:
                    errors.extend(err)

                if result is None:
                    failures.append(user)
                else:
                    usage.append(_trim_beegfs_output(result))

            output[name] = usage

        return json.dumps(output)

    def parse_diskusage_report(
        self, config: BeeGFSDiskUsageConfig, cluster_name: str, data_str: str
    ) -> DiskUsage:
        groups: list[DiskUsageGroup] = []
        data: dict[str, str] = json.loads(data_str)

        for name in config.config_files.keys():
            groups.append(
                DiskUsageGroup(
                    group_name=name, users=[_parse_line(line) for line in data[name]]
                )
            )

        return DiskUsage(
            cluster_name=cluster_name,
            groups=groups,
            timestamp=datetime.now(UTC),
        )


# Register the scraper to make it available
_builtin_scrapers["beegfs"] = BeeGFSDiskUsage()


def _trim_beegfs_output(output: str) -> str:
    collect = []
    started = False

    for line in output.splitlines():
        if beegfs_header in line:
            started = True
            continue
        if started:
            collect.append(line)

    return "\n".join(collect)


def _parse_line(line: str) -> DiskUsageUser:
    name, _, size, _, files, _ = line.split(",")
    return DiskUsageUser(user=name, nbr_files=int(files), size=cast(ByteSize, size))
