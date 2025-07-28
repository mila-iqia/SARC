"""
Fetching and parsing code specific to DRAC clusters
"""

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from fabric import Connection
from pydantic import ByteSize

from sarc.core.models.diskusage import DiskUsage, DiskUsageGroup, DiskUsageUser
from sarc.core.scraping.diskusage import DiskUsageScraper, _builtin_scrapers
from sarc.core.utils import run_command

logger = logging.getLogger(__name__)


@dataclass
class DRACDiskUsageConfig:
    diskusage_path: str = "diskusage_report"


class DRACDiskUsage(DiskUsageScraper[DRACDiskUsageConfig]):
    config_type = DRACDiskUsageConfig

    def get_diskusage_report(self, ssh: Connection, config: DRACDiskUsageConfig) -> str:
        """
        Get the output of the command diskusage_report --project --all_users on the wanted cluster

        The output is something like this:

                                 Description                Space           # of files
           /project (project rrg-bengioy-ad)              39T/75T          1226k/5000k
              /project (project def-bengioy)           956G/1000G            226k/500k

        Breakdown for project rrg-bengioy-ad (Last update: 2023-02-27 23:04:29)
                   User      File count                 Size             Location
        -------------------------------------------------------------------------
                 user01               2             0.00 GiB              On disk
                 user02           14212           223.99 GiB              On disk
        (...)
                 user99               4           819.78 GiB              On disk
                  Total          381818         36804.29 GiB              On disk


        Breakdown for project def-bengioy (Last update: 2023-02-27 23:00:57)
                   User      File count                 Size             Location
        -------------------------------------------------------------------------
                 user01               2             0.00 GiB              On disk
                 user02           14212           223.99 GiB              On disk
        (...)
                 user99               4           819.78 GiB              On disk
                  Total          381818         36804.29 GiB              On disk


        Disk usage can be explored using the following commands:
        diskusage_explorer /project/rrg-bengioy-ad 	 (Last update: 2023-02-27 20:06:27)
        diskusage_explorer /project/def-bengioy 	 (Last update: 2023-02-27 19:59:41)
        """
        cmd = f"{config.diskusage_path} --project --all_users"
        output, errors = run_command(ssh, cmd, 1)
        if output is None:
            logger.warning("Could not fetch diskusage report", exc_info=errors[0])
            return ""
        return output

    def parse_diskusage_report(
        self,
        config: DRACDiskUsageConfig,  # noqa: ARG002
        cluster_name: str,
        data: str,
    ) -> DiskUsage:
        report = data.split("\n")
        groups = _parse_body(report)

        return DiskUsage(
            cluster_name=cluster_name, groups=groups, timestamp=datetime.now(UTC)
        )


# Register the scraper to make it available
_builtin_scrapers["drac"] = DRACDiskUsage()


def _parse_body(L_lines: list[str]) -> list[DiskUsageGroup]:
    """
    Breakdown for project def-bengioy (Last update: 2022-10-25 14:01:28)
            User      File count                 Size             Location
    -------------------------------------------------------------------------
       kfsdfsdf               2             0.00 GiB              On disk
       k000f0ds               2             0.00 GiB              On disk
         kdf900              50            13.49 GiB              On disk
         k349ff               2             0.00 GiB              On disk
          Total          696928           877.51 GiB              On disk
    """

    output: list[DiskUsageGroup] = []

    project: str | None = None
    LD_results: list[DiskUsageUser] = []
    inside_segment = False
    for line in L_lines:
        if not inside_segment and re.match(
            r"^\s*$", line
        ):  # skip empty line when outside of segment
            continue
        if m := re.match(r"^\s*Breakdown\sfor\sproject\s(.+?)\s.*$", line):
            inside_segment = True
            project = m.group(1)
            continue
        if re.match(r"^\s*\-+\s*$", line):  # line with only -----
            continue
        if re.match(
            r"^\s*User\s*File\scount\s*Size\s*Location\s*$", line
        ):  # line with column names
            continue
        if inside_segment and re.match(r"^\s*$", line):  # empty line marks the end
            # accumulate into the dict to return before recursive call
            assert project
            assert LD_results
            output.append(DiskUsageGroup(group_name=project, users=LD_results))
            project = None
            LD_results = []
            inside_segment = False
            continue
        if inside_segment:
            # omitting the "On Disk" part of the line
            m = re.match(r"^\s*([\w\.]+)\s+(\d+)\s+([\d\.]+)\s(\w+)\s*", line)
            assert m, f"If this line doesn't match, we've got a problem.\n{line}"
            username = m.group(1)

            # Skip the "Total" line as it's a summary, not a user
            if username == "Total":
                continue

            nbr_files = int(m.group(2))
            size = f"{m.group(3)} {m.group(4)}"
            LD_results.append(
                DiskUsageUser(
                    user=username, nbr_files=nbr_files, size=cast(ByteSize, size)
                )
            )

    return output
