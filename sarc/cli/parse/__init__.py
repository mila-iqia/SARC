from dataclasses import dataclass

from simple_parsing import subparsers

from .allocations import ParseAllocations
from .diskusage import ParseDiskUsage
from .slurmconfig import ParseSlurmConfig
from .users import ParseUsers


@dataclass
class Parse:
    command: ParseUsers | ParseDiskUsage | ParseSlurmConfig | ParseAllocations = (
        subparsers(
            {
                "users": ParseUsers,
                "diskusage": ParseDiskUsage,
                "slurmconfig": ParseSlurmConfig,
                "allocations": ParseAllocations,
            }
        )
    )

    def execute(self) -> int:
        return self.command.execute()
