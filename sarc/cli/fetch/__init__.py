from dataclasses import dataclass

from simple_parsing import subparsers

from .allocations import FetchAllocations
from .diskusage import FetchDiskUsage
from .slurmconfig import FetchSlurmConfig
from .users import FetchUsers


@dataclass
class Fetch:
    command: FetchUsers | FetchDiskUsage | FetchSlurmConfig | FetchAllocations = (
        subparsers(
            {
                "users": FetchUsers,
                "diskusage": FetchDiskUsage,
                "slurmconfig": FetchSlurmConfig,
                "allocations": FetchAllocations,
            }
        )
    )

    def execute(self) -> int:
        return self.command.execute()
