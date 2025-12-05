from dataclasses import dataclass

from simple_parsing import subparsers

from .allocations import FetchAllocations
from .slurmconfig import FetchSlurmConfig
from .users import FetchUsers


@dataclass
class Fetch:
    command: FetchUsers | FetchSlurmConfig | FetchAllocations = subparsers(
        {
            "users": FetchUsers,
            "slurmconfig": FetchSlurmConfig,
            "allocations": FetchAllocations,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
