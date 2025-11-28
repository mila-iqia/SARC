from dataclasses import dataclass

from simple_parsing import subparsers

from .allocations import ParseAllocations
from .slurmconfig import ParseSlurmConfig
from .users import ParseUsers


@dataclass
class Parse:
    command: ParseAllocations | ParseUsers | ParseSlurmConfig = subparsers(
        {
            "users": ParseUsers,
            "slurmconfig": ParseSlurmConfig,
            "allocations": ParseAllocations,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
