from dataclasses import dataclass

from simple_parsing import subparsers

from .allocations import ParseAllocations
from .diskusage import ParseDiskUsage
from .slurmconfig import ParseSlurmConfig
from .jobs import ParseJobs
from .users import ParseUsers


@dataclass
class Parse:
    command: ParseUsers | ParseDiskUsage | ParseSlurmConfig | ParseAllocations | ParseJobs = (
        subparsers(
            {
                "users": ParseUsers,
                "diskusage": ParseDiskUsage,
                "slurmconfig": ParseSlurmConfig,
                "allocations": ParseAllocations,
                "jobs": ParseJobs,
            }
        )
    )

    def execute(self) -> int:
        return self.command.execute()
