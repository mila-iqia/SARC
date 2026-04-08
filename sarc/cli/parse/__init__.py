from dataclasses import dataclass

from simple_parsing import subparsers

from .allocations import ParseAllocations
from .diskusage import ParseDiskUsage
from .jobs import ParseJobs
from .prometheus import ParsePrometheus
from .slurmconfig import ParseSlurmConfig
from .users import ParseUsers


@dataclass
class Parse:
    command: (
        ParseUsers
        | ParseDiskUsage
        | ParseSlurmConfig
        | ParseAllocations
        | ParseJobs
        | ParsePrometheus
    ) = subparsers(
        {
            "users": ParseUsers,
            "diskusage": ParseDiskUsage,
            "slurmconfig": ParseSlurmConfig,
            "allocations": ParseAllocations,
            "jobs": ParseJobs,
            "prometheus": ParsePrometheus,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
