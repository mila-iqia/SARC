from dataclasses import dataclass

from simple_parsing import subparsers

from .allocations import FetchAllocations
from .diskusage import FetchDiskUsage
from .jobs import FetchJobs
from .slurmconfig import FetchSlurmConfig
from .users import FetchUsers


@dataclass
class Fetch:
    command: FetchUsers | FetchDiskUsage | FetchSlurmConfig | FetchAllocations | FetchJobs = (
        subparsers(
            {
                "users": FetchUsers,
                "diskusage": FetchDiskUsage,
                "slurmconfig": FetchSlurmConfig,
                "allocations": FetchAllocations,
                "jobs": FetchJobs,
            }
        )
    )

    def execute(self) -> int:
        return self.command.execute()
