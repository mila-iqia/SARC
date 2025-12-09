from dataclasses import dataclass

from simple_parsing import subparsers

from .diskusage import FetchDiskUsage
from .slurmconfig import FetchSlurmConfig
from .users import FetchUsers


@dataclass
class Fetch:
    command: FetchUsers | FetchDiskUsage | FetchSlurmConfig = subparsers(
        {
            "users": FetchUsers,
            "diskusage": FetchDiskUsage,
            "slurmconfig": FetchSlurmConfig,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
