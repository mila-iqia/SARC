from dataclasses import dataclass

from simple_parsing import subparsers

from .slurmconfig import FetchSlurmConfig
from .users import FetchUsers
from .jobs import FetchJobs


@dataclass
class Fetch:
    command: FetchJobs | FetchUsers | FetchSlurmConfig = subparsers(
        {
            "jobs": FetchJobs,
            "users": FetchUsers,
            "slurmconfig": FetchSlurmConfig,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
