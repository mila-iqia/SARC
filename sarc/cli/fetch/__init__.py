from dataclasses import dataclass

from simple_parsing import subparsers

from .slurmconfig import FetchSlurmConfig
from .users import FetchUsers


@dataclass
class Fetch:
    command: FetchUsers | FetchSlurmConfig = subparsers(
        {
            "users": FetchUsers,
            "slurmconfig": FetchSlurmConfig,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
