from dataclasses import dataclass

from simple_parsing import subparsers

from .diskusage import FetchDiskUsage
from .users import FetchUsers


@dataclass
class Fetch:
    command: FetchUsers | FetchDiskUsage = subparsers(
        {
            "users": FetchUsers,
            "diskusage": FetchDiskUsage,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
