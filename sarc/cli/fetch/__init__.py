from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from .diskusage import FetchDiskUsage
from .users import FetchUsers


@dataclass
class Fetch:
    command: Union[FetchUsers, FetchDiskUsage] = subparsers(
        {
            "users": FetchUsers,
            "diskusage": FetchDiskUsage,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
