from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from .diskusage import ParseDiskUsage
from .users import ParseUsers


@dataclass
class Parse:
    command: Union[ParseUsers, ParseDiskUsage] = subparsers(
        {
            "users": ParseUsers,
            "diskusage": ParseDiskUsage,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
