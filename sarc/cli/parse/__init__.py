from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from .users import ParseUsers


@dataclass
class Parse:
    command: Union[ParseUsers] = subparsers(
        {
            "users": ParseUsers,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
