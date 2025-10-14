from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from .users import FetchUsers


@dataclass
class Acquire:
    command: Union[FetchUsers] = subparsers(
        {
            "users": FetchUsers,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
