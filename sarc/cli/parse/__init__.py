from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from .jobs import ParseJobs
from .users import ParseUsers


@dataclass
class Parse:
    command: Union[ParseUsers] = subparsers(
        {
            "users": ParseUsers,
            "jobs": ParseJobs
        }
    )

    def execute(self) -> int:
        return self.command.execute()
