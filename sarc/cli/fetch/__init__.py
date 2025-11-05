from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from .users import FetchUsers
from .jobs import FetchJobs


@dataclass
class Fetch:
    command: Union[FetchUsers] = subparsers(
        {
            "jobs": FetchJobs,
            "users": FetchUsers,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
