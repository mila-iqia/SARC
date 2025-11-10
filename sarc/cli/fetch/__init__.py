from dataclasses import dataclass

from simple_parsing import subparsers

from .users import FetchUsers


@dataclass
class Fetch:
    command: FetchUsers = subparsers(
        {
            "users": FetchUsers,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
