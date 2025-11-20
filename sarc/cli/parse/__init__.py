from dataclasses import dataclass

from simple_parsing import subparsers

from .users import ParseUsers


@dataclass
class Parse:
    command: ParseUsers = subparsers(
        {
            "users": ParseUsers,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
