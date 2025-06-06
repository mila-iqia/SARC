from dataclasses import dataclass

from simple_parsing import subparsers

from sarc.cli.db.init import DbInit


@dataclass
class Db:
    """this is help"""

    command: DbInit = subparsers(
        {
            "init": DbInit,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
