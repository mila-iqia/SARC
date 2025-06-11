from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from sarc.cli.db.backup import DbBackup
from sarc.cli.db.init import DbInit
from sarc.cli.db.restore import DbRestore


@dataclass
class Db:
    """this is help"""

    command: DbInit | DbBackup | DbRestore = subparsers(
        {
            "init": DbInit,
            "backup": DbBackup,
            "restore": DbRestore,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
