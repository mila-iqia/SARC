from dataclasses import dataclass

from simple_parsing import subparsers

from sarc.cli.db.backup import DbBackup
from sarc.cli.db.init import DbInit
from sarc.cli.db.restore import DbRestore
from sarc.cli.db.prometheus import DbPrometheus


@dataclass
class Db:
    """this is help"""

    command: DbInit | DbBackup | DbRestore | DbPrometheus = subparsers(
        {
            "init": DbInit,
            "backup": DbBackup,
            "restore": DbRestore,
            "prometheus": DbPrometheus,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
