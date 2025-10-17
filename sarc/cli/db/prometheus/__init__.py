from dataclasses import dataclass

from simple_parsing import subparsers

from sarc.cli.db.prometheus.backup import DbPrometheusBackup
from sarc.cli.db.prometheus.restore import DbPrometheusRestore


@dataclass
class DbPrometheus:
    """this is help"""

    command: DbPrometheusBackup | DbPrometheusRestore = subparsers(
        {
            "backup": DbPrometheusBackup,
            "restore": DbPrometheusRestore,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
