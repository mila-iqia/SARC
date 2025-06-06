from dataclasses import dataclass

from simple_parsing import subparsers

from .check import HealthCheckCommand
from .history import HealthHistoryCommand
from .monitor import HealthMonitorCommand


@dataclass
class Health:
    command: HealthMonitorCommand | HealthCheckCommand | HealthHistoryCommand = (
        subparsers(
            {
                "monitor": HealthMonitorCommand,
                "check": HealthCheckCommand,
                "history": HealthHistoryCommand,
            }
        )
    )

    def execute(self) -> int:
        return self.command.execute()
