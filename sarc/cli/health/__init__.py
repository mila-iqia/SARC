from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from .check import HealthCheckCommand
from .history import HealthHistoryCommand
from .monitor import HealthMonitorCommand
from .run import HealthRunCommand


@dataclass
class Health:
    command: Union[
        HealthMonitorCommand,
        HealthCheckCommand,
        HealthHistoryCommand,
        HealthRunCommand,
    ] = subparsers(
        {
            "monitor": HealthMonitorCommand,
            "check": HealthCheckCommand,
            "history": HealthHistoryCommand,
            "run": HealthRunCommand,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
