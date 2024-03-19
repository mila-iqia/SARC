from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from .check import HealthCheckCommand
from .monitor import HealthMonitorCommand


@dataclass
class Health:
    command: Union[HealthMonitorCommand, HealthCheckCommand] = subparsers(
        {
            "monitor": HealthMonitorCommand,
            "check": HealthCheckCommand,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
