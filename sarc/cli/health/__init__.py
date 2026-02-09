from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from .check import HealthCheckCommand
from .history import HealthHistoryCommand
from .monitor import HealthMonitorCommand
from .poll import HealthPollCommand


@dataclass
class Health:
    command: Union[
        HealthMonitorCommand,
        HealthCheckCommand,
        HealthHistoryCommand,
        HealthPollCommand,
    ] = subparsers(
        {
            "monitor": HealthMonitorCommand,
            "check": HealthCheckCommand,
            "history": HealthHistoryCommand,
            "poll": HealthPollCommand,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
