from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from .check import HealthCheck
from .monitor import HealthMonitor


@dataclass
class Health:
    command: Union[HealthMonitor, HealthCheck] = subparsers(
        {
            "monitor": HealthMonitor,
            "check": HealthCheck,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
