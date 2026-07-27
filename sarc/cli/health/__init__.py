from dataclasses import dataclass

from simple_parsing import subparsers

from .list import HealthListCommand
from .run import HealthRunCommand


@dataclass
class Health:
    command: HealthRunCommand | HealthListCommand = subparsers(
        {"run": HealthRunCommand, "list": HealthListCommand}
    )

    def execute(self) -> int:
        return self.command.execute()
