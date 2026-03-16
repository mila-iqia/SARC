from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from .list import HealthListCommand
from .run import HealthRunCommand


@dataclass
class Health:
    command: Union[HealthRunCommand, HealthListCommand] = subparsers(
        {"run": HealthRunCommand, "list": HealthListCommand}
    )

    def execute(self) -> int:
        return self.command.execute()
