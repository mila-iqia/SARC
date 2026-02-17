from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from .run import HealthRunCommand


@dataclass
class Health:
    command: Union[HealthRunCommand] = subparsers({"run": HealthRunCommand})

    def execute(self) -> int:
        return self.command.execute()
