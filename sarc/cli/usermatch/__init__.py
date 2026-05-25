from dataclasses import dataclass

from simple_parsing import subparsers

from .apply import UsermatchApply


@dataclass
class Usermatch:
    command: UsermatchApply = subparsers(
        {"apply": UsermatchApply}  # ty:ignore[invalid-argument-type]
    )

    def execute(self) -> int:
        return self.command.execute()
