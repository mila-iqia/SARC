from dataclasses import dataclass

from simple_parsing import subparsers

from .usage import UsageNotifyCommand


@dataclass
class Notify:
    command: UsageNotifyCommand = subparsers({"usage": UsageNotifyCommand})

    def execute(self) -> int:
        return self.command.execute()
