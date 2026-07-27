from dataclasses import dataclass

from simple_parsing import subparsers

from .underusage import UnderusageNotifyCommand


@dataclass
class Notify:
    command: UnderusageNotifyCommand = subparsers(
        {"underusage": UnderusageNotifyCommand}
    )

    def execute(self) -> int:
        return self.command.execute()
