from dataclasses import dataclass

from simple_parsing import subparsers

from .notify import UsageNotifyCommand
from .refresh_store import UsageRefreshStoreCommand


@dataclass
class Usage:
    command: UsageNotifyCommand | UsageRefreshStoreCommand = subparsers(
        {"notify": UsageNotifyCommand, "refresh-store": UsageRefreshStoreCommand}
    )

    def execute(self) -> int:
        return self.command.execute()
