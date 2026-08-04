from dataclasses import dataclass

from simple_parsing import subparsers

from .refresh_store import UsageRefreshStoreCommand


@dataclass
class Usage:
    command: UsageRefreshStoreCommand = subparsers(
        {"refresh-store": UsageRefreshStoreCommand}
    )

    def execute(self) -> int:
        return self.command.execute()
