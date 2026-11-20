from dataclasses import dataclass

from simple_parsing import subparsers

from .ack_stats import UsageAckStatsCommand
from .notify import UsageNotifyCommand
from .refresh_store import UsageRefreshStoreCommand


@dataclass
class Usage:
    command: UsageNotifyCommand | UsageRefreshStoreCommand | UsageAckStatsCommand = (
        subparsers(
            {
                "notify": UsageNotifyCommand,
                "refresh-store": UsageRefreshStoreCommand,
                "ack-stats": UsageAckStatsCommand,
            }
        )
    )

    def execute(self) -> int:
        return self.command.execute()
