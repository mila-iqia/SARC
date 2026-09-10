from dataclasses import dataclass

from simple_parsing import subparsers

from .backfill_series import BackfillSeriesCommand


@dataclass
class Db:
    command: BackfillSeriesCommand = subparsers(
        {"backfill-series": BackfillSeriesCommand}
    )

    def execute(self) -> int:
        return self.command.execute()
