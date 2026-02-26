from dataclasses import dataclass

from simple_parsing import subparsers
from sarc.cli.acquire.prometheus import AcquirePrometheus


@dataclass
class Acquire:
    command: AcquirePrometheus = subparsers(  # type: ignore[type-var]
        {
            "prometheus": AcquirePrometheus,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
