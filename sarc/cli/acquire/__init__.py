from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from sarc.cli.acquire.allocations import AcquireAllocations
from sarc.cli.acquire.prometheus import AcquirePrometheus
from sarc.cli.acquire.storages import AcquireStorages


@dataclass
class Acquire:
    command: Union[AcquireAllocations, AcquireStorages] = subparsers(
        {
            "allocations": AcquireAllocations,
            "storages": AcquireStorages,
            "prometheus": AcquirePrometheus,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
