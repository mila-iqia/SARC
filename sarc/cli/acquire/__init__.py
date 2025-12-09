from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from sarc.cli.acquire.allocations import AcquireAllocations
from sarc.cli.acquire.jobs import AcquireJobs
from sarc.cli.acquire.prometheus import AcquirePrometheus


@dataclass
class Acquire:
    command: Union[AcquireAllocations, AcquireJobs, AcquirePrometheus] = subparsers(
        {
            "allocations": AcquireAllocations,
            "jobs": AcquireJobs,
            "prometheus": AcquirePrometheus,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
