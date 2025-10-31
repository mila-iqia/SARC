from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from sarc.cli.acquire.allocations import AcquireAllocations
from sarc.cli.acquire.jobs import AcquireJobs
from sarc.cli.acquire.prometheus import AcquirePrometheus
from sarc.cli.acquire.slurmconfig import AcquireSlurmConfig


@dataclass
class Acquire:
    command: Union[
        AcquireAllocations, AcquireJobs, AcquireSlurmConfig, AcquirePrometheus
    ] = subparsers(
        {
            "allocations": AcquireAllocations,
            "jobs": AcquireJobs,
            "slurmconfig": AcquireSlurmConfig,
            "prometheus": AcquirePrometheus,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
