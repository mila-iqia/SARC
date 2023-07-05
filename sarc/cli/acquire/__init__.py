from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from sarc.cli.acquire.allocations import AcquireAllocations
from sarc.cli.acquire.jobs import AcquireJobs
from sarc.cli.acquire.storages import AcquireStorages
from sarc.cli.acquire.ldap import AcquireLDAP


@dataclass
class Acquire:
    command: Union[AcquireAllocations, AcquireJobs, AcquireStorages] = subparsers(
        {
            "allocations": AcquireAllocations,
            "jobs": AcquireJobs,
            "storages": AcquireStorages,
            "ldap": AcquireLDAP,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
