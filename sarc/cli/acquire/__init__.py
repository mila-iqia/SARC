from dataclasses import dataclass
from typing import Union

from simple_parsing import subparsers

from sarc.cli.acquire.jobs import AcquireJobs
from sarc.cli.acquire.prometheus import AcquirePrometheus


@dataclass
class Acquire:
    command: Union[AcquireJobs, AcquirePrometheus] = subparsers(  # type: ignore[type-var]
        {
            "jobs": AcquireJobs,
            "prometheus": AcquirePrometheus,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
