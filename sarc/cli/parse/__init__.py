from dataclasses import dataclass

from simple_parsing import subparsers

from .slurmconfig import ParseSlurmConfig
from .jobs import ParseJobs
from .users import ParseUsers


@dataclass
class Parse:
    command: ParseUsers | ParseSlurmConfig | ParseJobs = subparsers(
        {"users": ParseUsers, "slurmconfig": ParseSlurmConfig, "jobs": ParseJobs}
    )

    def execute(self) -> int:
        return self.command.execute()
