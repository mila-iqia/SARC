from dataclasses import dataclass

from simple_parsing import subparsers

from .slurmconfig import ParseSlurmConfig
from .users import ParseUsers


@dataclass
class Parse:
    command: ParseUsers | ParseSlurmConfig = subparsers(
        {
            "users": ParseUsers,
            "slurmconfig": ParseSlurmConfig,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
