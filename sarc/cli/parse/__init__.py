from dataclasses import dataclass

from simple_parsing import subparsers

from .diskusage import ParseDiskUsage
from .slurmconfig import ParseSlurmConfig
from .users import ParseUsers


@dataclass
class Parse:
    # See https://github.com/python/mypy/issues/20140 for a description of the mypy bug
    command: ParseUsers | ParseDiskUsage = subparsers(  # type: ignore [type-var]
        {
            "users": ParseUsers,
            "diskusage": ParseDiskUsage,
            "slurmconfig": ParseSlurmConfig,
        }
    )

    def execute(self) -> int:
        return self.command.execute()
