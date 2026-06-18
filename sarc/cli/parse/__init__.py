from dataclasses import dataclass

from simple_parsing import subparsers
from sqlmodel import Session

from sarc.config import config
from sarc.patch import declare_patch

from .allocations import ParseAllocations
from .diskusage import ParseDiskUsage
from .jobs import ParseJobs
from .prometheus import ParsePrometheus
from .slurmconfig import ParseSlurmConfig
from .users import ParseUsers


@declare_patch
def patch_db(sess: Session) -> None:
    pass


@dataclass
class Parse:
    command: (
        ParseUsers
        | ParseDiskUsage
        | ParseSlurmConfig
        | ParseAllocations
        | ParseJobs
        | ParsePrometheus
    ) = subparsers(
        {
            "users": ParseUsers,
            "diskusage": ParseDiskUsage,
            "slurmconfig": ParseSlurmConfig,
            "allocations": ParseAllocations,
            "jobs": ParseJobs,
            "prometheus": ParsePrometheus,
        }  # ty:ignore[invalid-argument-type]
    )

    def execute(self) -> int:
        with config.db.session() as sess:
            patch_db(sess)
            sess.commit()
        return self.command.execute()
