import logging
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from filelock import SoftFileLock, Timeout
from simple_parsing import field

from sarc.config import config

logger = logging.getLogger(__name__)


def _get_current_timestring() -> str:
    """Return a string for current time."""
    fmt = "%Y-%m-%dT%Hh%Mm%Ss%z"
    return datetime.now().strftime(fmt)


@dataclass
class DbBackup:
    """Backup a Mongo database into a directory."""

    output: str = field(
        alias=["-o"],
        required=False,
        help="Backup directory. Default: a new timestamp-named subfolder: <sarc-cache>/backup/<timestamp>",
    )

    def execute(self) -> int:
        cfg = config()
        try:
            # Use a filelock to prevent concurrent DB backup/restore/writing operations
            # Module filelock: https://py-filelock.readthedocs.io/en/latest/
            # We use SoftFileLock instead of FileLock
            # to make sure lock file is ultimately deleted
            # (not yet guaranteed with FileLock).
            # With timeout=0, script will immediately terminate if file is already
            # locked by another process.
            with SoftFileLock(cfg.lock_path, timeout=0):
                if shutil.which("mongodump") is None:
                    logger.error(
                        "Cannot find executable mongodump in environment paths."
                    )
                    return -1

                if self.output:
                    backup_path = Path(self.output)
                else:
                    backup_path = cfg.cache / "backup" / _get_current_timestring()
                backup_path = backup_path.resolve()

                command = [
                    "mongodump",
                    "--gzip",
                    f"--uri={cfg.mongo.connection_string}",
                    f"--db={cfg.mongo.database_name}",
                    f"--out={backup_path}",
                ]

                logger.info(f"Backup folder: {backup_path}")
                logger.info(f"Backup command: {' '.join(command)}")

                result = subprocess.run(command, capture_output=False, check=False)
                if result.returncode == 0:
                    logger.info(f"Database successfully saved in: {backup_path}")
                else:
                    logger.error(
                        f"Database backup failed with exit code {result.returncode}. "
                        f"You might want to delete backup folder: {backup_path}"
                    )
                return result.returncode
        except Timeout:
            logger.error(
                "A database operation is already occurring in another process."
            )
            return -1
