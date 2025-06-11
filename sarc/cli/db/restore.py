import logging
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from filelock import SoftFileLock, Timeout
from simple_parsing import field

from sarc.config import config

logger = logging.getLogger(__name__)


@dataclass
class DbRestore:
    """Restore a Mongo database from a directory."""

    input: str = field(alias=["-i"], help="Backup directory to load")
    force: bool = field(
        alias=["-f"],
        action="store_true",
        help="If True, delete previous collections from target database before restoring.",
    )

    def execute(self) -> int:
        cfg = config()
        try:
            with SoftFileLock(cfg.lock_path, timeout=0):
                if shutil.which("mongorestore") is None:
                    logger.error(
                        "Cannot find executable mongorestore in environment paths."
                    )
                    return -1

                backup_path = Path(self.input).resolve()
                if not backup_path.is_dir():
                    logger.error(f"Backup path is not a directory: {backup_path}")
                    return -1

                # Get database subdirectory from backup path
                inp_db_dirs = list(backup_path.iterdir())
                if len(inp_db_dirs) != 1:
                    logger.error(
                        f"Expected to find only 1 sub-folder in backup directory, instead found {len(inp_db_dirs)}"
                    )
                    return -1
                (mongorestore_dir,) = inp_db_dirs
                # Get input database name
                inp_db_name = mongorestore_dir.name

                command = [
                    "mongorestore",
                    f"--uri={cfg.mongo.connection_string}",
                    # We must specify path to input database itself, not backup path
                    f"--dir={mongorestore_dir}",
                    # Make sure to restore in target database whose name is specified in config file
                    f'--nsInclude="{inp_db_name}.*"',
                    f'--nsFrom="{inp_db_name}.*"',
                    f'--nsTo="{cfg.mongo.database_name}.*"',
                    # Notify mongorestore that input data are compressed
                    "--gzip",
                ]
                if self.force:
                    # Drop previous collections in target database if --force is specified
                    command.append("--drop")

                logger.info(f"Restore input: {backup_path}")
                logger.info(f"Restore command: {' '.join(command)}")

                result = subprocess.run(command, capture_output=False, check=False)
                if result.returncode == 0:
                    logger.info(f"Database successfully restored from: {backup_path}")
                else:
                    logger.error(
                        f"Database restore failed with exit code {result.returncode}"
                    )
                return result.returncode
        except Timeout:
            logger.error(
                "A database operation is already occurring in another process."
            )
            return -1
