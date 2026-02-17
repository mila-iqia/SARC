import logging
import math
import os
from dataclasses import dataclass
from pathlib import Path

import psutil

from sarc.config import config

logger = logging.getLogger(__name__)


def _get_human_readable_file_size(size_bytes: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    if size_bytes == 0:
        human_size = 0
        unit = units[0]
    else:
        power = 1024
        p = min(math.floor(math.log(size_bytes, power)), len(units) - 1)
        human_size = size_bytes / (1024**p)
        unit = units[p]
    output = f"{human_size:.2f} {unit}"
    if unit != units[0]:
        output += f" ({size_bytes} B)"
    return output


def _get_physical_size(path: Path | str) -> int | None:
    path = str(path) if isinstance(path, Path) else path
    if not os.path.exists(path):
        return 0
    try:
        if os.path.isfile(path):
            return os.stat(path).st_size
        total = 0
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    try:
                        total += os.stat(fp).st_size
                    except FileNotFoundError:
                        # File may have been deleted since call to os.walk
                        pass
        return total
    except PermissionError:
        return None


def _find_mongodb_path_via_connection(client) -> str | None:
    # Get host and port from mongodb client
    host, port = client.address

    # Look for processes connected to this address
    for conn in psutil.net_connections(kind="inet"):
        if conn.laddr.port == port and conn.status == "LISTEN":
            try:
                proc = psutil.Process(conn.pid)
                if "mongod" in proc.name().lower():
                    # get dbpath from command line
                    cmdline = proc.cmdline()
                    if "--dbpath" in cmdline:
                        idx = cmdline.index("--dbpath")
                        raw_path = cmdline[idx + 1]

                        # Get absolute dbpath
                        if not os.path.isabs(raw_path):
                            return os.path.join(proc.cwd(), raw_path)
                        return raw_path
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    return None


@dataclass(slots=True)
class MongoDiskUsage:
    data_storage_bytes: int
    mongo_storage_bytes: int | None
    mongo_log_bytes: int | None

    @property
    def total_bytes(self) -> int:
        if self.mongo_storage_bytes is None:
            # Add only database size
            total = self.data_storage_bytes
        else:
            # Assume database size is already included into mongodb dir size
            total = self.mongo_storage_bytes
        if self.mongo_log_bytes:
            total += self.mongo_log_bytes
        return total

    def __str__(self) -> str:
        pieces = [
            "total: " + _get_human_readable_file_size(self.total_bytes),
            "db: " + _get_human_readable_file_size(self.data_storage_bytes),
        ]
        if self.mongo_storage_bytes is None:
            pieces += ["mongo dir: unknown (inexistent or permission error)"]
        else:
            pieces += [
                "mongo dir: " + _get_human_readable_file_size(self.mongo_storage_bytes)
            ]
        if self.mongo_log_bytes is None:
            pieces += ["mongo log: unknown (inexistent or permission error)"]
        else:
            pieces += [
                "mongo log: " + _get_human_readable_file_size(self.mongo_log_bytes)
            ]
        return ", ".join(pieces)

    @classmethod
    def compute_disk_usage(cls):
        # Database storage size
        db = config().mongo.database_instance
        stats = db.command("dbStats")
        storage_size_bytes = stats["storageSize"]
        index_size_bytes = stats["indexSize"]
        db_size_bytes = storage_size_bytes + index_size_bytes
        total_size_expected = stats.get("totalSize", None)
        if total_size_expected is not None and total_size_expected != db_size_bytes:
            logger.debug(
                f"Database size mismatch: "
                f"expected {total_size_expected} bytes, "
                f"inferred {db_size_bytes} bytes"
            )

        # MongoDB whole data storage size
        folder_size_bytes = None
        server_config = db.client.admin.command("getCmdLineOpts")
        db_path = server_config["parsed"].get("storage", {}).get("dbPath")
        if db_path is None:
            db_path = _find_mongodb_path_via_connection(db.client)
        if db_path is not None:
            logger.debug(f"[mongodb] db path: {db_path}")
            folder_size_bytes = _get_physical_size(db_path)

        # MongoDB log directory size
        log_path = server_config["parsed"].get("systemLog", {}).get("path")
        log_size_bytes = None
        if log_path:
            logger.debug(f"[mongodb] log path: {log_path}")
            log_size_bytes = _get_physical_size(log_path)

        return cls(
            data_storage_bytes=db_size_bytes,
            mongo_storage_bytes=folder_size_bytes,
            mongo_log_bytes=log_size_bytes,
        )


def check_disk_space_for_db(max_size_bytes: int) -> None:
    usage = MongoDiskUsage.compute_disk_usage()
    if usage.total_bytes > max_size_bytes:
        logger.warning(
            f"[mongodb] size exceeded: max {_get_human_readable_file_size(max_size_bytes)}, current: {usage}"
        )


def check_disk_space_for_cache(max_size_bytes: int) -> None:
    cache_path = config().cache
    logger.debug(f"[sarc-cache] folder: {cache_path}")
    cache_size_bytes = _get_physical_size(cache_path)
    if cache_size_bytes is None:
        logger.error(
            f"[sarc-cache] cannot get size for cache folder (inexistent or permission error): {cache_path}"
        )
        return
    if cache_size_bytes > max_size_bytes:
        logger.warning(
            f"[sarc-cache] size exceeded: max {_get_human_readable_file_size(max_size_bytes)}, "
            f"current: {_get_human_readable_file_size(cache_size_bytes)}"
        )
