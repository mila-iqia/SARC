import logging
import math
import os
from pathlib import Path

from sarc.config import config

logger = logging.getLogger(__name__)


def check_disk_space_for_db(max_size_bytes: int) -> None:
    usage_bytes = _compute_db_disk_usage()
    if usage_bytes > max_size_bytes:
        logger.warning(
            f"[mongodb] size exceeded: max {_get_human_readable_file_size(max_size_bytes)}, "
            f"current: {_get_human_readable_file_size(usage_bytes)}"
        )


def _compute_db_disk_usage():
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
    return db_size_bytes


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


def _get_physical_size(path: Path | str) -> int | None:
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
