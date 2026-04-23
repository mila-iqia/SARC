from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from zipfile import ZIP_LZMA, ZipFile

from .config import config
from .utils import ensure_utc

logger = logging.getLogger(__name__)
UTCOFFSET = timedelta(0)


def no_current(fname: Path) -> bool:
    return fname.suffix not in [".current", ".DS_Store"]


class CacheEntry:
    """Describe a single cache entry at a point in time.

    The cache entry contains multiple key-value pairs and the keys can be repeated with different content."""

    _zf: ZipFile

    def __init__(self, zf: ZipFile, entry_datetime: datetime):
        self._zf = zf
        self.entry_datetime = ensure_utc(entry_datetime)

    def add_value(self, key: str, value: bytes) -> None:
        """Add a key-value pair to the cache entry"""
        self._zf.writestr(key, value)

    def items(self) -> Iterator[tuple[str, bytes]]:
        """Get all the key, value pairs in the order they were added."""
        for zi in self._zf.infolist():
            yield zi.filename, self._zf.read(zi)

    def get_entry_datetime(self) -> datetime:
        """Get the time when this cache entry was created."""
        return self.entry_datetime

    def close(self) -> None:
        """Close the cache entry. MUST be called for new entries."""
        self._zf.close()


class Cache:
    """A simple file-based cache that stores data organized by date.

    This cache stores binary data in a hierarchical directory structure based on
    the date when the data was cached. Files are organized as:
    cache_root/subdirectory/YYYY/MM/DD/HH:MM:SS

    Attributes:
        subdirectory: The subdirectory name within the cache root where data
                     will be stored.
    """

    subdirectory: str

    def __init__(self, subdirectory: str):
        self.subdirectory = subdirectory

    @property
    def cache_dir(self) -> Path:
        """Get the cache directory path for this cache instance.

        Creates the directory if it doesn't exist.

        Returns:
            Path: The absolute path to the cache directory.
        """
        root = config().cache
        assert root is not None
        res = root / self.subdirectory
        res.mkdir(parents=True, exist_ok=True)
        return res

    def _dir_from_date(self, cdir: Path, d: datetime) -> Path:
        """Get the directory path for a specific date within the cache.

        Args:
            cdir: The base cache directory path.
            d: The datetime for which to get the directory path.

        Returns:
            Path: The path to the date-specific directory.
        """
        return cdir / f"{d.year:04}" / f"{d.month:02}" / f"{d.day:02}"

    @contextlib.contextmanager
    def create_entry(self, at_time: datetime) -> Iterator[CacheEntry]:
        """Create a writable CacheEntry for the specified time.

        This is a context manager so use like this:

        with cache.create_entry(date) as ce:
            ce.add_value(...)
        # rest of the code
        """
        cdir = self.cache_dir

        at_time = ensure_utc(at_time)

        output_file = self._dir_from_date(cdir, at_time) / at_time.time().isoformat(
            "milliseconds"
        )
        working_file = output_file.with_suffix(".current")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        zf = ZipFile(working_file, mode="x", compression=ZIP_LZMA)
        ce = CacheEntry(zf, at_time)
        try:
            yield ce
        finally:
            ce.close()
            working_file.rename(output_file)

    def save(self, key: str, at_time: datetime, value: bytes) -> None:
        """Save binary data to the cache for a specific key and timestamp.

        Only use this method if you want to save a single value for a given timestamp.

        Args:
            key: The cache key identifier.
            at_time: The datetime when this data was generated, must be in UTC.
            value: The binary data to store in the cache.

        Example:
            >>> cache = Cache()
            >>> cache.save("data", datetime.now(UTC), b"binary data")
        """
        with self.create_entry(at_time) as ce:
            ce.add_value(key, value)

    def _datetime_from_path(self, path: Path) -> datetime:
        """Get the datetime from a cache entry path."""
        file_time = time.fromisoformat(path.parts[-1])
        return datetime(
            year=int(path.parts[-4]),
            month=int(path.parts[-3]),
            day=int(path.parts[-2]),
            hour=file_time.hour,
            minute=file_time.minute,
            second=file_time.second,
            microsecond=file_time.microsecond,
            tzinfo=UTC,
        )

    def _paths_from(self, from_time: datetime) -> Iterable[tuple[Path, datetime]]:
        """Returns paths starting from a specific datetime.

        Returns an iterator over all cached entries that were created at or
        after the specified time. Searches through the date hierarchy starting
        from the given date and continuing forward through all subsequent dates.

        Args:
            from_time: The earliest datetime to include in results.

        Yields:
            Path: The path for each matching cache entry.
            datetime: The time this entry was fetched
        """
        cdir = self.cache_dir
        from_time = ensure_utc(from_time)

        first_dir = self._dir_from_date(cdir, from_time)

        ignore_files = [
            ".DS_Store"
        ]  # a bit hardcoded but soooo frequent on dev machines it had to be done

        if first_dir.exists():
            from_time_nodays = from_time.time()
            for file in filter(
                lambda fname: time.fromisoformat(fname.parts[-1]) > from_time_nodays,
                filter(no_current, sorted(first_dir.iterdir())),
            ):
                yield file, self._datetime_from_path(file)

        from_time = from_time.replace(hour=0, minute=0, second=0, microsecond=0)
        from_time += timedelta(days=1)

        first_year_done = False
        first_month_done = False

        for year_dir in sorted(
            filter(
                lambda y: y.name not in ignore_files and int(y.name) >= from_time.year,
                cdir.iterdir(),
            )
        ):
            if not first_year_done and int(year_dir.name) > from_time.year:
                first_year_done = True
            for month_dir in sorted(
                filter(
                    lambda m: (
                        m.name not in ignore_files
                        and (first_year_done or int(m.name) >= from_time.month)
                    ),
                    year_dir.iterdir(),
                )
            ):
                if not first_month_done and (
                    int(month_dir.name) > from_time.month or first_year_done
                ):
                    first_month_done = True
                for day_dir in sorted(
                    filter(
                        lambda d: (
                            d.name not in ignore_files
                            and (first_month_done or int(d.name) >= from_time.day)
                        ),
                        month_dir.iterdir(),
                    )
                ):
                    for file in filter(no_current, sorted(day_dir.iterdir())):
                        yield file, self._datetime_from_path(file)
                first_month_done = True
            first_year_done = True

    def read_from(self, from_time: datetime) -> Iterable[CacheEntry]:
        """Read all cached entries starting from a specific datetime.

        Returns an iterator over all cached entries that were created at or
        after the specified time. The cache files are searched through the date
        hierarchy starting from the given date and continuing forward through
        all subsequent dates.

        Args:
            from_time: The earliest datetime to include in results. Must be UTC.

        Yields:
            tuple[str, bytes]: A tuple containing:
                - The cache key
                - The binary data from the cache entry

        Example:
            >>> cache = Cache("my_data")
            >>> start_time = datetime(2024, 1, 15, 10, 0, 0)
            >>> for ce in cache.read_from(start_time):
            >>>     for key, data in ce.items():
            ...         print(f"Key: {key}, Data size: {len(data)} bytes")
        """
        for file, fetch_time in self._paths_from(from_time):
            yield CacheEntry(ZipFile(file, mode="r"), fetch_time)

    def latest_entry(self) -> CacheEntry | None:
        """Returns the most recent cache entry if exists, otherwise None."""
        cdir = self.cache_dir
        for year_dir in sorted(cdir.iterdir(), key=_basename_to_int, reverse=True):
            for month_dir in sorted(
                year_dir.iterdir(), key=_basename_to_int, reverse=True
            ):
                for day_dir in sorted(
                    month_dir.iterdir(), key=_basename_to_int, reverse=True
                ):
                    for file in sorted(
                        filter(no_current, day_dir.iterdir()),
                        key=_basename_to_time,
                        reverse=True,
                    ):
                        return CacheEntry(
                            ZipFile(file, mode="r"), self._datetime_from_path(file)
                        )
        return None

    def oldest_year(self) -> datetime:
        """
        Return the oldest year in the cache if exists, otherwise current year.
        return: January 1st of year found, at 00h 00min 00sec 00microseconds in UTC timezone.
        """
        cdir = self.cache_dir
        for year_dir in sorted(cdir.iterdir(), key=_basename_to_int):
            return datetime(year=_basename_to_int(year_dir), month=1, day=1, tzinfo=UTC)
        return datetime.now(tz=UTC).replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )


def _basename_to_int(path: Path) -> int:
    return int(path.parts[-1])


def _basename_to_time(path: Path) -> time:
    return time.fromisoformat(path.parts[-1])
