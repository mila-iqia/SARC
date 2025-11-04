from __future__ import annotations

import contextlib
import json
import logging
import os
import re
from collections.abc import Iterable, Iterator
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta
from enum import Enum
from functools import partial, wraps
from pathlib import Path
from typing import IO, Any, Callable, ClassVar, Literal, Protocol, overload
from zipfile import ZIP_LZMA, ZipFile

from .config import config

logger = logging.getLogger(__name__)
UTCOFFSET = timedelta(0)


class FormatterProto[T](Protocol):
    read_flags: ClassVar[str]
    write_flags: ClassVar[str]

    @staticmethod
    def load(fp: IO[Any]) -> T: ...  # pragma: nocover

    @staticmethod
    def dump(obj: T, fp: IO[Any]) -> None: ...  # pragma: nocover


class JSONFormatter[T](FormatterProto[T]):
    read_flags = "r"
    write_flags = "w"

    @staticmethod
    def load(fp: IO[Any]) -> T:
        return json.load(fp)

    @staticmethod
    def dump(obj: T, fp: IO[Any]) -> None:
        json.dump(obj, fp)


class BinaryFormatter(FormatterProto[bytes]):
    read_flags = "rb"
    write_flags = "wb"

    @staticmethod
    def load(fp: IO[bytes]) -> bytes:
        return fp.read()

    @staticmethod
    def dump(obj: bytes, fp: IO[bytes]) -> None:
        todo = len(obj)
        while todo:
            res = fp.write(obj)
            if res > 0:
                todo -= res
            obj = obj[res:]


class CacheException(Exception):
    pass


def ensure_utc(d: datetime) -> datetime:
    assert d.tzinfo is not None
    return d.astimezone(UTC)


class CacheEntry:
    """Describe a single cache entry at a point in time.

    The cache entry contains multiple key-value pairs and the keys can be repeated with different content."""

    _zf: ZipFile

    def __init__(self, zf: ZipFile):
        self._zf = zf

    def add_value(self, key: str, value: bytes) -> None:
        """Add a key-value pair to the cache entry"""
        self._zf.writestr(key, value)

    def items(self) -> Iterator[tuple[str, bytes]]:
        """Get all the key, value pairs in the order they were added."""
        for zi in self._zf.infolist():
            yield zi.filename, self._zf.read(zi)

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
            "seconds"
        )
        output_file.parent.mkdir(parents=True, exist_ok=True)
        zf = ZipFile(
            output_file,
            mode="x",
            compression=ZIP_LZMA,
        )
        ce = CacheEntry(zf)
        try:
            yield ce
        finally:
            ce.close()

    def save(self, key: str, at_time: datetime, value: bytes) -> None:
        """Save binary data to the cache for a specific key and timestamp.

        Only use this method if you want to save a single value for a given timestamp.

        Args:
            key: The cache key identifier.
            at_time: The datetime when this data was generated, must be in UTC.
            value: The binary data to store in the cache.

        Example:
            >>> cache = Cache()
            >>> cache.save("data", datetime.now(), b"binary data")
        """
        with self.create_entry(at_time) as ce:
            ce.add_value(key, value)

    def _paths_from(self, from_time: datetime) -> Iterable[Path]:
        """Returns paths starting from a specific datetime.

        Returns an iterator over all cached entries that were created at or
        after the specified time. Searches through the date hierarchy starting
        from the given date and continuing forward through all subsequent dates.

        Args:
            from_time: The earliest datetime to include in results.

        Yields:
            Path: The path for each matching cache entry.
        """
        cdir = self.cache_dir
        from_time = ensure_utc(from_time)

        first_dir = self._dir_from_date(cdir, from_time)

        if first_dir.exists():
            from_time_nodays = from_time.time()
            for file in filter(
                lambda fname: time.fromisoformat(fname.parts[-1]) >= from_time_nodays,
                sorted(first_dir.iterdir()),
            ):
                yield file

        from_time = from_time.replace(hour=0, minute=0, second=0, microsecond=0)
        from_time += timedelta(days=1)

        first_year_done = False
        first_month_done = False

        for year_dir in sorted(
            filter(lambda y: int(y.parts[-1]) >= from_time.year, cdir.iterdir())
        ):
            if not first_year_done and int(year_dir.parts[-1]) > from_time.year:
                first_year_done = True
            for month_dir in sorted(
                filter(
                    lambda m: first_year_done or int(m.parts[-1]) >= from_time.month,
                    year_dir.iterdir(),
                )
            ):
                if not first_month_done and (
                    int(month_dir.parts[-1]) > from_time.month or first_year_done
                ):
                    first_month_done = True
                for day_dir in sorted(
                    filter(
                        lambda d: first_month_done or int(d.parts[-1]) >= from_time.day,
                        month_dir.iterdir(),
                    )
                ):
                    for file in sorted(day_dir.iterdir()):
                        yield file
                first_month_done = True
            first_year_done = True

    def read_from(self, from_time: datetime) -> Iterable[CacheEntry]:
        """Read all cached entries starting from a specific datetime.

        Returns an iterator over all cached entries that were created at or after
        the specified time. Unlike `read_from()`, this method returns entries for
        all keys, not just a specific key. The cache files are searched through
        the date hierarchy starting from the given date and continuing forward
        through all subsequent dates.

        Args:
            from_time: The earliest datetime to include in results. Must be UTC.

        Yields:
            tuple[str, bytes]: A tuple containing:
                - The cache key
                - The binary data from the cache entry

        Example:
            >>> cache = Cache("my_data")
            >>> start_time = datetime(2024, 1, 15, 10, 0, 0)
            >>> for key, data in cache.read_from_all(start_time):
            ...     print(f"Key: {key}, Data size: {len(data)} bytes")
        """
        for file in self._paths_from(from_time):
            yield CacheEntry(ZipFile(file, mode="r"))


@dataclass
class CachedResult[T]:
    """Represents a result computed at some time."""

    # Cached value
    value: T
    # Date at which the value was produced
    issued: datetime


_time_format = "%Y-%m-%d-%H-%M-%S"
_time_glob_pattern = "????-??-??-??-??-??"
_time_regexp = _time_glob_pattern.replace("?", r"\d")


class CachePolicy(Enum):
    use = True
    refresh = False
    ignore = "ignore"
    always = "always"
    # Cache policy to check live data aginast cached values
    check = "check"


# Context var to store cache policy got from env var SARC_CACHE
cache_policy_var: ContextVar[CachePolicy | None] = ContextVar(
    "cache_policy_var", default=None
)


def _cache_policy_from_env() -> CachePolicy:
    """Infer cache policy from env var SARC_CACHE"""

    if (current := cache_policy_var.get()) is not None:
        return current

    policy_name = os.getenv("SARC_CACHE", "use")
    policy = getattr(CachePolicy, policy_name, CachePolicy.use)
    logger.info(f"inferred cache policy: {policy}")

    cache_policy_var.set(policy)
    return policy


@dataclass(kw_only=True)
class OldCache[T]:
    formatter: type[FormatterProto[T]] = JSONFormatter[T]
    cache_root: Path | None
    subdirectory: str
    on_disk: bool = True
    live: bool = False
    live_cache: dict[tuple[Path | None, str], CachedResult[T]] = field(
        default_factory=dict
    )

    @property
    def cache_dir(self) -> Path | None:
        root = self.cache_root or config().cache
        return root and root / self.subdirectory

    def read(
        self,
        key: str,
        at_time: datetime | None = None,
        valid: timedelta | Literal[True] = True,
    ) -> T:
        if at_time is None:
            at_time = datetime.now(UTC)
        return self._read_for_key(key, at_time, valid)

    def save(self, key: str, value: T, at_time: datetime | None = None) -> None:
        if at_time is None:
            at_time = datetime.now(UTC)
        return self._save_for_key(key, value, at_time)

    def _save_for_key(self, key: str, value: T, at_time: datetime) -> None:
        """Save a value in cache for the given key."""
        if self.live:
            self.live_cache[(self.cache_dir, key)] = CachedResult(
                issued=at_time,
                value=value,
            )
        cdir = self.cache_dir
        if self.on_disk and cdir is not None:
            cdir.mkdir(parents=True, exist_ok=True)
            output_file = cdir / key.format(time=at_time.strftime(_time_format))
            encoding = None if "b" in self.formatter.write_flags else "utf-8"
            with open(
                output_file, self.formatter.write_flags, encoding=encoding
            ) as output_fp:
                self.formatter.dump(value, output_fp)
            logger.debug("saved to cache file '%s'", output_file)

    def _read_for_key(
        self, key_value: str, at_time: datetime, valid: timedelta | Literal[True] = True
    ) -> T:
        timestring = at_time.strftime(_time_format)
        live_key = (self.cache_dir, key_value)

        if self.live and (previous_result := self.live_cache.get(live_key, None)):
            if valid is True or at_time <= previous_result.issued + valid:
                logger.debug("read from live cache for key '%s'", key_value)
                return previous_result.value

        cdir = self.cache_dir
        if self.on_disk and cdir is not None:
            candidates = sorted(
                cdir.glob(key_value.format(time=_time_glob_pattern)),
                reverse=True,
            )
            maximum = key_value.format(time=timestring)
            possible = [c for c in candidates if c.name <= maximum]
            for candidate in possible:
                if valid is True:
                    # The specific value doesn't matter, it's ignored later
                    candidate_time = datetime.now(UTC)
                else:
                    m = re.match(
                        string=candidate.name,
                        pattern=key_value.format(time=f"({_time_regexp})"),
                    )
                    if m is None:
                        logger.warning(
                            "Could not parse time from cache file name '%s'", candidate
                        )
                        continue
                    candidate_time = datetime.strptime(
                        m.group(1), _time_format
                    ).replace(tzinfo=UTC)

                if valid is True or at_time <= candidate_time + valid:
                    encoding = None if "b" in self.formatter.read_flags else "utf-8"
                    with open(
                        candidate, self.formatter.read_flags, encoding=encoding
                    ) as candidate_fp:
                        try:
                            value = self.formatter.load(candidate_fp)
                        except (  # pylint: disable=broad-exception-caught
                            Exception
                        ) as exc:
                            logger.warning(
                                "Could not load malformed cache file: %s",
                                candidate,
                                exc_info=exc,
                            )
                            continue
                    if self.live:
                        self.live_cache[live_key] = CachedResult(
                            issued=candidate_time,
                            value=value,
                        )
                    logger.debug("read from cache file '%s'", candidate)
                    return value

        raise CacheException(f"There is no cached result for key `{key_value}`")


@dataclass(kw_only=True)
class CachedFunction[**P, R](OldCache[R]):  # pylint: disable=too-many-instance-attributes
    fn: Callable[P, R]
    key: Callable[P, str | None]
    validity: timedelta | Callable[P, timedelta] | Literal[True] = True

    def __post_init__(self):
        self.logger = logging.getLogger(self.fn.__module__)
        self.name = self.fn.__qualname__

    def __get__(self, parent, _):
        """Called when a cached function is a method."""
        symbol = f"_cached_{self.name}"
        cf = getattr(parent, symbol, None)
        if not cf:
            cf = CachedFunction(
                fn=self.fn.__get__(parent),
                formatter=self.formatter,
                key=self.key.__get__(parent),
                validity=(
                    self.validity.__get__(parent)
                    if callable(self.validity)
                    else self.validity
                ),
                live=self.live,
                cache_root=self.cache_root,
                subdirectory=self.subdirectory,
            )
            setattr(parent, symbol, cf)
        return cf

    # pylint: disable=too-many-branches
    def __call__(
        self,
        *args,
        cache_policy: CachePolicy | None = CachePolicy.use,
        save_cache: bool | None = None,
        at_time: datetime | None = None,
        **kwargs,
    ) -> R:
        # If cache_policy is None,
        # we infer if from environment variable
        # SARC_CACHE.
        cache_policy = (
            _cache_policy_from_env()
            if cache_policy is None
            else CachePolicy(cache_policy)
        )
        at_time = at_time or datetime.now(UTC)
        key_value = self.key(*args, **kwargs)

        # Whether to **require** the cache
        require_cache = cache_policy is CachePolicy.always

        # Whether to use the cache
        use_cache = key_value is not None and (
            require_cache
            or (cache_policy is CachePolicy.use)
            or (cache_policy is CachePolicy.check)
        )

        cached_value = None
        has_cache = False
        if use_cache:
            # We know this is true, but this is just to reassure the type system
            assert key_value is not None
            try:
                if cache_policy is CachePolicy.always or self.validity is True:
                    valid: timedelta | Literal[True] = True
                else:
                    assert "{time}" in key_value
                    if isinstance(self.validity, timedelta):
                        valid = self.validity
                    else:
                        valid = self.validity(*args, **kwargs)
                cached_value = self._read_for_key(
                    key_value, valid=valid, at_time=at_time
                )
                has_cache = True
                if cache_policy is not CachePolicy.check:
                    return cached_value
            except CacheException:
                pass

        if require_cache:
            raise CacheException(f"There is no cached result for key `{key_value}`")

        self.logger.debug(f"Computing {self.name}(...) for key '{key_value}'")
        value = self.fn(*args, **kwargs)

        if cache_policy is CachePolicy.check and has_cache:
            if cached_value == value:
                logger.info(f"cache checked: {key_value}")
            else:
                # Live result != cached result. Raise an exception.
                # Try to pretty print diff if we have JSON data.
                if self.formatter is JSONFormatter:
                    import difflib

                    d1_str = json.dumps(cached_value, indent=1, sort_keys=True)
                    d2_str = json.dumps(value, indent=1, sort_keys=True)

                    difference = "\n".join(
                        difflib.unified_diff(
                            d1_str.splitlines(),
                            d2_str.splitlines(),
                            fromfile="cached",
                            tofile="value",
                            lineterm="",
                        )
                    )
                else:
                    difference = (
                        f"Cached:\n{repr(cached_value)}\n\nValue:\n{repr(value)}\n"
                    )
                raise CacheException(
                    f"\nCached result != live result:\n"
                    f"Key: {key_value}\n\n"
                    f"{difference}\n"
                )

        # Whether to save the cache
        if key_value is None:
            save_cache = False
        elif save_cache is None:
            save_cache = cache_policy is not CachePolicy.ignore

        if save_cache:
            # Once again, we know this is true, but this reassures the type system.
            assert key_value is not None
            self._save_for_key(key_value, value, at_time=at_time)

        return value


def default_key(*_, **__) -> str:
    return "{time}.json"


def make_cached_function[**P, R](
    fn: Callable[P, R],
    formatter: type[FormatterProto[R]],
    key: Callable[P, str | None] | None,
    subdirectory: str | None,
    validity: timedelta | Callable[P, timedelta] | Literal[True],
    on_disk: bool,
    live: bool,
    cache_root: Path | None,
) -> CachedFunction[P, R]:
    if subdirectory is None:
        subdirectory = fn.__qualname__

    return CachedFunction(
        fn=fn,
        formatter=formatter,
        key=key or default_key,
        validity=validity,
        live=live,
        on_disk=on_disk,
        cache_root=cache_root,
        subdirectory=subdirectory,
    )


@overload
def with_cache[**P, R](
    fn: Callable[P, R],
    formatter: type[FormatterProto[R]] = JSONFormatter,
    key: Callable[P, str | None] | None = None,
    subdirectory: str | None = None,
    validity: timedelta | Callable[P, timedelta] | Literal[True] = True,
    on_disk: bool = True,
    live: bool = False,
    cache_root: Path | None = None,
) -> CachedFunction[P, R]: ...


@overload
def with_cache[**P, R](
    fn: None = None,
    formatter: type[FormatterProto[R]] = JSONFormatter,
    key: Callable[P, str | None] | None = None,
    subdirectory: str | None = None,
    validity: timedelta | Callable[P, timedelta] | Literal[True] = True,
    on_disk: bool = True,
    live: bool = False,
    cache_root: Path | None = None,
) -> Callable[[Callable[P, R]], CachedFunction[P, R]]: ...


def with_cache[**P, R](
    fn: Callable[P, R] | None = None,
    formatter: type[FormatterProto[R]] = JSONFormatter,
    key: Callable[P, str | None] | None = None,
    subdirectory: str | None = None,
    validity: timedelta | Callable[P, timedelta] | Literal[True] = True,
    on_disk: bool = True,
    live: bool = False,
    cache_root: Path | None = None,
) -> CachedFunction[P, R] | Callable[[Callable[P, R]], CachedFunction[P, R]]:
    """Cache the output value of the function in a file.

    Arguments:
        fn: The function for which to cache results.
        subdirectory: Subdirectory for the cache files. Defaults to the qualified
            name of the function.
        key: A key function that returns the filename of the cache file to use.
            The function takes the same arguments as fn. The special string
            ``{time}`` can be inserted in the return value, to represent the time
            at which the data was generated. If validity is not the boolean True,
            ``{{time}}`` **must** be in the key.
        validity:
            * True: Cache entries are valid indefinitely.
            * timedelta: A cache entry is valid for up to that duration.
            * (*args, **kwargs) -> timedelta: If the validity time depends on the
              inputs.
        on_disk: If True, the values will be saved to disk.
        live: If True, the values will be kept in memory.
        formatter: A module or object with load and dump properties, used to load or
            save the data from disk. Defaults to the ``json`` module.
        cache_root: The root cache directory, defaults to config().cache

    Returns:
        A function with the same signature, except for a few extra arguments:
        * cache_policy (default: CachePolicy.use (== True)):
          * CachePolicy.use (True): Use the cache, refresh it if past validity period.
          * CachePolicy.refresh (False): Recompute the value no matter what.
          * CachePolicy.always ("always"): Never recompute; if there is no cached result,
            raise an exception.
          * CachePolicy.ignore ("ignore"): Ignore the cache altogether: recompute, and
            do not save the result to cache.
          * CachePolicy.check ("check"): Recompute the value and compare it
            to cache if available. Raise an exception if cache != value.
          * None: Get cache policy from environment variable SARC_CACHE
            (default: CachePolicy.use)
        * save_cache (default: True): Whether to cache the result on disk or not.
        * at_time (default: now): The time at which to evaluate the request.
    """
    deco = partial(
        make_cached_function,
        formatter=formatter,
        key=key,
        subdirectory=subdirectory,
        validity=validity,
        on_disk=on_disk,
        live=live,
        cache_root=cache_root,
    )
    if fn is None:
        return deco
    else:
        return wraps(fn)(deco(fn=fn))  # type: ignore
