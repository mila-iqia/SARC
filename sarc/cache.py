from __future__ import annotations

import json
import logging
import os
import re
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import partial, wraps
from pathlib import Path
from typing import (
    IO,
    Any,
    Callable,
    ClassVar,
    Literal,
    Optional,
    Protocol,
    overload,
)

from .config import config


class FormatterProto[T](Protocol):
    read_flags: ClassVar[str]
    write_flags: ClassVar[str]

    @staticmethod
    def load(fp: IO[Any]) -> T: ...

    @staticmethod
    def dump(obj: T, fp: IO[Any]) -> None: ...


class JSONFormatter[T](FormatterProto[T]):
    read_flags = "r"
    write_flags = "w"

    @staticmethod
    def load(fp: IO[Any]) -> T:
        return json.load(fp)

    @staticmethod
    def dump(obj: T, fp: IO[Any]) -> None:
        json.dump(obj, fp)


class CacheException(Exception):
    pass


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
    logging.info(f"inferred cache policy: {policy}")

    cache_policy_var.set(policy)
    return policy


@dataclass(kw_only=True)
class CachedFunction[**P, R]:  # pylint: disable=too-many-instance-attributes
    fn: Callable[P, R]
    formatter: type[FormatterProto[R]] = JSONFormatter[R]
    key: Callable[P, str]
    subdirectory: str
    validity: timedelta | Callable[P, timedelta] | Literal[True] = True
    on_disk: bool = True
    live: bool = False
    cache_root: Optional[Path] = None
    live_cache: dict[tuple[Path, str], CachedResult[R]] = field(default_factory=dict)

    def __post_init__(self):
        self.logger = logging.getLogger(self.fn.__module__)
        self.name = self.fn.__qualname__

    @property
    def cache_dir(self) -> Path:
        """Return the cache directory."""
        root = self.cache_root or config().cache
        assert root is not None
        return root and (root / self.subdirectory)

    def cache_path(self, *args, at_time: datetime | None = None, **kwargs) -> Path:
        """Cache path for the result of the call for given args and kwargs."""
        key = self.key(*args, **kwargs)
        return self.cache_path_for_key(key, at_time)

    def cache_path_for_key(self, key: str, at_time: datetime | None = None) -> Path:
        """Cache path for the given key."""
        at_time = at_time or datetime.now()
        return self.cache_dir / key.format(time=at_time.strftime(_time_format))

    def save(
        self, *args, value_to_save: R, at_time: datetime | None = None, **kwargs
    ) -> None:
        """Save a value in cache for the given args and kwargs.

        This does not execute the function.
        """
        key = self.key(*args, **kwargs)
        self.save_for_key(key, value_to_save, at_time=at_time)

    def save_for_key(self, key: str, value: R, at_time: datetime | None = None) -> None:
        """Save a value in cache for the given key."""
        at_time = at_time or datetime.now()
        cdir = self.cache_dir
        if self.live:
            self.live_cache[(cdir, key)] = CachedResult(
                issued=at_time,
                value=value,
            )
        if cdir and self.on_disk:
            cdir.mkdir(parents=True, exist_ok=True)
            output_file = cdir / key.format(time=at_time.strftime(_time_format))
            encoding = None if "b" in self.formatter.write_flags else "utf-8"
            with open(
                output_file, self.formatter.write_flags, encoding=encoding
            ) as output_fp:
                self.formatter.dump(value, output_fp)
            self.logger.debug(f"{self.name}(...) saved to cache file '{output_file}'")

    def read(self, *args, at_time: datetime | None = None, **kwargs) -> R:
        key = self.key(*args, **kwargs)
        return self.read_for_key(key, at_time=at_time)

    def read_for_key(
        self,
        key_value: str,
        valid: timedelta | Literal[True] = True,
        at_time: datetime | None = None,
    ) -> R:
        at_time = at_time or datetime.now()
        timestring = at_time.strftime(_time_format)
        cdir = self.cache_dir
        live_key = (cdir, key_value)

        if self.live and (previous_result := self.live_cache.get(live_key, None)):
            if valid is True or at_time <= previous_result.issued + valid:
                self.logger.debug(
                    f"{self.name}(...) read from live cache for key '{key_value}'"
                )
                return previous_result.value

        if cdir and self.on_disk:
            candidates = sorted(
                cdir.glob(key_value.format(time=_time_glob_pattern)),
                reverse=True,
            )
            maximum = key_value.format(time=timestring)
            possible = [c for c in candidates if c.name <= maximum]
            if possible:
                candidate = possible[0]
                m = re.match(
                    string=candidate.name,
                    pattern=key_value.format(time=f"({_time_regexp})"),
                )
                if m is None:
                    raise CacheException(
                        f"Could not parse cache file name '{candidate}'"
                    )
                candidate_time = (
                    datetime.now()
                    if valid is True
                    else datetime.strptime(m.group(1), _time_format)
                )
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
                            self.logger.warning(
                                f"Could not load malformed cache file: {candidate}",
                                exc_info=exc,
                            )
                            raise CacheException(
                                f"Could not load malformed cache file: {candidate}"
                            ) from exc
                    if self.live:
                        self.live_cache[live_key] = CachedResult(
                            issued=candidate_time,
                            value=value,
                        )
                    self.logger.debug(
                        f"{self.name}(...) read from cache file '{candidate}'"
                    )
                    return value

        raise CacheException(f"There is no cached result for key `{key_value}`")

    def __get__(self, parent, _):
        """Called when a cached function is a method."""
        symbol = f"_cached_{self.name}"
        cf = getattr(parent, symbol, None)
        if not cf:
            cf = CachedFunction(
                fn=self.fn.__get__(parent),
                formatter=self.formatter,
                key=self.key.__get__(parent),
                subdirectory=self.subdirectory,
                validity=(
                    self.validity.__get__(parent)
                    if callable(self.validity)
                    else self.validity
                ),
                on_disk=self.on_disk,
                live=self.live,
                cache_root=self.cache_root,
            )
            setattr(parent, symbol, cf)
        return cf

    # pylint: disable=too-many-branches
    def __call__(
        self,
        *args,
        cache_policy: CachePolicy = CachePolicy.use,
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
        at_time = at_time or datetime.now()
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
            try:
                if cache_policy is CachePolicy.always or self.validity is True:
                    valid: timedelta | Literal[True] = True
                else:
                    assert "{time}" in key_value
                    if isinstance(self.validity, timedelta):
                        valid = self.validity
                    else:
                        valid = self.validity(*args, **kwargs)
                cached_value = self.read_for_key(
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
                logging.info(f"cache checked: {key_value}")
            else:
                # Live result != cached result. Raise an exception.
                # Try to pretty print diff if we have JSON data.
                if self.formatter is json:
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
                        f"Cached:\n"
                        f"{repr(cached_value)}\n\n"
                        f"Value:\n"
                        f"{repr(value)}\n"
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
            self.save_for_key(key_value, value, at_time=at_time)

        return value


def default_key(*_, **__) -> str:
    return "{time}.json"


def make_cached_function[**P, R](
    fn: Callable[P, R],
    formatter: type[FormatterProto[R]],
    key: Callable[P, str] | None,
    subdirectory: str | None,
    validity: timedelta | Callable[P, timedelta] | Literal[True],
    on_disk: bool,
    live: bool,
    cache_root: Path | None,
) -> CachedFunction[P, R]:
    return CachedFunction(
        fn=fn,
        formatter=formatter,
        key=key or default_key,
        subdirectory=subdirectory or fn.__qualname__,
        validity=validity,
        on_disk=on_disk,
        live=live,
        cache_root=cache_root,
    )


@overload
def with_cache[**P1, R1](
    fn: Callable[P1, R1],
    formatter: type[FormatterProto[R1]] = JSONFormatter,
    key: Callable[P1, str] | None = None,
    subdirectory: str | None = None,
    validity: timedelta | Callable[P1, timedelta] | Literal[True] = True,
    on_disk: bool = True,
    live: bool = False,
    cache_root: Path | None = None,
) -> CachedFunction[P1, R1]: ...


@overload
def with_cache[**P2, R2](
    fn: None = None,
    formatter: type[FormatterProto[R2]] = JSONFormatter,
    key: Callable[P2, str] | None = None,
    subdirectory: str | None = None,
    validity: timedelta | Callable[P2, timedelta] | Literal[True] = True,
    on_disk: bool = True,
    live: bool = False,
    cache_root: Path | None = None,
) -> Callable[[Callable[P2, R2]], CachedFunction[P2, R2]]: ...


def with_cache[**P, R](
    fn: Callable[P, R] | None = None,
    formatter: type[FormatterProto[R]] = JSONFormatter,
    key: Callable[P, str] | None = None,
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
