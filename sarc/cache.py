from __future__ import annotations

import json
import logging
import pickle
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import partial, wraps
from pathlib import Path
from typing import Callable, Optional

from .config import config

pickle.read_flags = "rb"
pickle.write_flags = "wb"


class plaintext:
    read_flags = "r"
    write_flags = "w"

    @staticmethod
    def load(fp):
        return fp.read()

    @staticmethod
    def dump(obj, fp):
        fp.write(obj)


class CacheException(Exception):
    pass


@dataclass
class CachedResult:
    """Represents a result computed at some time."""

    # Cached value
    value: object = None
    # Date at which the value was produced
    issued: datetime = None


_time_format = "%Y-%m-%d-%H-%M-%S"
_time_glob_pattern = "????-??-??-??-??-??"
_time_regexp = _time_glob_pattern.replace("?", r"\d")


class CachePolicy(Enum):
    use = True
    refresh = False
    ignore = "ignore"
    always = "always"


@dataclass
class CachedFunction:  # pylint: disable=too-many-instance-attributes
    fn: Callable
    formatter: object = json
    key: Optional[Callable] = None
    subdirectory: Optional[str] = None
    validity: timedelta | Callable[..., timedelta] | bool = True
    on_disk: bool = True
    live: bool = False
    cache_root: Optional[Path] = None
    live_cache: dict = field(default_factory=dict)

    def __post_init__(self):
        self.subdirectory = self.subdirectory or self.fn.__qualname__
        self.logger = logging.getLogger(self.fn.__module__)
        self.key = self.key or self.default_key
        self.name = self.fn.__qualname__

    def default_key(self, *_, **__):
        return "{time}.json"

    @property
    def cache_dir(self):
        """Return the cache directory."""
        root = self.cache_root or config().cache
        return root and (root / self.subdirectory)

    def cache_path(self, *args, at_time=None, **kwargs):
        """Cache path for the result of the call for given args and kwargs."""
        key = self.key(*args, **kwargs)
        return self.cache_path_for_key(key, at_time)

    def cache_path_for_key(self, key, at_time=None):
        """Cache path for the given key."""
        at_time = at_time or datetime.now()
        return self.cache_dir / key.format(time=at_time.strftime(_time_format))

    def save(self, *args, value_to_save, at_time=None, **kwargs):
        """Save a value in cache for the given args and kwargs.

        This does not execute the function.
        """
        key = self.key(*args, **kwargs)
        return self.save_for_key(key, value_to_save, at_time=at_time)

    def save_for_key(self, key, value, at_time=None):
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
            flags = getattr(self.formatter, "write_flags", "w")
            encoding = None if "b" in flags else "utf-8"
            with open(output_file, flags, encoding=encoding) as output_fp:
                self.formatter.dump(value, output_fp)
            self.logger.debug(f"{self.name}(...) saved to cache file '{output_file}'")

    def read(self, *args, at_time=None, **kwargs):
        key = self.key(*args, **kwargs)
        return self.read_for_key(key, at_time=at_time)

    def read_for_key(self, key_value, valid=True, at_time=None):
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
                candidate_time = (
                    None
                    if valid is True
                    else datetime.strptime(m.group(1), _time_format)
                )
                if valid is True or at_time <= candidate_time + valid:
                    flags = getattr(self.formatter, "read_flags", "r")
                    encoding = None if "b" in flags else "utf-8"
                    with open(candidate, flags, encoding=encoding) as candidate_fp:
                        try:
                            value = self.formatter.load(candidate_fp)
                            success = True
                        except (  # pylint: disable=broad-exception-caught
                            Exception
                        ) as exc:
                            self.logger.warning(
                                f"Could not load malformed cache file: {candidate}",
                                exc_info=exc,
                            )
                            success = False
                    if success:
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

    def __call__(
        self,
        *args,
        cache_policy=CachePolicy.use,
        save_cache=None,
        at_time=None,
        **kwargs,
    ):
        cache_policy = CachePolicy(cache_policy)
        at_time = at_time or datetime.now()
        key_value = self.key(*args, **kwargs)

        # Whether to **require** the cache
        require_cache = cache_policy is CachePolicy.always

        # Whether to use the cache
        use_cache = key_value is not None and (
            require_cache or (cache_policy is CachePolicy.use)
        )

        if use_cache:
            try:
                if cache_policy is CachePolicy.always or self.validity is True:
                    valid = True
                else:
                    assert "{time}" in key_value
                    if isinstance(self.validity, timedelta):
                        valid = self.validity
                    else:
                        valid = self.validity(*args, **kwargs)
                return self.read_for_key(key_value, valid=valid, at_time=at_time)
            except CacheException:
                pass

        if require_cache:
            raise CacheException(f"There is no cached result for key `{key_value}`")

        self.logger.debug(f"Computing {self.name}(...) for key '{key_value}'")
        value = self.fn(*args, **kwargs)

        # Whether to save the cache
        if key_value is None:
            save_cache = False
        elif save_cache is None:
            save_cache = cache_policy is not CachePolicy.ignore

        if save_cache:
            self.save_for_key(key_value, value, at_time=at_time)

        return value


def with_cache(
    fn=None,
    formatter=json,
    key=None,
    subdirectory=None,
    validity=True,
    on_disk=True,
    live=False,
    cache_root=None,
):
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
        * save_cache (default: True): Whether to cache the result on disk or not.
        * at_time (default: now): The time at which to evaluate the request.
    """
    deco = partial(
        CachedFunction,
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
        return wraps(fn)(deco(fn))
