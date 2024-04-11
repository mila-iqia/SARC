import json
import logging
import pickle
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
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


@dataclass
class CachedResult:
    """Represents a result computed at some time."""

    # Cached value
    value: object = None
    # Date at which the value was produced
    issued: datetime = None


def default_key(*args, **kwargs):
    return "{time}.json"


_time_format = "%Y-%m-%d-%H-%M-%S"
_time_glob_pattern = "????-??-??-??-??-??"
_time_regexp = _time_glob_pattern.replace("?", r"\d")


@dataclass
class CachedFunction:
    fn: Callable
    format: object = None
    key: Optional[Callable] = None
    subdirectory: Optional[str] = None
    validity: timedelta | Callable[..., timedelta] | bool = True
    on_disk: bool = True
    live: bool = False
    cachedir: Optional[Path] = None
    live_cache: dict[str, object] = field(default_factory=dict)

    def __post_init__(self):
        self.subdirectory = self.subdirectory or self.fn.__qualname__
        self.logger = logging.getLogger(self.fn.__module__)
        self.key = self.key or self.default_key

    def default_key(self, *args, **kwargs):
        return "{time}.json"

    def __call__(
        self,
        *args,
        use_cache=True,
        save_cache=True,
        require_cache=False,
        at_time=None,
        **kwargs,
    ):
        name = self.fn.__qualname__
        at_time = at_time or datetime.now()
        timestring = at_time.strftime(_time_format)
        key_value = self.key(*args, **kwargs)

        if self.format is None:
            sfx = Path(key_value).suffix
            if sfx == ".json":
                formatter = json
            elif sfx == ".txt":
                formatter = plaintext
            elif sfx == ".pkl":
                formatter = pickle
            else:
                raise Exception(f"Cannot load/dump file format: {sfx}")
        else:
            formatter = self.format

        if require_cache and not use_cache:
            raise ValueError("use_cache cannot be False if require_cache is True")

        cdir = self.cachedir or config().cache

        if use_cache:
            if require_cache or self.validity is True:
                valid = True
            else:
                assert "{time}" in key_value
                if isinstance(self.validity, timedelta):
                    valid = self.validity
                else:
                    valid = self.validity(*args, **kwargs)

            if self.live and (previous_result := self.live_cache.get(key_value, None)):
                if valid is True or at_time <= previous_result.issued + valid:
                    self.logger.debug(
                        f"{name}(...) read from live cache for key '{key_value}'"
                    )
                    return previous_result.value

            if self.on_disk:
                candidates = sorted(
                    (cdir / self.subdirectory).glob(
                        key_value.format(time=_time_glob_pattern)
                    ),
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
                        with open(
                            candidate, getattr(formatter, "read_flags", "r")
                        ) as candidate_fp:
                            value = formatter.load(candidate_fp)
                        if self.live:
                            self.live_cache[key_value] = CachedResult(
                                issued=candidate_time,
                                value=value,
                            )
                        self.logger.debug(
                            f"{name}(...) read from cache file '{candidate}'"
                        )
                        return value

        if require_cache:
            raise Exception(f"There is no cached result for key `{key_value}`")

        self.logger.debug(f"Computing {name}(...) for key '{key_value}'")
        value = self.fn(*args, **kwargs)

        if save_cache:
            (cdir / self.subdirectory).mkdir(parents=True, exist_ok=True)

            if self.live:
                self.live_cache[key_value] = CachedResult(
                    issued=at_time,
                    value=value,
                )
            if self.on_disk:
                output_file = (
                    cdir / self.subdirectory / key_value.format(time=timestring)
                )
                with open(
                    output_file, getattr(formatter, "write_flags", "w")
                ) as output_fp:
                    formatter.dump(value, output_fp)
                self.logger.debug(f"{name}(...) saved to cache file '{output_file}'")

        return value


def with_cache(
    fn=None,
    format=None,
    key=None,
    subdirectory=None,
    validity=True,
    on_disk=True,
    live=False,
    cachedir=None,
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
        format: A module or object with load and dump properties, used to load or
            save the data from disk. If None, will be inferred from the file suffix
            returned by key.
        cachedir: The cache directory, defaults to config().cache

    Returns:
        A function with the same signature, except for a few extra arguments:
        * use_cache (default: True): If ``use_cache`` is False, we will not read
          the output from cache on the disk even if the file exists.
        * save_cache (default: True): Whether to cache the result on disk or not.
        * require_cache (default: False): If True, only return a result from cache,
          and if there is no cached result, raise an exception.
        * at_time (default: now): The time at which to evaluate the request.
    """
    deco = partial(
        CachedFunction,
        format=format,
        key=key,
        subdirectory=subdirectory,
        validity=validity,
        on_disk=on_disk,
        live=live,
        cachedir=cachedir,
    )
    if fn is None:
        return deco
    else:
        return wraps(fn)(deco(fn))
