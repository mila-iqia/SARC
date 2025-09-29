"""Timespan-dependent cache."""

import inspect
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property, wraps
from typing import Callable, Concatenate

from gifnoc.std import time
from serieux import deserialize


@dataclass
class CachedResult[T]:
    """Represents a result computed at some time."""

    # Cached value
    value: T
    # Date at which the value was produced
    issued: datetime


@dataclass(unsafe_hash=True)
class _Timespan:
    # Time duration
    duration: timedelta

    # How long a cached result for this timespan is valid (defaults to same as duration)
    validity: timedelta

    # Time offset between the end of the duration and time.now()
    offset: timedelta = timedelta(seconds=0)

    def __post_init__(self):
        if isinstance(self.duration, str):
            self.duration = deserialize(timedelta, self.duration)
        if isinstance(self.offset, str):
            self.offset = deserialize(timedelta, self.offset)
        if isinstance(self.validity, str):
            self.validity = deserialize(timedelta, self.validity)
        elif self.validity is None:
            self.validity = self.duration

    def calculate_bounds(
        self, anchor: datetime | None = None
    ) -> tuple[datetime, datetime]:
        """Calculate time bounds (start, end).

        The anchor is the end of the time period (default: time.now()). The offset
        is subtracted from it to give `end`, and duration is subtracted from `end`
        to give `start`.
        """
        end = (anchor or time.now()) - self.offset
        start = end - self.duration
        return (start, end)

    @cached_property
    def bounds(self) -> tuple[datetime, datetime]:
        """Return the time bounds (start, end) anchored at time.now()."""
        return self.calculate_bounds()

    @cached_property
    def key(self) -> tuple[timedelta, timedelta]:
        """Key for caching."""
        # Validity does not need to be part of the key because the cached
        # information does not depend on the validity period.
        return (self.duration, self.offset)

    def __str__(self) -> str:
        s = f"{self.duration}"
        if self.offset:
            s += f" {self.offset} ago"
        return s


def Timespan(
    duration: timedelta | str,
    offset: timedelta | str = timedelta(seconds=0),
    validity: timedelta | str | None = None,
) -> _Timespan:
    if isinstance(duration, str):
        _duration = deserialize(timedelta, duration)
    else:
        _duration = duration
    if isinstance(offset, str):
        _offset = deserialize(timedelta, offset)
    else:
        _offset = offset
    if isinstance(validity, str):
        _validity = deserialize(timedelta, validity)
    elif validity is None:
        _validity = _duration
    else:
        _validity = validity

    return _Timespan(duration=_duration, offset=_offset, validity=_validity)


def spancache[**P, R](
    fn: Callable[Concatenate[_Timespan, P], R],
) -> Callable[Concatenate[_Timespan, P], R]:
    """Decorator to cache a function's result for a certain duration.

    The function's first argument should be a Timespan object which contains
    a duration, optional offset, and a validity period.
    """
    if "self" in inspect.signature(fn).parameters:
        # It's just kind of a pain in the ass, we can try to make it work if
        # necessary.
        raise TypeError("@spancache does not work on methods")

    cache: dict[tuple[timedelta, timedelta], CachedResult[R]] = {}

    @wraps(fn)
    def wrapped(timespan: _Timespan, *args: P.args, **kwargs: P.kwargs) -> R:
        if current := cache.get(timespan.key, None):
            if time.now() < current.issued + timespan.validity:
                return current.value

        value = fn(timespan, *args, **kwargs)
        entry = CachedResult(value=value, issued=time.now())

        cache[timespan.key] = entry
        return value

    return wrapped
