"""Timespan-dependent cache."""

import inspect
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property, wraps
from typing import Optional

from apischema import deserialize
from gifnoc.std import time


@dataclass
class CachedResult:
    """Represents a result computed at some time."""

    # Cached value
    value: object = None
    # Date at which the value was produced
    issued: datetime = None


@dataclass(unsafe_hash=True)
class Timespan:

    # Time duration
    duration: timedelta

    # Time offset between the end of the duration and time.now()
    offset: Optional[timedelta] = timedelta(seconds=0)

    # How long a cached result for this timespan is valid (defaults to same as duration)
    validity: Optional[timedelta] = None

    def __post_init__(self):
        if isinstance(self.duration, str):
            self.duration = deserialize(timedelta, self.duration)
        if isinstance(self.offset, str):
            self.offset = deserialize(timedelta, self.offset)
        if isinstance(self.validity, str):
            self.validity = deserialize(timedelta, self.validity)
        elif self.validity is None:
            self.validity = self.duration

    def calculate_bounds(self, anchor=None):
        """Calculate time bounds (start, end).

        The anchor is the end of the time period (default: time.now()). The offset
        is subtracted from it to give `end`, and duration is subtracted from `end`
        to give `start`.
        """
        end = (anchor or time.now()) - self.offset
        start = end - self.duration
        return (start, end)

    @cached_property
    def bounds(self):
        """Return the time bounds (start, end) anchored at time.now()."""
        return self.calculate_bounds()

    @cached_property
    def key(self):
        """Key for caching."""
        # Validity does not need to be part of the key because the cached
        # information does not depend on the validity period.
        return (self.duration, self.offset)

    def __str__(self):
        s = f"{self.duration}"
        if self.offset:
            s += f" {self.offset} ago"
        return s


def spancache(fn):
    """Decorator to cache a function's result for a certain duration.

    The function's first argument should be a Timespan object which contains
    a duration, optional offset, and a validity period.
    """
    if "self" in inspect.signature(fn).parameters:
        # It's just kind of a pain in the ass, we can try to make it work if
        # necessary.
        raise TypeError("@spancache does not work on methods")

    cache = {}

    @wraps(fn)
    def wrapped(timespan, *args, **kwargs):
        if current := cache.get(timespan.key, None):
            if time.now() < current.issued + timespan.validity:
                return current.value

        value = fn(timespan, *args, **kwargs)
        entry = CachedResult(value=value, issued=time.now())

        cache[timespan.key] = entry
        return value

    return wrapped
