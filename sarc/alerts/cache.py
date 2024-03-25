from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property, wraps
from typing import Optional

from apischema import deserialize
from gifnoc.std import time


@dataclass
class CachedResult:
    value: object = None
    issued: datetime = None


@dataclass(unsafe_hash=True)
class Timespan:
    duration: timedelta
    offset: Optional[timedelta] = timedelta(seconds=0)
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
        end = (anchor or time.now()) - self.offset
        start = end - self.duration
        return (start, end)

    @cached_property
    def bounds(self):
        return self.calculate_bounds()

    def __str__(self):
        s = f"{self.duration}"
        if self.offset:
            s += f" {self.offset} ago"
        return s


def spancache(fn):
    """Cache a function's result for a certain duration.

    The function's first argument should be a Timespan object which contains
    a duration, optional offset, and a validity period.
    """
    cache = {}

    @wraps(fn)
    def wrapped(timespan, **kwargs):
        # Validity does not need to be part of the key because the cached
        # information does not depend on the validity period.
        key = (timespan.duration, timespan.offset)
        if current := cache.get(key, None):
            if time.now() < current.issued + timespan.validity:
                return current.value

        value = fn(timespan, **kwargs)
        entry = CachedResult(value=value, issued=time.now())

        cache[key] = entry
        return value

    return wrapped
