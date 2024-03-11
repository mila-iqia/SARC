from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import wraps

from gifnoc.std import time


@dataclass
class CachedResult:
    value: object = None
    expiry: datetime = None


def cache(**validity):
    """Cache data for a certain validity period."""

    delta = timedelta(**validity)

    def deco(fn):
        current = CachedResult()

        @wraps(fn)
        def wrapped():
            if current.expiry is None or time.now() >= current.expiry:
                current.value = fn()
                current.expiry = time.now() + delta

            return current.value

        return wrapped

    return deco
