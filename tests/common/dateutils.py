from __future__ import annotations

from datetime import datetime

from sarc.config import MTL, UTC

_time_format = "%Y-%m-%dT%H:%M"


def _dtfmt(
    year: int, month: int, day: int, hour: int = 0, minute: int = 0, tzinfo=MTL
) -> str:
    """Return date formatted in _time_format. Convert to UTC from given timezone (default MTL)."""
    return (
        datetime(year, month, day, hour, minute, tzinfo=tzinfo)
        .astimezone(UTC)
        .strftime(_time_format)
    )


def _dtstr(
    year: int, month: int, day: int, hour: int = 0, minute: int = 0, tzinfo=MTL
) -> str:
    """Return str(date). Convert to UTC from given timezone (default MTL)."""
    return str(datetime(year, month, day, hour, minute, tzinfo=tzinfo).astimezone(UTC))


def _dtreg(
    year: int, month: int, day: int, hour: int = 0, minute: int = 0, tzinfo=MTL
) -> str:
    """Return str(date) compatible with regex strings. Convert to UTC from given timezone (default MTL)."""
    return str(
        datetime(year, month, day, hour, minute, tzinfo=tzinfo).astimezone(UTC)
    ).replace("+", "\\+")
